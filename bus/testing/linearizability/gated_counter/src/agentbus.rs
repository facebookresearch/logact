/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Payload;
use agentbus_api::AgentBus;
use agentbus_api::Environment;
use agentbus_core::decider::Decider;
use agentbus_core::vote_trackers::create_vote_tracker_for_decider_policy;
use agentbus_simulator::Simulator;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::ConformanceFixture;
use anyhow::Context as _;
use anyhow::Result;
use counter_suite::agentbus::AgentBusCounter;
use counter_suite::agentbus::decisions;
use counter_suite::agentbus::last_log_position;
use counter_suite::agentbus::log_position;
use counter_suite::counter::CounterError;
use counter_suite::counter::CounterSpec;
use counter_suite::counter::counter_command;
use linearizability::history::History;
use linearizability::history::OperationId;
use linearizability::object::LinearizabilityImplementation;
use linearizability::object::LinearizableObject;
use linearizability::object::OperationResult;
use linearizability::object::candidate_from_ordered_tags;
use rand::RngExt as _;

use crate::model::GatedCounterCommand;
use crate::model::GatedCounterCommandExt as _;
use crate::model::GatedCounterOperation;
use crate::model::GatedCounterOutput;
use crate::model::GatedCounterSpec;

pub struct AgentBusGatedCounter<T: AgentBus, E: Environment> {
    inner: AgentBusCounter<T, E>,
}

pub struct AgentBusGatedCounterImplementation<F>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
{
    fixture: Rc<F>,
    environment: Rc<Simulator>,
    agent_bus_id: String,
    stop: Rc<Cell<bool>>,
}

impl<F> AgentBusGatedCounterImplementation<F>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
{
    pub fn new(fixture: Rc<F>) -> Self {
        let environment = fixture.get_env();
        let agent_bus_id = format!(
            "gated-counter-{}",
            environment.with_rng(|rng| rng.random::<u64>())
        );
        let stop = Rc::new(Cell::new(false));
        spawn_decider(fixture.as_ref(), &environment, &agent_bus_id, stop.clone());
        Self {
            fixture,
            environment,
            agent_bus_id,
            stop,
        }
    }
}

impl<F> LinearizabilityImplementation for AgentBusGatedCounterImplementation<F>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    F::Impl: 'static,
{
    type Object = AgentBusGatedCounter<F::Impl, Simulator>;

    fn environment(&self) -> Rc<Simulator> {
        self.environment.clone()
    }

    fn create(&self, _client: usize) -> Rc<Self::Object> {
        Rc::new(AgentBusGatedCounter::new(
            self.fixture.create_impl(),
            self.agent_bus_id.clone(),
            self.environment.clone(),
        ))
    }

    async fn candidate(
        &self,
        history: &History<
            GatedCounterOperation,
            GatedCounterOutput,
            <Self::Object as LinearizableObject>::Tag,
        >,
    ) -> Result<Vec<OperationId>> {
        candidate_from_ordered_tags::<GatedCounterSpec, _>(history, [])
    }

    fn on_complete(&self) -> Option<Rc<dyn Fn()>> {
        let stop = self.stop.clone();
        Some(Rc::new(move || stop.set(true)))
    }
}

impl<T: AgentBus, E: Environment> AgentBusGatedCounter<T, E> {
    fn new(agent_bus: T, agent_bus_id: String, environment: Rc<E>) -> Self {
        Self {
            inner: AgentBusCounter::new(agent_bus, agent_bus_id, environment),
        }
    }

    async fn set_gate(&self, open: bool) -> Result<OperationResult<GatedCounterOutput, i64>> {
        let position = self
            .inner
            .append_payload(Payload {
                payload: Some(
                    agent_bus_proto_rust::agent_bus::payload::Payload::DeciderPolicy(if open {
                        DeciderPolicy::OnByDefault
                    } else {
                        DeciderPolicy::OffByDefault
                    }
                        as i32),
                ),
            })
            .await
            .context("append gate policy failed")?;
        let entries = self.inner.entries().await?;
        let (_, gate_open) = replay_gated(&entries, position)?;
        Ok(OperationResult {
            output: GatedCounterOutput::Gate(gate_open),
            tag: position,
        })
    }

    async fn execute_counter(
        &self,
        command: GatedCounterCommand,
    ) -> Result<OperationResult<GatedCounterOutput, i64>> {
        let counter_command = command
            .counter_command()
            .context("counter operation should produce a counter command")?;
        if !counter_command.operation.is_write() {
            let entries = self.inner.entries().await?;
            let tag = last_log_position(&entries);
            let (counter, _) = replay_gated(&entries, i64::MAX)?;
            return Ok(OperationResult {
                output: GatedCounterOutput::Counter(Ok(counter.value())),
                tag,
            });
        }

        let intention_position = self.inner.append_intention(counter_command).await?;
        let (entries, decision) = self.inner.wait_for_decision(intention_position).await?;
        let (counter, _) = replay_gated(&entries, intention_position)?;
        Ok(OperationResult {
            output: counter_write_output(decision.approved, counter.value()),
            tag: intention_position,
        })
    }

    async fn execute_gated(
        &self,
        command: GatedCounterCommand,
    ) -> Result<OperationResult<GatedCounterOutput, i64>> {
        match command.operation {
            GatedCounterOperation::Counter(_) => self.execute_counter(command).await,
            GatedCounterOperation::SetGate(open) => self.set_gate(open).await,
            GatedCounterOperation::ReadGate => {
                let entries = self.inner.entries().await?;
                let tag = last_log_position(&entries);
                let (_, gate_open) = replay_gated(&entries, i64::MAX)?;
                Ok(OperationResult {
                    output: GatedCounterOutput::Gate(gate_open),
                    tag,
                })
            }
        }
    }
}

impl<T: AgentBus, E: Environment> LinearizableObject for AgentBusGatedCounter<T, E> {
    type Spec = GatedCounterSpec;
    type Tag = i64;

    async fn execute(
        &self,
        id: OperationId,
        operation: GatedCounterOperation,
    ) -> Option<OperationResult<GatedCounterOutput, Self::Tag>> {
        Some(
            self.execute_gated(GatedCounterCommand::new(id, operation))
                .await
                .expect("AgentBus gated-counter adapter should execute"),
        )
    }
}

fn replay_gated(entries: &[BusEntry], through_position: i64) -> Result<(CounterSpec, bool)> {
    let mut counter = CounterSpec::new();
    let mut gate_open = true;
    let decisions = decisions(entries);
    for entry in entries {
        let position = log_position(entry)?;
        if position > through_position {
            continue;
        }
        if let Some(open) = gate(entry) {
            gate_open = open;
            continue;
        }
        let Some(command) = counter_command(entry)? else {
            continue;
        };
        if decisions
            .get(&position)
            .is_some_and(|decision| decision.approved)
        {
            counter.apply(command.operation);
        }
    }
    Ok((counter, gate_open))
}

fn counter_write_output(approved: bool, value: i64) -> GatedCounterOutput {
    GatedCounterOutput::Counter(if approved {
        Ok(value)
    } else {
        Err(CounterError::Denied)
    })
}

fn gate(entry: &BusEntry) -> Option<bool> {
    let agent_bus_proto_rust::agent_bus::payload::Payload::DeciderPolicy(policy) =
        entry.payload.as_ref()?.payload.as_ref()?
    else {
        return None;
    };
    match *policy {
        value if value == DeciderPolicy::OnByDefault as i32 => Some(true),
        value if value == DeciderPolicy::OffByDefault as i32 => Some(false),
        _ => None,
    }
}

fn spawn_decider<F>(
    fixture: &F,
    environment: &Rc<Simulator>,
    agent_bus_id: &str,
    stop: Rc<Cell<bool>>,
) where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
{
    let decider_impl = fixture.create_impl();
    let decider_environment = environment.clone();
    let agent_bus_id = agent_bus_id.to_string();
    environment.spawn_named(
        async move {
            let mut decider = Decider::new(
                decider_impl,
                agent_bus_id,
                0,
                create_vote_tracker_for_decider_policy,
            );
            while !stop.get() {
                decider
                    .poll_and_decide(None)
                    .await
                    .expect("Decider should succeed");
                let sleep_millis = decider_environment.with_rng(|rng| rng.random_range(1..5));
                decider_environment
                    .sleep(std::time::Duration::from_millis(sleep_millis))
                    .await;
            }
        },
        Some("decider".to_string()),
    );
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::Abort;
    use agent_bus_proto_rust::agent_bus::Commit;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use counter_suite::counter::CounterCommand;
    use counter_suite::counter::CounterCommandExt as _;
    use counter_suite::counter::CounterOperation;
    use linearizability::object::SequentialSpec as _;

    use super::*;

    fn entry(position: i64, inner: payload::Payload) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(inner),
            }),
        }
    }

    fn intention_entry(position: i64, operation: CounterOperation) -> BusEntry {
        let command = CounterCommand::new(
            OperationId {
                client: 0,
                sequence: position as usize,
            },
            operation,
        );
        entry(
            position,
            payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention(command.encode())),
                ..Default::default()
            }),
        )
    }

    fn decision_entry(position: i64, intention_id: i64, approved: bool) -> BusEntry {
        let decision = if approved {
            payload::Payload::Commit(Commit {
                intention_id,
                ..Default::default()
            })
        } else {
            payload::Payload::Abort(Abort {
                intention_id,
                ..Default::default()
            })
        };
        entry(position, decision)
    }

    fn observed_output(entries: &[BusEntry], intention_position: i64) -> GatedCounterOutput {
        let (counter, _) = replay_gated(entries, intention_position)
            .expect("AgentBus replay should process the mutation");
        let decision = decisions(entries)
            .get(&intention_position)
            .copied()
            .expect("the mutation should include a decision");
        counter_write_output(decision.approved, counter.value())
    }

    #[test]
    fn false_denial_is_rejected() {
        let entries = vec![
            intention_entry(0, CounterOperation::Increment),
            decision_entry(1, 0, false),
        ];
        let observed = observed_output(&entries, 0);
        let mut spec = GatedCounterSpec::new();

        assert_eq!(
            observed,
            GatedCounterOutput::Counter(Err(CounterError::Denied)),
            "AgentBus replay should preserve the abort"
        );
        assert!(
            spec.verify_and_apply(
                &GatedCounterOperation::Counter(CounterOperation::Increment),
                &observed,
            )
            .is_err(),
            "an open gate must reject an AgentBus abort"
        );
    }

    #[test]
    fn false_approval_is_rejected() {
        let entries = vec![
            entry(
                0,
                payload::Payload::DeciderPolicy(DeciderPolicy::OffByDefault as i32),
            ),
            intention_entry(1, CounterOperation::Increment),
            decision_entry(2, 1, true),
        ];
        let observed = observed_output(&entries, 1);
        let mut spec = GatedCounterSpec::new();
        spec.apply(GatedCounterOperation::SetGate(false));

        assert_eq!(
            observed,
            GatedCounterOutput::Counter(Ok(1)),
            "AgentBus replay should preserve the commit"
        );
        assert!(
            spec.verify_and_apply(
                &GatedCounterOperation::Counter(CounterOperation::Increment),
                &observed,
            )
            .is_err(),
            "a closed gate must reject an AgentBus commit"
        );
    }
}
