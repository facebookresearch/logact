/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::base_engine_control;
use agent_bus_proto_rust::agent_bus::control;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBus;
use agentbus_api::Environment;
use agentbus_simulator::Simulator;
use agentbus_tests::common::helpers::read_linearizable_snapshot;
use anyhow::Context as _;
use anyhow::Result;
use anyhow::bail;
use counter_suite::counter::CounterCommand;
use counter_suite::counter::CounterCommandExt as _;
use counter_suite::counter::CounterError;
use counter_suite::counter::CounterSpec;
use linearizability::history::History;
use linearizability::history::Observation;
use linearizability::history::OperationId;
use linearizability::object::LinearizabilityImplementation;
use linearizability::object::LinearizableObject;
use linearizability::object::OperationResult;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_tests::fixtures::DeciderPolicyFixture;
use logact_commit_service_tests::fixtures::SettableDeciderPolicy;
use rand::RngExt as _;

use crate::model::GatedCounterCommand;
use crate::model::GatedCounterCommandExt as _;
use crate::model::GatedCounterOperation;
use crate::model::GatedCounterOutput;
use crate::model::GatedCounterSpec;

type CounterOutcomes = Rc<RefCell<HashMap<i64, CounterOutcome>>>;

#[derive(Clone, Copy)]
enum CounterOutcome {
    Applied,
    GateDenied,
    VersionStale,
}

#[derive(Clone, Debug)]
pub enum PolicyTag {
    RegisterVersion(i64),
    AgentBusPosition(i64),
    Unanchored,
}

pub struct CommitServiceGatedCounter<S, C> {
    service: S,
    policy: Rc<C>,
    agent_id: String,
    environment: Rc<Simulator>,
    outcomes: CounterOutcomes,
}

pub struct CommitServiceGatedCounterImplementation<S, C, B> {
    environment: Rc<Simulator>,
    service: S,
    verification_bus: B,
    policy: Rc<C>,
    agent_id: String,
    outcomes: CounterOutcomes,
}

impl<S, C, B> CommitServiceGatedCounterImplementation<S, C, B> {
    /// Build a gated counter over an independently configured service topology.
    pub fn new(
        service: S,
        verification_bus: B,
        policy: C,
        agent_id: String,
        environment: Rc<Simulator>,
    ) -> Self {
        Self {
            service,
            verification_bus,
            policy: Rc::new(policy),
            agent_id,
            outcomes: CounterOutcomes::default(),
            environment,
        }
    }

    fn counter_for_client(&self) -> CommitServiceGatedCounter<S, C>
    where
        S: Clone,
    {
        CommitServiceGatedCounter {
            service: self.service.clone(),
            policy: self.policy.clone(),
            agent_id: self.agent_id.clone(),
            environment: self.environment.clone(),
            outcomes: self.outcomes.clone(),
        }
    }
}

impl<S, C, B> LinearizabilityImplementation for CommitServiceGatedCounterImplementation<S, C, B>
where
    S: CommitSvc<Bus = B> + Clone + 'static,
    C: SettableDeciderPolicy + 'static,
    B: AgentBus + 'static,
{
    type Object = CommitServiceGatedCounter<S, C>;

    fn environment(&self) -> Rc<Simulator> {
        self.environment.clone()
    }

    fn create(&self, _client: usize) -> Rc<Self::Object> {
        Rc::new(self.counter_for_client())
    }

    async fn candidate(
        &self,
        history: &History<
            GatedCounterOperation,
            GatedCounterOutput,
            <Self::Object as LinearizableObject>::Tag,
        >,
    ) -> Result<Vec<OperationId>> {
        let (entries, _) = read_linearizable_snapshot(
            &self.verification_bus,
            self.agent_id.clone(),
            0,
            None,
            &*self.environment,
            Some(1000),
        )
        .await;
        let candidate = candidate_from_bus(&entries, &history.observations())?;
        history.complete_candidate(candidate)
    }
}

pub fn gated_counter_implementation<F>(
    fixture: &F,
) -> CommitServiceGatedCounterImplementation<F::Impl, F::Controller, <F::Impl as CommitSvc>::Bus>
where
    F: DeciderPolicyFixture<Env = Simulator>,
    F::Impl: Clone + 'static,
    <F::Impl as CommitSvc>::Bus: Clone + 'static,
    F::Controller: 'static,
{
    let environment = fixture.get_env();
    let agent_id = format!(
        "gated-counter-{}",
        environment.with_rng(|rng| rng.random::<u64>())
    );
    let service = fixture.create_impl();
    let verification_service = fixture.create_impl();
    let verification_bus = verification_service.agent_bus().clone();
    let policy = fixture.policy_controller();
    CommitServiceGatedCounterImplementation::new(
        service,
        verification_bus,
        policy,
        agent_id,
        environment,
    )
}

impl<S, C> CommitServiceGatedCounter<S, C>
where
    S: CommitSvc,
    C: SettableDeciderPolicy,
{
    async fn execute_counter(
        &self,
        command: GatedCounterCommand,
    ) -> Result<OperationResult<GatedCounterOutput, PolicyTag>> {
        let counter_command = command
            .counter_command()
            .context("counter operation should produce a counter command")?;
        let (response, outcome) = loop {
            let response = self
                .service
                .commit_intention(CommitIntentionCommand {
                    bus_id: BusId {
                        agent_bus_id: self.agent_id.clone(),
                    },
                    intention: intention::Intention::StringIntention(counter_command.encode()),
                })
                .await?;
            let outcome = if response.approved {
                CounterOutcome::Applied
            } else if skipped_for_policy_version(&response.reason) {
                CounterOutcome::VersionStale
            } else {
                CounterOutcome::GateDenied
            };
            self.outcomes
                .borrow_mut()
                .insert(response.log_position, outcome);
            if response.approved || !skipped_for_policy_version(&response.reason) {
                break (response, outcome);
            }
            self.environment.sleep(Duration::from_millis(1)).await;
        };

        let value = self.replay_through(response.log_position).await?;
        Ok(OperationResult {
            output: counter_output(outcome, value)?,
            tag: PolicyTag::AgentBusPosition(response.log_position),
        })
    }

    async fn replay_through(&self, through_position: i64) -> Result<i64> {
        let mut last_error = None;
        for _ in 0..20 {
            let (entries, _) = read_linearizable_snapshot(
                self.service.agent_bus(),
                self.agent_id.clone(),
                0,
                None,
                &*self.environment,
                Some(1000),
            )
            .await;
            match replay_gated_counter(&entries, through_position, &self.outcomes) {
                Ok(value) => return Ok(value),
                Err(error) => last_error = Some(error),
            }
            self.environment.sleep(Duration::from_millis(1)).await;
        }
        Err(last_error.expect("counter replay should record a missing-outcome error"))
    }

    async fn execute_gated(
        &self,
        command: GatedCounterCommand,
    ) -> Result<OperationResult<GatedCounterOutput, PolicyTag>> {
        match command.operation {
            GatedCounterOperation::Counter(_) => self.execute_counter(command).await,
            GatedCounterOperation::SetGate(open) => {
                let policy = if open {
                    DeciderPolicy::OnByDefault
                } else {
                    DeciderPolicy::OffByDefault
                };
                let version = self.policy.set_decider_policy(policy).await?;
                Ok(OperationResult {
                    output: GatedCounterOutput::Gate(open),
                    tag: PolicyTag::RegisterVersion(version),
                })
            }
            GatedCounterOperation::ReadGate => {
                let (policy, _) = self.policy.read_decider_policy().await?;
                Ok(OperationResult {
                    output: GatedCounterOutput::Gate(policy == DeciderPolicy::OnByDefault),
                    tag: PolicyTag::Unanchored,
                })
            }
        }
    }
}

impl<S, C> LinearizableObject for CommitServiceGatedCounter<S, C>
where
    S: CommitSvc,
    C: SettableDeciderPolicy,
{
    type Spec = GatedCounterSpec;
    type Tag = PolicyTag;

    async fn execute(
        &self,
        id: OperationId,
        operation: GatedCounterOperation,
    ) -> OperationResult<GatedCounterOutput, Self::Tag> {
        self.execute_gated(GatedCounterCommand::new(id, operation))
            .await
            .expect("CommitService gated-counter adapter should execute")
    }
}

fn counter_command(entry: &BusEntry) -> Result<Option<CounterCommand>> {
    let Some(payload::Payload::Intention(intention)) = entry
        .payload
        .as_ref()
        .and_then(|payload| payload.payload.as_ref())
    else {
        return Ok(None);
    };
    let Some(intention::Intention::StringIntention(body)) = intention.intention.as_ref() else {
        return Ok(None);
    };
    CounterCommand::decode(body)
}

fn replay_gated_counter(
    entries: &[BusEntry],
    through_position: i64,
    outcomes: &CounterOutcomes,
) -> Result<i64> {
    let outcomes = outcomes.borrow().clone();
    let mut counter = CounterSpec::new();
    let mut applied_version = None;
    for entry in entries {
        let position = entry
            .header
            .as_ref()
            .context("AgentBus entry has no header")?
            .log_position;
        if position > through_position {
            continue;
        }
        if let Some(batch) = policy_batch(entry)
            && policy_batch_applies(applied_version, batch)
        {
            applied_version = Some(batch.new_version);
            if let Some(policy) = batch.decider_policy {
                match DeciderPolicy::try_from(policy) {
                    Ok(DeciderPolicy::OnByDefault | DeciderPolicy::OffByDefault) => {}
                    other => bail!("unexpected gated-counter policy {other:?}"),
                }
            }
        }
        let Some(command) = counter_command(entry)? else {
            continue;
        };
        match outcomes.get(&position).ok_or_else(|| {
            anyhow::anyhow!("counter intention at {position} has no observed outcome")
        })? {
            CounterOutcome::VersionStale => continue,
            CounterOutcome::Applied => {
                counter.apply(command.operation);
            }
            CounterOutcome::GateDenied => {}
        }
    }
    Ok(counter.value())
}

fn counter_output(outcome: CounterOutcome, value: i64) -> Result<GatedCounterOutput> {
    match outcome {
        CounterOutcome::Applied => Ok(GatedCounterOutput::Counter(Ok(value))),
        CounterOutcome::GateDenied => Ok(GatedCounterOutput::Counter(Err(CounterError::Denied))),
        CounterOutcome::VersionStale => {
            bail!("a policy-version-stale attempt cannot complete a counter operation")
        }
    }
}

fn skipped_for_policy_version(reason: &str) -> bool {
    reason.starts_with("required policy version ") || reason.starts_with("minimum policy version ")
}

fn policy_batch(entry: &BusEntry) -> Option<&PolicyBatch> {
    let payload::Payload::Control(control) = entry.payload.as_ref()?.payload.as_ref()? else {
        return None;
    };
    let control::Control::BaseEngineControl(base) = control.control.as_ref()?;
    let base_engine_control::Control::PolicyBatch(batch) = base.control.as_ref()? else {
        return None;
    };
    Some(batch)
}

fn minimum_policy_version(entry: &BusEntry, position: i64) -> Result<i64> {
    let Some(payload::Payload::Intention(proposed)) = entry
        .payload
        .as_ref()
        .and_then(|payload| payload.payload.as_ref())
    else {
        bail!("proposal position {position} is not an intention");
    };
    match proposed.policy_version_constraint.as_ref() {
        Some(intention::PolicyVersionConstraint::MinimumPolicyVersion(version)) => Ok(*version),
        other => bail!("proposal position {position} has unexpected policy constraint {other:?}"),
    }
}

fn policy_batch_applies(applied_version: Option<i64>, batch: &PolicyBatch) -> bool {
    applied_version == batch.expected_current_version
        && batch
            .expected_current_version
            .is_none_or(|current| batch.new_version > current)
}

fn candidate_from_bus(
    entries: &[BusEntry],
    observations: &[Observation<GatedCounterOperation, GatedCounterOutput, PolicyTag>],
) -> Result<Vec<OperationId>> {
    let mut writes: Vec<_> = observations
        .iter()
        .filter(|observation| {
            matches!(
                observation.completed.operation,
                GatedCounterOperation::SetGate(_)
            )
        })
        .map(|observation| match observation.completed.tag {
            PolicyTag::RegisterVersion(version) => Ok((version, observation.id.clone())),
            _ => bail!("gate write {:?} has no register version", observation.id),
        })
        .collect::<Result<Vec<_>>>()?;
    writes.sort_by_key(|(version, _)| *version);
    anyhow::ensure!(
        writes
            .windows(2)
            .all(|versions| versions[0].0 < versions[1].0),
        "policy register versions must be strictly increasing"
    );
    let mut writes = writes.into_iter().peekable();

    let mut proposals = HashMap::new();
    for observation in observations.iter().filter(|observation| {
        matches!(
            observation.completed.operation,
            GatedCounterOperation::Counter(_)
        )
    }) {
        let PolicyTag::AgentBusPosition(position) = observation.completed.tag else {
            bail!(
                "counter operation {:?} has no AgentBus position",
                observation.id
            );
        };
        anyhow::ensure!(
            proposals.insert(position, observation).is_none(),
            "multiple counter operations recorded at AgentBus position {position}"
        );
    }

    let mut candidate = Vec::with_capacity(observations.len());
    let mut applied_version = None;
    for entry in entries {
        if let Some(batch) = policy_batch(entry)
            && policy_batch_applies(applied_version, batch)
        {
            applied_version = Some(batch.new_version);
            while writes
                .peek()
                .is_some_and(|(version, _)| *version <= batch.new_version)
            {
                candidate.push(writes.next().expect("peeked gate write should exist").1);
            }
        }
        let position = entry
            .header
            .as_ref()
            .map(|header| header.log_position)
            .context("AgentBus entry has no header")?;
        let Some(proposal) = proposals.remove(&position) else {
            continue;
        };
        let minimum_version = minimum_policy_version(entry, position)?;
        anyhow::ensure!(
            applied_version.is_some_and(|applied| applied >= minimum_version),
            "completed proposal at {position} requires policy version {minimum_version}, but only {applied_version:?} is applied"
        );
        candidate.push(proposal.id.clone());
    }
    candidate.extend(writes.map(|(_, id)| id));
    anyhow::ensure!(proposals.is_empty(), "AgentBus candidate omitted proposals");
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::payload;
    use counter_suite::counter::CounterCommand;
    use counter_suite::counter::CounterOperation;
    use linearizability::history::OperationId;
    use linearizability::object::SequentialSpec as _;

    use super::*;

    #[test]
    fn workload_clients_share_configured_service_topology() {
        let services = Rc::new(vec![0, 1, 2]);
        let implementation = CommitServiceGatedCounterImplementation::new(
            services.clone(),
            (),
            (),
            "test-bus".to_string(),
            Rc::new(Simulator::new(0)),
        );

        let first = implementation.counter_for_client();
        let second = implementation.counter_for_client();

        assert_eq!(
            first.service.len(),
            3,
            "the topology should own three servers"
        );
        assert!(
            Rc::ptr_eq(&first.service, &second.service),
            "workload clients should use the same configured service topology"
        );
        assert!(
            Rc::ptr_eq(&first.service, &services),
            "client creation should not construct additional servers"
        );
        assert!(
            Rc::ptr_eq(&first.policy, &second.policy),
            "clients should share the policy controller"
        );
        assert!(
            Rc::ptr_eq(&first.outcomes, &second.outcomes),
            "clients should share observation bookkeeping"
        );
    }

    fn intention_entry(position: i64, operation: CounterOperation) -> BusEntry {
        let command = CounterCommand::new(
            OperationId {
                client: 0,
                sequence: position as usize,
            },
            operation,
        );
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention(command.encode())),
                    ..Default::default()
                })),
            }),
        }
    }

    fn observed_output(outcome: CounterOutcome) -> GatedCounterOutput {
        let entries = vec![intention_entry(0, CounterOperation::Increment)];
        let outcomes = CounterOutcomes::default();
        outcomes.borrow_mut().insert(0, outcome);
        let value = replay_gated_counter(&entries, 0, &outcomes)
            .expect("counter replay should process the mutation");
        counter_output(outcome, value).expect("the final outcome should not be version-stale")
    }

    #[test]
    fn false_denial_is_rejected() {
        let observed = observed_output(CounterOutcome::GateDenied);
        let mut spec = GatedCounterSpec::new();

        assert_eq!(
            observed,
            GatedCounterOutput::Counter(Err(CounterError::Denied)),
            "CommitService replay should preserve the denial"
        );
        assert!(
            spec.verify_and_apply(
                &GatedCounterOperation::Counter(CounterOperation::Increment),
                &observed,
            )
            .is_err(),
            "an open gate must reject a CommitService denial"
        );
    }

    #[test]
    fn false_approval_is_rejected() {
        let observed = observed_output(CounterOutcome::Applied);
        let mut spec = GatedCounterSpec::new();
        spec.apply(GatedCounterOperation::SetGate(false));

        assert_eq!(
            observed,
            GatedCounterOutput::Counter(Ok(1)),
            "CommitService replay should preserve the approval"
        );
        assert!(
            spec.verify_and_apply(
                &GatedCounterOperation::Counter(CounterOperation::Increment),
                &observed,
            )
            .is_err(),
            "a closed gate must reject a CommitService approval"
        );
    }
}
