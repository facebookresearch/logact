/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BlockingPollRequest;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBus;
use agentbus_api::Environment;
use agentbus_simulator::Simulator;
use anyhow::Context as _;
use anyhow::Result;
use linearizability::history::Observation;
use linearizability::history::OperationId;
use linearizability::object::Client;
use linearizability::object::HistoryFor;
use linearizability::object::LinearizabilityImplementation;
use linearizability::object::LinearizableObject;
use linearizability::object::OperationResult;
use linearizability::object::candidate_from_ordered_tags;
use linearizability::object::run_linearizability_test;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_tests::fixtures::CommitServiceTestFixture;
use rand::RngExt as _;

use crate::counter::CounterCommand;
use crate::counter::CounterCommandExt as _;
use crate::counter::CounterError;
use crate::counter::CounterOperation;
use crate::counter::CounterOutput;
use crate::counter::CounterSpec;
use crate::counter::counter_command;

type CounterLog = BTreeMap<i64, (CounterCommand, bool)>;

#[derive(Default)]
struct CounterReplay {
    counter: CounterSpec,
    next_log_position: i64,
    applied_through: Option<i64>,
    intentions: BTreeMap<i64, CounterCommand>,
    decisions: HashMap<i64, bool>,
}

impl CounterReplay {
    fn process_entry(&mut self, entry: &BusEntry) -> Result<()> {
        let position = entry
            .header
            .as_ref()
            .context("AgentBus entry has no header")?
            .log_position;
        if let Some(command) = counter_command(entry)? {
            anyhow::ensure!(
                self.intentions.insert(position, command).is_none(),
                "duplicate counter intention at {position}"
            );
        }
        let decision = match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
            Some(payload::Payload::Commit(commit)) => Some((commit.intention_id, true)),
            Some(payload::Payload::Abort(abort)) => Some((abort.intention_id, false)),
            _ => None,
        };
        if let Some((intention, approved)) = decision {
            if let Some(previous) = self.decisions.insert(intention, approved) {
                anyhow::ensure!(
                    previous == approved,
                    "conflicting decisions for intention {intention}"
                );
            }
        }
        Ok(())
    }

    fn replay_through(&mut self, through_position: i64) -> Option<i64> {
        if self.next_log_position <= through_position {
            return None;
        }
        let start = self
            .applied_through
            .map_or(Bound::Unbounded, Bound::Excluded);
        for (&position, command) in self
            .intentions
            .range((start, Bound::Included(through_position)))
        {
            if *self.decisions.get(&position)? {
                self.counter.apply(command.operation);
            }
            self.applied_through = Some(position);
        }
        Some(self.counter.value())
    }

    fn counter_log(&self) -> Result<CounterLog> {
        self.intentions
            .iter()
            .map(|(&position, &command)| {
                let approved = *self
                    .decisions
                    .get(&position)
                    .with_context(|| format!("counter intention at {position} has no decision"))?;
                Ok((position, (command, approved)))
            })
            .collect()
    }
}

pub struct CommitServiceCounter<S> {
    service: S,
    agent_id: String,
    replay: RefCell<CounterReplay>,
    max_poll_entries: i32,
}

impl<S> CommitServiceCounter<S> {
    pub(crate) fn new(service: S, agent_id: String) -> Self {
        Self {
            service,
            agent_id,
            replay: RefCell::new(CounterReplay::default()),
            max_poll_entries: 1000,
        }
    }
}

impl<S: CommitSvc> LinearizableObject for CommitServiceCounter<S> {
    type Spec = CounterSpec;
    type Tag = i64;

    async fn execute(
        &self,
        id: OperationId,
        operation: CounterOperation,
    ) -> OperationResult<CounterOutput, Self::Tag> {
        self.execute_counter(CounterCommand::new(id, operation))
            .await
            .expect("CommitService counter adapter should execute")
    }
}

impl<S: CommitSvc> CommitServiceCounter<S> {
    async fn poll_and_process(&self) -> Result<()> {
        let cursor = self.replay.borrow().next_log_position;
        let response = self
            .service
            .agent_bus()
            .blocking_poll(BlockingPollRequest {
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_id.clone(),
                }),
                start_log_position: cursor,
                max_entries: self.max_poll_entries,
                filter: None,
                timeout_ms: 1000,
            })
            .await
            .context("counter blocking_poll failed")?;
        anyhow::ensure!(
            response.next_start_position >= cursor
                && (response.entries.is_empty() || response.next_start_position > cursor),
            "counter poll made no progress from log position {cursor}"
        );
        let mut replay = self.replay.borrow_mut();
        for entry in response.entries {
            replay.process_entry(&entry)?;
        }
        replay.next_log_position = response.next_start_position;
        Ok(())
    }

    pub(crate) async fn execute_counter(
        &self,
        command: CounterCommand,
    ) -> Result<OperationResult<CounterOutput, i64>> {
        let response = self
            .service
            .commit_intention(CommitIntentionCommand {
                bus_id: BusId {
                    agent_bus_id: self.agent_id.clone(),
                },
                intention: intention::Intention::StringIntention(command.encode()),
            })
            .await
            .context("commit_intention failed")?;
        loop {
            {
                let mut replay = self.replay.borrow_mut();
                if replay.next_log_position > response.log_position {
                    anyhow::ensure!(
                        replay.intentions.get(&response.log_position) == Some(&command),
                        "counter response at {} does not match command {command:?}",
                        response.log_position
                    );
                    if let Some(value) = replay.replay_through(response.log_position) {
                        let approved = replay.decisions[&response.log_position];
                        anyhow::ensure!(
                            approved == response.approved,
                            "counter response at {} disagrees with its logged decision",
                            response.log_position
                        );
                        return Ok(OperationResult {
                            output: if command.operation.is_write() && !approved {
                                Err(CounterError::Denied)
                            } else {
                                Ok(value)
                            },
                            tag: response.log_position,
                        });
                    }
                }
            }
            self.poll_and_process().await?;
        }
    }

    async fn counter_log(&self) -> Result<CounterLog> {
        let tail = self
            .service
            .agent_bus()
            .check_tail(CheckTailRequest {
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_id.clone(),
                }),
            })
            .await?
            .tail_position;
        while self.replay.borrow().next_log_position < tail {
            self.poll_and_process().await?;
        }
        self.replay.borrow().counter_log()
    }
}

pub struct CommitServiceCounterImplementation<F>
where
    F: CommitServiceTestFixture<Env = Simulator>,
{
    fixture: Rc<F>,
    environment: Rc<Simulator>,
    agent_id: String,
    clients: Rc<RefCell<Vec<Rc<CommitServiceCounter<F::Impl>>>>>,
}

impl<F> CommitServiceCounterImplementation<F>
where
    F: CommitServiceTestFixture<Env = Simulator>,
{
    pub fn new(fixture: Rc<F>) -> Self {
        let environment = fixture.get_env();
        let agent_id = format!(
            "counter-{}",
            environment.with_rng(|rng| rng.random::<u64>())
        );
        Self {
            fixture,
            environment,
            agent_id,
            clients: Rc::default(),
        }
    }
}

impl<F> LinearizabilityImplementation for CommitServiceCounterImplementation<F>
where
    F: CommitServiceTestFixture<Env = Simulator>,
    F::Impl: 'static,
{
    type Object = CommitServiceCounter<F::Impl>;

    fn environment(&self) -> Rc<Simulator> {
        self.environment.clone()
    }

    fn create(&self, _client: usize) -> Rc<Self::Object> {
        let client = Rc::new(CommitServiceCounter::new(
            self.fixture.create_impl(),
            self.agent_id.clone(),
        ));
        self.clients.borrow_mut().push(client.clone());
        client
    }

    async fn candidate(&self, history: &HistoryFor<Self::Object>) -> Result<Vec<OperationId>> {
        let clients = self.clients.borrow().clone();
        let mut logs = Vec::with_capacity(clients.len());
        for client in clients {
            logs.push(client.counter_log().await?);
        }
        verify_log_evidence(&history.observations(), &logs)?;
        Ok(candidate_from_ordered_tags::<CounterSpec, _>(history))
    }
}

fn verify_log_evidence(
    observations: &[Observation<CounterOperation, CounterOutput, i64>],
    logs: &[CounterLog],
) -> Result<()> {
    let log = logs
        .first()
        .context("counter should retain at least one client")?;
    for (client, observed) in logs.iter().enumerate().skip(1) {
        anyhow::ensure!(
            observed == log,
            "CommitService client {client} counter history differs from client 0"
        );
    }
    let mut observed_positions = HashSet::new();
    for observation in observations {
        let position = observation.completed.tag;
        anyhow::ensure!(
            observed_positions.insert(position),
            "duplicate counter response position {position}"
        );
        let (command, approved) = log
            .get(&position)
            .with_context(|| format!("counter response at {position} has no logged intention"))?;
        anyhow::ensure!(
            command.id == observation.id && command.operation == observation.completed.operation,
            "counter response at {position} does not match its logged command"
        );
        anyhow::ensure!(
            observation.completed.output.is_ok() == (*approved || !command.operation.is_write()),
            "counter response at {position} disagrees with its logged decision"
        );
    }
    anyhow::ensure!(
        observations.len() == log.len(),
        "counter response count {} differs from logged intention count {}",
        observations.len(),
        log.len()
    );
    Ok(())
}

pub async fn run_counter_with_denials<F>(fixture: Rc<F>) -> Result<()>
where
    F: CommitServiceTestFixture<Env = Simulator>,
    F::Impl: 'static,
{
    let environment = fixture.get_env();
    let write_count = environment.with_rng(|rng| rng.random_range(6..12));
    let operations = (0..write_count)
        .map(|_| {
            if environment.with_rng(|rng| rng.random_range(0..2)) == 0 {
                CounterOperation::Increment
            } else {
                CounterOperation::Decrement
            }
        })
        .collect();
    let implementation = CommitServiceCounterImplementation::new(fixture);
    let clients = implementation.clients.clone();
    run_linearizability_test(vec![Client::new("counter", operations)], implementation).await?;
    let client = clients.borrow()[0].clone();
    let log = client.replay.borrow().counter_log()?;
    let committed = log.values().filter(|(_, approved)| *approved).count();
    let aborted = log.len() - committed;
    anyhow::ensure!(
        log.len() == write_count
            && committed == write_count.div_ceil(2)
            && aborted == write_count / 2,
        "modulus-2 voter should commit ceil(N/2) and abort floor(N/2) of N={write_count}: \
         got committed={committed}, aborted={aborted}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::Abort;
    use agent_bus_proto_rust::agent_bus::Commit;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;

    use super::*;

    fn command(client: usize, sequence: usize, operation: CounterOperation) -> CounterCommand {
        CounterCommand::new(OperationId { client, sequence }, operation)
    }

    fn entry(position: i64, payload: payload::Payload) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload),
            }),
        }
    }

    fn intention(position: i64, command: CounterCommand) -> BusEntry {
        entry(
            position,
            payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention(command.encode())),
                ..Default::default()
            }),
        )
    }

    fn decision(position: i64, intention_id: i64, approved: bool) -> BusEntry {
        entry(
            position,
            if approved {
                payload::Payload::Commit(Commit {
                    intention_id,
                    ..Default::default()
                })
            } else {
                payload::Payload::Abort(Abort {
                    intention_id,
                    ..Default::default()
                })
            },
        )
    }

    #[test]
    fn replay_waits_for_earlier_decisions_and_stops_at_the_requested_intention() {
        let mut replay = CounterReplay::default();
        for entry in [
            intention(0, command(0, 0, CounterOperation::Increment)),
            intention(1, command(1, 0, CounterOperation::Decrement)),
            intention(2, command(2, 0, CounterOperation::Increment)),
            decision(3, 2, true),
            decision(4, 1, false),
        ] {
            replay.process_entry(&entry).unwrap();
        }
        replay.next_log_position = 5;
        assert_eq!(replay.replay_through(2), None);
        assert_eq!(replay.counter.value(), 0);

        replay.process_entry(&decision(5, 0, true)).unwrap();
        replay.next_log_position = 6;
        assert_eq!(replay.replay_through(0), Some(1));
        assert_eq!(replay.replay_through(2), Some(2));
        assert_eq!(replay.replay_through(2), Some(2));
    }

    #[test]
    fn log_evidence_rejects_corrupted_responses_and_client_histories() {
        let command = command(0, 0, CounterOperation::Increment);
        let observation = Observation {
            id: command.id,
            start_time: agentbus_api::MonotonicInstant::from_duration_since_clock_origin(
                Duration::ZERO,
            ),
            end_time: agentbus_api::MonotonicInstant::from_duration_since_clock_origin(
                Duration::ZERO,
            ),
            completed: linearizability::history::CompletedOperation {
                operation: command.operation,
                output: Ok(1),
                tag: 0,
            },
        };
        let log = CounterLog::from([(0, (command, true))]);
        verify_log_evidence(
            std::slice::from_ref(&observation),
            std::slice::from_ref(&log),
        )
        .unwrap();
        for (mutated, expected) in [
            (
                Observation {
                    id: OperationId {
                        client: 1,
                        sequence: 0,
                    },
                    ..observation.clone()
                },
                "logged command",
            ),
            (
                Observation {
                    completed: linearizability::history::CompletedOperation {
                        operation: CounterOperation::Decrement,
                        ..observation.completed.clone()
                    },
                    ..observation.clone()
                },
                "logged command",
            ),
            (
                Observation {
                    completed: linearizability::history::CompletedOperation {
                        output: Err(CounterError::Denied),
                        ..observation.completed.clone()
                    },
                    ..observation.clone()
                },
                "logged decision",
            ),
            (
                Observation {
                    completed: linearizability::history::CompletedOperation {
                        tag: 1,
                        ..observation.completed.clone()
                    },
                    ..observation.clone()
                },
                "no logged intention",
            ),
        ] {
            let error = verify_log_evidence(&[mutated], std::slice::from_ref(&log)).unwrap_err();
            assert!(error.to_string().contains(expected));
        }
        let error = verify_log_evidence(
            &[observation.clone(), observation.clone()],
            std::slice::from_ref(&log),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate counter response position")
        );
        let error = verify_log_evidence(&[], std::slice::from_ref(&log)).unwrap_err();
        assert!(error.to_string().contains("response count"));
        let error = verify_log_evidence(&[observation], &[log, CounterLog::new()]).unwrap_err();
        assert!(error.to_string().contains("history differs"));
    }
}

conformance::define_driver! {
    $,
    root = counter_suite,
    module = commit_service,
    scenario_mods = [adapter],
    fixtures = counter_commit_service_fixtures,
    suite = commit_service_adapter,
}
