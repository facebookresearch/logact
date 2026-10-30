/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BlockingPollRequest;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::Vote;
use agentbus_api::AgentBus;
use agentbus_api::Environment;
use agentbus_core::decider::Decider;
use agentbus_core::vote_trackers::create_vote_tracker_for_decider_policy;
use agentbus_simulator::Simulator;
use agentbus_tests::fixtures::AgentBusTestFixture;
use anyhow::Context as _;
use anyhow::Result;
use conformance::ConformanceFixture;
use linearizability::history::History;
use linearizability::history::Observation;
use linearizability::history::OperationId;
use linearizability::object::LinearizabilityImplementation;
use linearizability::object::LinearizableObject;
use linearizability::object::OperationResult;
use linearizability::object::candidate_from_ordered_tags;
use rand::RngExt as _;

use crate::counter::CounterCommand;
use crate::counter::CounterCommandExt as _;
use crate::counter::CounterError;
use crate::counter::CounterOperation;
use crate::counter::CounterOutput;
use crate::counter::CounterSpec;
use crate::counter::counter_command;

#[derive(Clone, Copy)]
pub struct Decision {
    pub approved: bool,
    pub log_position: i64,
}

pub struct AgentBusCounter<T: AgentBus, E: Environment> {
    agent_bus: T,
    agent_bus_id: String,
    environment: Rc<E>,
    state: RefCell<CounterState>,
    skip_commits: bool,
    max_poll_entries: i32,
}

struct CounterState {
    counter: CounterSpec,
    next_log_position: i64,
    last_log_position: i64,
    buffered_intentions: HashMap<i64, CounterOperation>,
    entries: Vec<BusEntry>,
}

impl<T: AgentBus, E: Environment> AgentBusCounter<T, E> {
    pub fn new(agent_bus: T, agent_bus_id: String, environment: Rc<E>) -> Self {
        Self {
            agent_bus,
            agent_bus_id,
            environment,
            state: RefCell::new(CounterState {
                counter: CounterSpec::new(),
                next_log_position: 0,
                last_log_position: -1,
                buffered_intentions: HashMap::new(),
                entries: Vec::new(),
            }),
            skip_commits: false,
            max_poll_entries: 1000,
        }
    }

    pub fn with_skip_commits(mut self) -> Self {
        self.skip_commits = true;
        self
    }

    pub fn with_max_poll_entries(mut self, max_entries: i32) -> Self {
        self.max_poll_entries = max_entries;
        self
    }

    pub async fn append_payload(&self, payload: Payload) -> Result<i64> {
        let response = self
            .agent_bus
            .append(AppendRequest {
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                payload: Some(payload),
                ..Default::default()
            })
            .await
            .context("append failed")?;
        Ok(response.log_position)
    }

    pub async fn append_intention(&self, command: CounterCommand) -> Result<i64> {
        self.append_payload(Payload {
            payload: Some(
                agent_bus_proto_rust::agent_bus::payload::Payload::Intention(Intention {
                    intention: Some(
                        agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(
                            command.encode(),
                        ),
                    ),
                    ..Default::default()
                }),
            ),
        })
        .await
    }

    async fn tail(&self) -> Result<i64> {
        Ok(self
            .agent_bus
            .check_tail(CheckTailRequest {
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
            })
            .await
            .context("check_tail failed")?
            .tail_position)
    }

    async fn poll_and_process(
        &self,
        watched_intention: Option<i64>,
    ) -> Result<Option<OperationResult<CounterOutput, i64>>> {
        let cursor = self.state.borrow().next_log_position;
        let response = self
            .agent_bus
            .blocking_poll(BlockingPollRequest {
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                start_log_position: cursor,
                max_entries: self.max_poll_entries,
                filter: None,
                timeout_ms: 0,
            })
            .await
            .context("blocking_poll failed")?;
        anyhow::ensure!(
            response.next_start_position > cursor,
            "poll made no progress from log position {cursor}"
        );

        let mut completed = None;
        let mut state = self.state.borrow_mut();
        for entry in response.entries {
            let position = log_position(&entry)?;
            state.last_log_position = position;
            if let Some(result) = Self::process_entry(
                &mut state,
                &entry,
                position,
                watched_intention,
                self.skip_commits,
            )? {
                completed = Some(result);
            }
            state.entries.push(entry);
        }
        state.next_log_position = response.next_start_position;
        Ok(completed)
    }

    pub async fn entries(&self) -> Result<Vec<BusEntry>> {
        let tail = self.tail().await?;
        self.drain_to(tail, None).await?;
        Ok(self.state.borrow().entries.clone())
    }

    pub async fn wait_for_decision(
        &self,
        intention_position: i64,
    ) -> Result<(Vec<BusEntry>, Decision)> {
        loop {
            let entries = self.entries().await?;
            if self.skip_commits {
                return Ok((
                    entries,
                    Decision {
                        approved: true,
                        log_position: intention_position,
                    },
                ));
            }
            if let Some(decision) = decisions(&entries).get(&intention_position) {
                return Ok((entries, *decision));
            }
            let delay_ms = self.environment.with_rng(|rng| rng.random_range(0..5));
            self.environment
                .sleep(std::time::Duration::from_millis(delay_ms))
                .await;
        }
    }

    fn process_entry(
        state: &mut CounterState,
        entry: &BusEntry,
        position: i64,
        watched_intention: Option<i64>,
        skip_commits: bool,
    ) -> Result<Option<OperationResult<CounterOutput, i64>>> {
        if skip_commits {
            let Some(command) = counter_command(entry)? else {
                return Ok(None);
            };
            if !command.operation.is_write() {
                return Ok(None);
            }
            let value = state.counter.apply(command.operation);
            return Ok(
                (watched_intention == Some(position)).then_some(OperationResult {
                    output: Ok(value),
                    tag: position,
                }),
            );
        }

        let Some(payload) = entry
            .payload
            .as_ref()
            .and_then(|payload| payload.payload.as_ref())
        else {
            return Ok(None);
        };
        match payload {
            agent_bus_proto_rust::agent_bus::payload::Payload::Intention(_) => {
                if let Some(command) = counter_command(entry)?
                    && command.operation.is_write()
                {
                    state
                        .buffered_intentions
                        .insert(position, command.operation);
                }
            }
            agent_bus_proto_rust::agent_bus::payload::Payload::Commit(commit) => {
                if let Some(operation) = state.buffered_intentions.remove(&commit.intention_id) {
                    let value = state.counter.apply(operation);
                    if watched_intention == Some(commit.intention_id) {
                        return Ok(Some(OperationResult {
                            output: Ok(value),
                            tag: position,
                        }));
                    }
                }
            }
            agent_bus_proto_rust::agent_bus::payload::Payload::Abort(abort) => {
                state.buffered_intentions.remove(&abort.intention_id);
                if watched_intention == Some(abort.intention_id) {
                    return Ok(Some(OperationResult {
                        output: Err(CounterError::Denied),
                        tag: position,
                    }));
                }
            }
            _ => {}
        }
        Ok(None)
    }

    async fn drain_to(
        &self,
        tail: i64,
        watched_intention: Option<i64>,
    ) -> Result<Option<OperationResult<CounterOutput, i64>>> {
        let mut completed = None;
        while self.state.borrow().next_log_position < tail {
            if let Some(result) = self.poll_and_process(watched_intention).await? {
                completed = Some(result);
            }
        }
        Ok(completed)
    }

    async fn wait_for_completion(
        &self,
        intention_position: i64,
    ) -> Result<OperationResult<CounterOutput, i64>> {
        loop {
            let tail = self.tail().await?;
            if let Some(result) = self.drain_to(tail, Some(intention_position)).await? {
                return Ok(result);
            }
            let delay_ms = self.environment.with_rng(|rng| rng.random_range(0..5));
            self.environment
                .sleep(std::time::Duration::from_millis(delay_ms))
                .await;
        }
    }

    async fn execute_counter(
        &self,
        command: CounterCommand,
    ) -> Result<OperationResult<CounterOutput, i64>> {
        if !command.operation.is_write() {
            let tail = self.tail().await?;
            self.drain_to(tail, None).await?;
            let state = self.state.borrow();
            return Ok(OperationResult {
                output: Ok(state.counter.value()),
                tag: state.last_log_position,
            });
        }

        let intention_position = self.append_intention(command).await?;
        self.wait_for_completion(intention_position).await
    }
}

impl<T: AgentBus, E: Environment> LinearizableObject for AgentBusCounter<T, E> {
    type Spec = CounterSpec;
    type Tag = i64;

    async fn execute(
        &self,
        id: OperationId,
        operation: CounterOperation,
    ) -> OperationResult<CounterOutput, Self::Tag> {
        self.execute_counter(CounterCommand::new(id, operation))
            .await
            .expect("AgentBus counter adapter should execute")
    }
}

pub struct AgentBusCounterImplementation<F>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
{
    fixture: Rc<F>,
    environment: Rc<Simulator>,
    agent_bus_id: String,
    options: AgentBusCounterOptions,
    stop: Option<Rc<Cell<bool>>>,
    clients: RefCell<Vec<Rc<AgentBusCounter<F::Impl, Simulator>>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LoggedCommand {
    id: OperationId,
    operation: CounterOperation,
    log_position: i64,
}

#[derive(Clone)]
pub enum PollEntries {
    Fixed(i32),
    Random,
    PerClient(Vec<i32>),
}

#[derive(Clone)]
pub struct AgentBusCounterOptions {
    pub skip_commits: bool,
    pub poll_entries: PollEntries,
}

impl Default for AgentBusCounterOptions {
    fn default() -> Self {
        Self {
            skip_commits: false,
            poll_entries: PollEntries::Fixed(1000),
        }
    }
}

impl<F> AgentBusCounterImplementation<F>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
{
    pub fn with_configuration(
        fixture: Rc<F>,
        agent_bus_id: String,
        options: AgentBusCounterOptions,
    ) -> Self {
        let environment = fixture.get_env();
        let stop = (!options.skip_commits).then(|| {
            let stop = Rc::new(Cell::new(false));
            spawn_decider_and_voter(fixture.as_ref(), &environment, &agent_bus_id, stop.clone());
            stop
        });
        Self {
            fixture,
            environment,
            agent_bus_id,
            options,
            stop,
            clients: RefCell::new(Vec::new()),
        }
    }
}

impl<F> LinearizabilityImplementation for AgentBusCounterImplementation<F>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    F::Impl: 'static,
{
    type Object = AgentBusCounter<F::Impl, Simulator>;

    fn environment(&self) -> Rc<Simulator> {
        self.environment.clone()
    }

    fn create(&self, client: usize) -> Rc<Self::Object> {
        let max_poll_entries = match &self.options.poll_entries {
            PollEntries::Fixed(entries) => *entries,
            PollEntries::Random => self.environment.with_rng(|rng| rng.random_range(1..=64)),
            PollEntries::PerClient(entries) => *entries.get(client).unwrap_or_else(|| {
                panic!("PerClient poll_entries missing entry for client {client}")
            }),
        };
        let mut counter = AgentBusCounter::new(
            self.fixture.create_impl(),
            self.agent_bus_id.clone(),
            self.environment.clone(),
        )
        .with_max_poll_entries(max_poll_entries);
        if self.options.skip_commits {
            counter = counter.with_skip_commits();
        }
        let counter = Rc::new(counter);
        self.clients.borrow_mut().push(counter.clone());
        counter
    }

    async fn candidate(
        &self,
        history: &History<
            CounterOperation,
            CounterOutput,
            <Self::Object as LinearizableObject>::Tag,
        >,
    ) -> Result<Vec<OperationId>> {
        let clients = self.clients.borrow().clone();
        let mut command_histories = Vec::with_capacity(clients.len());
        for client in clients {
            command_histories.push(command_history(
                &client.entries().await?,
                self.options.skip_commits,
            )?);
        }
        verify_log_evidence(&history.observations(), &command_histories)?;
        Ok(candidate_from_ordered_tags::<CounterSpec, _>(history))
    }

    fn on_complete(&self) -> Option<Rc<dyn Fn()>> {
        self.stop.as_ref().map(|stop| {
            let stop = stop.clone();
            Rc::new(move || stop.set(true)) as Rc<dyn Fn()>
        })
    }
}

fn command_history(entries: &[BusEntry], skip_commits: bool) -> Result<Vec<LoggedCommand>> {
    let mut intentions = HashMap::new();
    let mut commands = Vec::new();
    for entry in entries {
        let position = log_position(entry)?;
        let Some(payload) = entry
            .payload
            .as_ref()
            .and_then(|payload| payload.payload.as_ref())
        else {
            continue;
        };
        match payload {
            agent_bus_proto_rust::agent_bus::payload::Payload::Intention(_) => {
                if let Some(command) = counter_command(entry)?
                    && command.operation.is_write()
                {
                    if skip_commits {
                        commands.push(LoggedCommand {
                            id: command.id,
                            operation: command.operation,
                            log_position: position,
                        });
                    } else {
                        intentions.insert(position, command);
                    }
                }
            }
            agent_bus_proto_rust::agent_bus::payload::Payload::Commit(commit) if !skip_commits => {
                if let Some(command) = intentions.remove(&commit.intention_id) {
                    commands.push(LoggedCommand {
                        id: command.id,
                        operation: command.operation,
                        log_position: position,
                    });
                }
            }
            agent_bus_proto_rust::agent_bus::payload::Payload::Abort(abort) if !skip_commits => {
                intentions.remove(&abort.intention_id);
            }
            _ => {}
        }
    }
    Ok(commands)
}

fn verify_log_evidence(
    observations: &[Observation<CounterOperation, CounterOutput, i64>],
    command_histories: &[Vec<LoggedCommand>],
) -> Result<()> {
    let command_history = command_histories
        .first()
        .context("AgentBus counter should retain at least one client")?;
    for (client, observed) in command_histories.iter().enumerate().skip(1) {
        anyhow::ensure!(
            observed == command_history,
            "AgentBus client {client} command history differs from client 0: {observed:?} vs {command_history:?}"
        );
    }

    let mut successful_writes: Vec<_> = observations
        .iter()
        .filter(|observation| {
            observation.completed.operation.is_write() && observation.completed.output.is_ok()
        })
        .collect();
    successful_writes.sort_by_key(|observation| observation.completed.tag);
    if let Some(duplicates) = successful_writes
        .windows(2)
        .find(|writes| writes[0].completed.tag == writes[1].completed.tag)
    {
        anyhow::bail!(
            "successful writes {:?} and {:?} have duplicate log position {}",
            duplicates[0].id,
            duplicates[1].id,
            duplicates[0].completed.tag
        );
    }

    anyhow::ensure!(
        successful_writes.len() == command_history.len(),
        "successful write count {} differs from logged command count {}",
        successful_writes.len(),
        command_history.len()
    );
    for (observed, logged) in successful_writes.into_iter().zip(command_history) {
        anyhow::ensure!(
            observed.completed.tag == logged.log_position,
            "operation {:?} reports log position {}, but the log records {}",
            observed.id,
            observed.completed.tag,
            logged.log_position
        );
        anyhow::ensure!(
            observed.id == logged.id,
            "operation {:?} is backed by logged command {:?}",
            observed.id,
            logged.id
        );
        anyhow::ensure!(
            observed.completed.operation == logged.operation,
            "operation {:?} reports {:?}, but the log records {:?}",
            observed.id,
            observed.completed.operation,
            logged.operation
        );
    }
    Ok(())
}

pub fn decisions(entries: &[BusEntry]) -> HashMap<i64, Decision> {
    entries
        .iter()
        .filter_map(|entry| {
            let log_position = entry.header.as_ref()?.log_position;
            match entry.payload.as_ref()?.payload.as_ref()? {
                agent_bus_proto_rust::agent_bus::payload::Payload::Commit(commit) => Some((
                    commit.intention_id,
                    Decision {
                        approved: true,
                        log_position,
                    },
                )),
                agent_bus_proto_rust::agent_bus::payload::Payload::Abort(abort) => Some((
                    abort.intention_id,
                    Decision {
                        approved: false,
                        log_position,
                    },
                )),
                _ => None,
            }
        })
        .collect()
}

pub fn log_position(entry: &BusEntry) -> Result<i64> {
    Ok(entry
        .header
        .as_ref()
        .context("AgentBus entry has no header")?
        .log_position)
}

pub fn last_log_position(entries: &[BusEntry]) -> i64 {
    entries
        .last()
        .and_then(|entry| entry.header.as_ref())
        .map_or(-1, |header| header.log_position)
}

fn spawn_decider_and_voter<F>(
    fixture: &F,
    environment: &Rc<Simulator>,
    agent_bus_id: &str,
    stop: Rc<Cell<bool>>,
) where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
{
    let policy_bus = fixture.create_impl();
    let policy_bus_id = agent_bus_id.to_string();
    environment.spawn_named(
        async move {
            policy_bus
                .append(AppendRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: policy_bus_id,
                    }),
                    payload: Some(Payload {
                        payload: Some(
                            agent_bus_proto_rust::agent_bus::payload::Payload::DeciderPolicy(
                                DeciderPolicy::FirstBooleanWins as i32,
                            ),
                        ),
                    }),
                    ..Default::default()
                })
                .await
                .expect("Policy change should succeed");
        },
        Some("policy_setter".to_string()),
    );

    let decider_bus = fixture.create_impl();
    let decider_environment = environment.clone();
    let decider_bus_id = agent_bus_id.to_string();
    let decider_stop = stop.clone();
    environment.spawn_named(
        async move {
            let mut decider = Decider::new(
                decider_bus,
                decider_bus_id,
                0,
                create_vote_tracker_for_decider_policy,
            );
            while !decider_stop.get() {
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

    let voter = RandomVoter::new(
        fixture.create_impl(),
        agent_bus_id.to_string(),
        environment.clone(),
    );
    environment.spawn_named(
        async move {
            while !stop.get() {
                let timeout_ms = voter.environment.with_rng(|rng| rng.random_range(0..5));
                voter.poll_and_vote_once(timeout_ms).await;
            }
        },
        Some("voter_0".to_string()),
    );
}

struct RandomVoter<T: AgentBus, E: Environment> {
    agent_bus: T,
    agent_bus_id: String,
    environment: Rc<E>,
    next_log_position: RefCell<i64>,
}

impl<T: AgentBus, E: Environment> RandomVoter<T, E> {
    fn new(agent_bus: T, agent_bus_id: String, environment: Rc<E>) -> Self {
        Self {
            agent_bus,
            agent_bus_id,
            environment,
            next_log_position: RefCell::new(0),
        }
    }

    async fn poll_and_vote_once(&self, timeout_ms: i32) {
        let start_log_position = *self.next_log_position.borrow();
        let response = self
            .agent_bus
            .blocking_poll(BlockingPollRequest {
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                start_log_position,
                max_entries: 1000,
                filter: Some(agent_bus_proto_rust::agent_bus::PayloadTypeFilter {
                    payload_types: vec![
                        agent_bus_proto_rust::agent_bus::SelectivePollType::Intention as i32,
                    ],
                }),
                timeout_ms,
            })
            .await
            .expect("blocking_poll should succeed");

        for entry in response.entries {
            let Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(intention)) =
                entry
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.payload.as_ref())
            else {
                continue;
            };
            if !matches!(
                intention.intention,
                Some(agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(_))
            ) {
                continue;
            }
            let intention_id = entry
                .header
                .as_ref()
                .expect("intention should have a header")
                .log_position;
            let vote = self.environment.with_rng(|rng| rng.random_bool(0.5));
            self.agent_bus
                .append(AppendRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: self.agent_bus_id.clone(),
                    }),
                    payload: Some(Payload {
                        payload: Some(agent_bus_proto_rust::agent_bus::payload::Payload::Vote(
                            Vote {
                                abstract_vote: Some(agent_bus_proto_rust::agent_bus::VoteType {
                                    vote_type: Some(
                                        agent_bus_proto_rust::agent_bus::vote_type::VoteType::BooleanVote(
                                            vote,
                                        ),
                                    ),
                                }),
                                intention_id,
                                ..Default::default()
                            },
                        )),
                    }),
                    ..Default::default()
                })
                .await
                .expect("Vote append should succeed");
        }
        *self.next_log_position.borrow_mut() = response.next_start_position;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentbus_api::MonotonicInstant;
    use linearizability::history::CompletedOperation;

    use super::*;

    fn observation(
        id: OperationId,
        operation: CounterOperation,
        output: CounterOutput,
        tag: i64,
    ) -> Observation<CounterOperation, CounterOutput, i64> {
        Observation {
            id,
            start_time: MonotonicInstant::from_duration_since_clock_origin(Duration::ZERO),
            end_time: MonotonicInstant::from_duration_since_clock_origin(Duration::from_millis(1)),
            completed: CompletedOperation {
                operation,
                output,
                tag,
            },
        }
    }

    fn command(id: OperationId, operation: CounterOperation, log_position: i64) -> LoggedCommand {
        LoggedCommand {
            id,
            operation,
            log_position,
        }
    }

    #[test]
    fn rejects_duplicate_successful_write_position_mutation() {
        let first = OperationId {
            client: 0,
            sequence: 0,
        };
        let second = OperationId {
            client: 1,
            sequence: 0,
        };
        let observations = vec![
            observation(first, CounterOperation::Increment, Ok(1), 4),
            observation(second, CounterOperation::Increment, Ok(2), 4),
        ];
        let history = vec![
            command(first, CounterOperation::Increment, 4),
            command(second, CounterOperation::Increment, 4),
        ];

        let error = verify_log_evidence(&observations, &[history])
            .expect_err("duplicate successful write positions should be rejected");

        assert!(error.to_string().contains("duplicate log position"));
    }

    #[test]
    fn rejects_logged_command_count_mutation() {
        let id = OperationId {
            client: 0,
            sequence: 0,
        };
        let observations = vec![observation(id, CounterOperation::Increment, Ok(1), 4)];

        let error = verify_log_evidence(&observations, &[Vec::new()])
            .expect_err("a missing logged command should be rejected");

        assert!(error.to_string().contains("write count"));
    }

    #[test]
    fn rejects_logged_command_identity_mutation() {
        let observed_id = OperationId {
            client: 0,
            sequence: 0,
        };
        let logged_id = OperationId {
            client: 1,
            sequence: 0,
        };
        let observations = vec![observation(
            observed_id,
            CounterOperation::Increment,
            Ok(1),
            4,
        )];
        let history = vec![command(logged_id, CounterOperation::Increment, 4)];

        let error = verify_log_evidence(&observations, &[history])
            .expect_err("a different logged command identity should be rejected");

        assert!(error.to_string().contains("backed by logged command"));
    }

    #[test]
    fn rejects_logged_operation_mutation() {
        let id = OperationId {
            client: 0,
            sequence: 0,
        };
        let observations = vec![observation(id, CounterOperation::Increment, Ok(1), 4)];
        let history = vec![command(id, CounterOperation::Decrement, 4)];

        let error = verify_log_evidence(&observations, &[history])
            .expect_err("a different logged operation should be rejected");

        assert!(error.to_string().contains("the log records Decrement"));
    }

    #[test]
    fn rejects_logged_position_mutation() {
        let id = OperationId {
            client: 0,
            sequence: 0,
        };
        let observations = vec![observation(id, CounterOperation::Increment, Ok(1), 4)];
        let history = vec![command(id, CounterOperation::Increment, 5)];

        let error = verify_log_evidence(&observations, &[history])
            .expect_err("a different logged position should be rejected");

        assert!(error.to_string().contains("reports log position"));
    }

    #[test]
    fn rejects_client_history_length_mutation() {
        let id = OperationId {
            client: 0,
            sequence: 0,
        };
        let observations = vec![observation(id, CounterOperation::Increment, Ok(1), 4)];
        let history = vec![command(id, CounterOperation::Increment, 4)];

        let error = verify_log_evidence(&observations, &[history, Vec::new()])
            .expect_err("clients with different history lengths should be rejected");

        assert!(error.to_string().contains("command history differs"));
    }

    #[test]
    fn rejects_client_history_content_mutation() {
        let id = OperationId {
            client: 0,
            sequence: 0,
        };
        let observations = vec![observation(id, CounterOperation::Increment, Ok(1), 4)];
        let first = vec![command(id, CounterOperation::Increment, 4)];
        let second = vec![command(id, CounterOperation::Decrement, 4)];

        let error = verify_log_evidence(&observations, &[first, second])
            .expect_err("clients with different history contents should be rejected");

        assert!(error.to_string().contains("command history differs"));
    }
}
