/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Counter implementation backed by AgentBus

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BlockingPollRequest;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agentbus_api::AgentBus;
use agentbus_api::environment::Environment;
use rand::RngExt as _;

use super::counter_trait::CommandResult;
use super::counter_trait::Counter;
use super::linearizability_tracker::ExecutedCommand;

enum CounterOp {
    Increment,
    Decrement,
    Noop,
}

impl fmt::Display for CounterOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CounterOp::Increment => write!(f, "inc"),
            CounterOp::Decrement => write!(f, "dec"),
            CounterOp::Noop => write!(f, "noop"),
        }
    }
}

impl CounterOp {
    fn from_string(s: &str) -> Option<Self> {
        match s {
            "inc" => Some(CounterOp::Increment),
            "dec" => Some(CounterOp::Decrement),
            "noop" => Some(CounterOp::Noop),
            _ => None,
        }
    }

    fn needs_commit(&self) -> bool {
        !matches!(self, CounterOp::Noop)
    }
}

pub struct AgentBusCounter<T: AgentBus, E: Environment> {
    agent_bus_impl: T,
    agent_bus_id: String,
    env: Rc<E>,
    state: RefCell<CounterState>,
    skip_commits: bool,
    max_poll_entries: i32,
}

/// A simple sequential counter for use as a linearizability specification.
pub struct SequentialCounter {
    value: RefCell<i64>,
    // Only used by the standalone Counter impl (execute). Not used by AgentBusCounter.
    next_op_id: RefCell<i64>,
    command_history: RefCell<Vec<ExecutedCommand>>,
}

impl SequentialCounter {
    pub fn new() -> Self {
        Self {
            value: RefCell::new(0),
            next_op_id: RefCell::new(0),
            command_history: RefCell::new(Vec::new()),
        }
    }

    pub fn get_value(&self) -> i64 {
        *self.value.borrow()
    }

    pub fn apply_operation(&self, log_position: i64, operation: &str) -> i64 {
        let mut value = self.value.borrow_mut();
        match operation {
            "inc" => *value += 1,
            "dec" => *value -= 1,
            "read" | "noop" => {}
            other => panic!("Unknown operation: {}", other),
        }
        self.command_history.borrow_mut().push(ExecutedCommand {
            log_position,
            operation: operation.to_string(),
        });
        *value
    }

    // Only used by the standalone Counter impl. AgentBusCounter calls apply_operation directly.
    fn execute(&self, operation: &str) -> CommandResult {
        let mut log_position = self.next_op_id.borrow_mut();
        let id = *log_position;
        *log_position += 1;
        drop(log_position);
        let value = self.apply_operation(id, operation);
        CommandResult {
            log_position: id,
            value,
        }
    }
}

impl Counter for SequentialCounter {
    async fn increment(&self) -> Result<CommandResult, String> {
        Ok(self.execute("inc"))
    }

    async fn decrement(&self) -> Result<CommandResult, String> {
        Ok(self.execute("dec"))
    }

    async fn read(&self) -> Result<CommandResult, String> {
        Ok(CommandResult {
            log_position: *self.next_op_id.borrow() - 1,
            value: self.get_value(),
        })
    }

    async fn get_command_history(&self) -> Vec<ExecutedCommand> {
        self.command_history.borrow().clone()
    }
}

struct CounterState {
    counter: SequentialCounter,
    next_log_position: i64,
    buffered_intentions: std::collections::HashMap<i64, CounterOp>,
}

/// Result of processing a single log entry
enum EntryResult {
    Continue,
    FoundMyOp { log_position: i64, value: i64 },
    Aborted,
}

impl<T: AgentBus, E: Environment> AgentBusCounter<T, E> {
    pub fn new(agent_bus_impl: T, agent_bus_id: String, env: Rc<E>, _worker_id: usize) -> Self {
        Self {
            agent_bus_impl,
            agent_bus_id,
            env,
            state: RefCell::new(CounterState {
                counter: SequentialCounter::new(),
                next_log_position: 0,
                buffered_intentions: std::collections::HashMap::new(),
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

    async fn execute_op(&self, op: CounterOp) -> Result<CommandResult, String> {
        let needs_commit = op.needs_commit();

        let my_intention_id = self.append_intention(&op).await;
        let appended_log_position = my_intention_id;

        loop {
            // For noop, return once we've seen any new entries
            if !needs_commit && self.state.borrow().next_log_position > appended_log_position {
                return Ok(CommandResult {
                    log_position: my_intention_id,
                    value: self.state.borrow().counter.get_value(),
                });
            }

            let timeout_ms = self.env.with_rng(|rng| rng.random_range(0..5));
            match self.poll_and_process(my_intention_id, timeout_ms).await {
                EntryResult::FoundMyOp {
                    log_position,
                    value,
                } => {
                    return Ok(CommandResult {
                        log_position,
                        value,
                    });
                }
                EntryResult::Aborted => {
                    return Err(format!("Intention {} was aborted", my_intention_id));
                }
                EntryResult::Continue => {}
            }
        }
    }

    async fn append_intention(&self, op: &CounterOp) -> i64 {
        let intention_payload = Payload {
            payload: Some(
                agent_bus_proto_rust::agent_bus::payload::Payload::Intention(Intention {
                    intention: Some(
                        agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(
                            op.to_string(),
                        ),
                    ),
                    ..Default::default()
                }),
            ),
        };
        let response = self
            .agent_bus_impl
            .append(AppendRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                payload: Some(intention_payload),
                ..Default::default()
            })
            .await
            .expect("Append should succeed");
        response.log_position
    }

    async fn poll_and_process(&self, my_intention_id: i64, timeout_ms: i32) -> EntryResult {
        let start_log_position = self.state.borrow().next_log_position;
        let resp = self
            .agent_bus_impl
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                start_log_position,
                max_entries: self.max_poll_entries,
                filter: None,
                timeout_ms,
            })
            .await
            .expect("blocking_poll should succeed");

        let mut result = EntryResult::Continue;
        for entry in resp.entries {
            let log_position = entry.header.as_ref().unwrap().log_position;

            let entry_result = if self.skip_commits {
                self.process_entry_skip_commits(&entry, log_position, my_intention_id)
            } else {
                self.process_entry_with_commits(&entry, log_position, my_intention_id)
            };

            match entry_result {
                EntryResult::Continue => {}
                other => result = other,
            }
        }
        self.state.borrow_mut().next_log_position = resp.next_start_position;
        result
    }

    fn process_entry_skip_commits(
        &self,
        entry: &BusEntry,
        log_position: i64,
        my_intention_id: i64,
    ) -> EntryResult {
        if let Some(ref payload) = entry.payload {
            if let Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(intention)) =
                &payload.payload
            {
                if let Some(
                    agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(s),
                ) = &intention.intention
                {
                    if let Some(parsed_op) = CounterOp::from_string(s) {
                        if parsed_op.needs_commit() {
                            let value =
                                self.state.borrow().counter.apply_operation(log_position, s);
                            if log_position == my_intention_id {
                                return EntryResult::FoundMyOp {
                                    log_position,
                                    value,
                                };
                            }
                        }
                    }
                }
            }
        }
        EntryResult::Continue
    }

    fn process_entry_with_commits(
        &self,
        entry: &BusEntry,
        log_position: i64,
        my_intention_id: i64,
    ) -> EntryResult {
        let Some(ref payload) = entry.payload else {
            return EntryResult::Continue;
        };

        match &payload.payload {
            Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(intention)) => {
                if let Some(
                    agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(s),
                ) = &intention.intention
                {
                    if let Some(parsed_op) = CounterOp::from_string(s) {
                        if parsed_op.needs_commit() {
                            self.state
                                .borrow_mut()
                                .buffered_intentions
                                .insert(log_position, parsed_op);
                        }
                    }
                }
            }
            Some(agent_bus_proto_rust::agent_bus::payload::Payload::Commit(commit)) => {
                let mut state = self.state.borrow_mut();
                if let Some(op) = state.buffered_intentions.remove(&commit.intention_id) {
                    let value = state.counter.apply_operation(log_position, &op.to_string());
                    if commit.intention_id == my_intention_id {
                        return EntryResult::FoundMyOp {
                            log_position,
                            value,
                        };
                    }
                }
            }
            Some(agent_bus_proto_rust::agent_bus::payload::Payload::Abort(abort)) => {
                self.state
                    .borrow_mut()
                    .buffered_intentions
                    .remove(&abort.intention_id);
                if abort.intention_id == my_intention_id {
                    return EntryResult::Aborted;
                }
            }
            _ => {}
        }
        EntryResult::Continue
    }
}

impl<T: AgentBus, E: Environment> Counter for AgentBusCounter<T, E> {
    async fn increment(&self) -> Result<CommandResult, String> {
        self.execute_op(CounterOp::Increment).await
    }

    async fn decrement(&self) -> Result<CommandResult, String> {
        self.execute_op(CounterOp::Decrement).await
    }

    async fn read(&self) -> Result<CommandResult, String> {
        let tail = self
            .agent_bus_impl
            .check_tail(CheckTailRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
            })
            .await
            .map_err(|e| format!("check_tail failed: {}", e))?
            .tail_position;

        // Drain entries until we get to the tail
        while self.state.borrow().next_log_position < tail {
            self.poll_and_process(-1, 0).await;
        }

        Ok(CommandResult {
            log_position: self.state.borrow().next_log_position - 1,
            value: self.state.borrow().counter.get_value(),
        })
    }

    async fn get_command_history(&self) -> Vec<ExecutedCommand> {
        let _ = self.execute_op(CounterOp::Noop).await;
        self.state.borrow().counter.command_history.borrow().clone()
    }
}
