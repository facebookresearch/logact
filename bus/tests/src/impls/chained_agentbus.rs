/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Chain-replicated AgentBus implementation with correct and buggy modes.
//!
//! Correct mode:
//! - Writes to all buses in order (with delays between writes)
//! - Polls from the tail bus (last in chain)
//! - Linearizable because all reads see a consistent ordering from the tail
//!
//! BuggyPoll mode (for testing linearizability detection):
//! - Writes to all buses in order (with delays)
//! - Each client polls from its own bus instead of the tail
//! - Causes linearizability violations because different clients may see
//!   different orderings due to write interleaving across buses

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBus;
use agentbus_api::BusResult;
use agentbus_api::environment::Environment;
use agentbus_simple::InMemoryAgentBus;
use rand::RngExt as _;

use crate::simulator::Simulator;

/// Mode of operation for the ChainedAgentBus
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainedMode {
    /// Correct chain replication: write to all buses, read from tail
    Correct,
    /// Buggy polls: write to all with delays (allows interleaving), read from own bus
    BuggyPoll,
}

/// Shared state for tracking the chain of buses
struct SharedState {
    buses: Vec<InMemoryAgentBus<Simulator>>,
    next_client_id: usize,
}

impl SharedState {
    fn new(env: Rc<Simulator>, ttl: Option<Duration>) -> Self {
        let bus = || match ttl {
            Some(ttl) => InMemoryAgentBus::with_ttl(env.clone(), ttl),
            None => InMemoryAgentBus::new(env.clone()),
        };
        let buses = vec![bus(), bus()];
        Self {
            buses,
            next_client_id: 0,
        }
    }

    fn next_client(&mut self) -> usize {
        let id = self.next_client_id;
        self.next_client_id += 1;
        id
    }
}

/// Chain-replicated AgentBus with configurable correct/buggy behavior
pub struct ChainedAgentBus {
    state: Rc<RefCell<SharedState>>,
    env: Rc<Simulator>,
    client_id: usize,
    mode: ChainedMode,
}

impl ChainedAgentBus {
    /// Build a chain in `mode`. With a TTL, its buses expire each entry `ttl` after
    /// it is appended.
    pub fn new(env: Rc<Simulator>, mode: ChainedMode, ttl: Option<Duration>) -> Self {
        let state = Rc::new(RefCell::new(SharedState::new(env.clone(), ttl)));
        let client_id = state.borrow_mut().next_client();
        Self {
            state,
            env,
            client_id,
            mode,
        }
    }

    pub fn environment(&self) -> Rc<Simulator> {
        self.env.clone()
    }
}

impl Clone for ChainedAgentBus {
    fn clone(&self) -> Self {
        let client_id = self.state.borrow_mut().next_client();
        Self {
            state: self.state.clone(),
            env: self.env.clone(),
            client_id,
            mode: self.mode,
        }
    }
}

impl AgentBus for ChainedAgentBus {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        self.append_with_delays(request).await
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        match self.mode {
            ChainedMode::Correct => self.poll_correct(request).await,
            ChainedMode::BuggyPoll => self.poll_own_bus(request).await,
        }
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        match self.mode {
            ChainedMode::Correct => {
                let tail_bus = {
                    let state = self.state.borrow();
                    state
                        .buses
                        .last()
                        .expect("Should have at least one bus")
                        .clone()
                };
                tail_bus.read_next(request).await
            }
            ChainedMode::BuggyPoll => {
                let my_bus_idx = self.client_id % 2;
                let my_bus = self.state.borrow().buses[my_bus_idx].clone();
                my_bus.read_next(request).await
            }
        }
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        match self.mode {
            ChainedMode::Correct => {
                let tail_bus = {
                    let state = self.state.borrow();
                    state
                        .buses
                        .last()
                        .expect("Should have at least one bus")
                        .clone()
                };
                tail_bus.check_tail(request).await
            }
            ChainedMode::BuggyPoll => {
                let my_bus_idx = self.client_id % 2;
                let my_bus = self.state.borrow().buses[my_bus_idx].clone();
                my_bus.check_tail(request).await
            }
        }
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        match self.mode {
            ChainedMode::Correct => {
                let tail_bus = {
                    let state = self.state.borrow();
                    state
                        .buses
                        .last()
                        .expect("Should have at least one bus")
                        .clone()
                };
                tail_bus.blocking_poll(request).await
            }
            ChainedMode::BuggyPoll => {
                let my_bus_idx = self.client_id % 2;
                let my_bus = self.state.borrow().buses[my_bus_idx].clone();
                my_bus.blocking_poll(request).await
            }
        }
    }
}

impl ChainedAgentBus {
    /// Poll from the tail bus (correct behavior)
    async fn poll_correct(&self, request: PollRequest) -> BusResult<PollResponse> {
        let tail_bus = {
            let state = self.state.borrow();
            state
                .buses
                .last()
                .expect("Should have at least one bus")
                .clone()
        };
        tail_bus.poll(request).await
    }

    /// Append to all buses in order, but with random delays between each write.
    /// This simulates network delays and allows interleaving between concurrent appends,
    /// causing different buses to see different orderings.
    async fn append_with_delays(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        let buses = self.state.borrow().buses.to_vec();
        let mut response = None;
        for bus in buses {
            let delay = self.env.with_rng(|rng| rng.random_range(1..10));
            self.env.sleep(Duration::from_millis(delay)).await;
            response = Some(bus.append(request.clone()).await?);
        }
        Ok(response.expect("Should have at least one bus"))
    }

    /// Buggy poll: poll from own bus only
    async fn poll_own_bus(&self, request: PollRequest) -> BusResult<PollResponse> {
        let my_bus_idx = self.client_id % 2;
        let my_bus = self.state.borrow().buses[my_bus_idx].clone();
        my_bus.poll(request).await
    }
}

#[cfg(test)]
mod tests {
    use agentbus_simulator::LatencyConfig;

    use super::*;
    use crate::common::helpers::append_string_intention;
    use crate::impls::latency_wrapper::LatencyWrapper;

    #[test]
    fn latency_wrapper_preserves_client_routing() {
        for latency in [
            None,
            Some(LatencyConfig {
                max_latency: Duration::from_millis(20),
                loss_fraction: 0.0,
            }),
        ] {
            let env = Rc::new(Simulator::new(0).with_latency(latency));
            let bus = ChainedAgentBus::new(env.clone(), ChainedMode::BuggyPoll, None);
            let head = bus.state.borrow().buses[0].clone();
            let bus = LatencyWrapper::new(bus, env.clone());
            let other_client = bus.clone();
            env.spawn(async move {
                append_string_intention(&head, "routing".to_owned(), "entry".to_owned()).await;
                let bus_id = BusId {
                    agent_bus_id: "routing".to_owned(),
                };
                for (client, expected_tail) in [(&bus, 1), (&other_client, 0)] {
                    for _ in 0..2 {
                        let tail = client
                            .check_tail(CheckTailRequest {
                                bus_id: Some(bus_id.clone()),
                            })
                            .await
                            .expect("checking the replica tail should succeed");
                        assert_eq!(
                            tail.tail_position, expected_tail,
                            "each client should keep reading its own replica"
                        );
                    }
                    let entries = client
                        .blocking_poll(BlockingPollRequest {
                            bus_id: Some(bus_id.clone()),
                            start_log_position: 0,
                            max_entries: 1,
                            filter: None,
                            timeout_ms: 0,
                        })
                        .await
                        .expect("polling the replica should succeed")
                        .entries;
                    assert_eq!(
                        entries.len(),
                        expected_tail as usize,
                        "blocking polls should use the same replica as other calls"
                    );
                }
            });
            env.run();
        }
    }
}
