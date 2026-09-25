/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBus;
use agentbus_api::AgentBusError;
use agentbus_api::BusResult;
use agentbus_api::environment::Clock;
use agentbus_api::environment::Environment;
use agentbus_api::resolve_bus_id;
use tracing::debug;
use tracing::error;

use crate::in_memory_agentbus_state::InMemoryAgentBusState;

pub struct InMemoryAgentBus<E: Environment> {
    state: Rc<RefCell<InMemoryAgentBusState>>,
    environment: Rc<E>,
}

impl<E: Environment> InMemoryAgentBus<E> {
    pub fn new(environment: Rc<E>) -> Self {
        Self {
            state: Rc::new(RefCell::new(InMemoryAgentBusState::new())),
            environment,
        }
    }

    pub fn environment(&self) -> Rc<E> {
        self.environment.clone()
    }
}

impl<E: Environment> Clone for InMemoryAgentBus<E> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            environment: self.environment.clone(),
        }
    }
}

impl<E: Environment> InMemoryAgentBus<E> {
    pub async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        let rt_timestamp_ms = self
            .environment
            .with_clock(|c| c.unsafe_wall_time().as_millis() as i64);
        let result = self
            .state
            .borrow_mut()
            .append(request.clone(), rt_timestamp_ms);

        match &result {
            Ok(response) => {
                debug!(
                    agent_bus_id = bus_id,
                    position = response.log_position,
                    rt_timestamp_ms = rt_timestamp_ms,
                    "Intention added to AgentBus"
                );
            }
            Err(e) => {
                error!(
                    agent_bus_id = bus_id,
                    error = %e,
                    "Error adding intention"
                );
            }
        }

        result
    }

    pub async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        let result = self.state.borrow().poll(request.clone());

        match &result {
            Ok(response) => {
                let current_position = self.state.borrow().get_tail(&bus_id);
                debug!(
                    agent_bus_id = bus_id,
                    start_position = request.start_log_position,
                    max_entries = request.max_entries,
                    entries_found = response.entries.len(),
                    current_position = current_position,
                    complete = response.complete,
                    "Poll for AgentBus"
                );
            }
            Err(e) => {
                error!(
                    agent_bus_id = bus_id,
                    start_position = request.start_log_position,
                    error = %e,
                    "Error polling entries"
                );
            }
        }

        result
    }
}

// Implement the AgentBus trait for InMemoryAgentBus
impl<E: Environment + 'static> AgentBus for InMemoryAgentBus<E> {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        self.append(request).await
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        self.poll(request).await
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        if request.max_entries <= 0 {
            return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                "max_entries must be > 0"
            )));
        }
        if request.start_log_position < 0 {
            return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                "start_log_position must be >= 0"
            )));
        }
        if request.end_log_position < request.start_log_position {
            return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                "end_log_position {} is before start_log_position {}",
                request.end_log_position,
                request.start_log_position
            )));
        }
        if let Some(ref f) = request.filter {
            if f.payload_types.is_empty() {
                return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                    "filter.payload_types must not be empty; omit filter entirely for no filtering"
                )));
            }
        }

        let payload_types = request.filter.as_ref().map(|f| f.payload_types.clone());
        let max_entries = request.max_entries as usize;

        let (entries, next_start_position) = self
            .state
            .borrow()
            .read_filtered_entries(
                bus_id,
                request.start_log_position,
                max_entries,
                &payload_types,
                request.end_log_position,
            )
            .map_err(AgentBusError::Internal)?;
        Ok(ReadNextResponse {
            entries,
            next_start_position,
        })
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        let tail_position = self.state.borrow().get_tail(bus_id);
        Ok(CheckTailResponse { tail_position })
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        agentbus_api::helpers::blocking_poll_default(self, &*self.environment, &request, None, None)
            .await
    }
}
