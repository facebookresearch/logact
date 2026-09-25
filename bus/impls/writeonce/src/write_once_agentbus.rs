/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! WriteOnceAgentBus: An AgentBus backed by a write-once address space.
//!
//! The WriteOnceSpace handles routing via space_id internally. Multiple WriteOnceAgentBus
//! instances can share the same WriteOnceSpace, enabling testing of contention and retry-on-conflict.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBusError;
use agentbus_api::BusResult;
use agentbus_api::TailError;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceSpace;
use agentbus_api::environment::Clock;
use agentbus_api::environment::Environment;
use agentbus_api::payload_matches_filter;
use agentbus_api::resolve_bus_id;
use agentbus_api::validate_bus_id;
use bytes::Bytes;
use prost::Message as ProstMessage;
use tracing::debug;
use tracing::error;

const MAX_POLL_ENTRIES: usize = 64;
// WriteOnceAgentBus only appends at the tail, so writes are always contiguous
// and a window size of 1 is sufficient.
const TAIL_WINDOW_SIZE: u64 = 1;
const DEFAULT_BLOCKING_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
const SPACE_PREFIX: &str = "bus/";

fn prefixed_space_id(bus_id: &str) -> String {
    format!("{}{}", SPACE_PREFIX, bus_id)
}

fn tail_error_to_bus(error: TailError) -> AgentBusError {
    AgentBusError::Unavailable(anyhow::Error::new(error).context("Failed to find tail"))
}

/// Shared state across all clones of a WriteOnceAgentBus
struct WriteOnceAgentBusState<W: WriteOnceSpace, E: Environment> {
    space: W,
    environment: Rc<E>,
    next_positions: HashMap<String, u64>,
    blocking_poll_interval: std::time::Duration,
}

impl<W: WriteOnceSpace, E: Environment> WriteOnceAgentBusState<W, E> {
    fn get_next_position(&mut self, bus_id: &str) -> u64 {
        *self.next_positions.entry(bus_id.to_string()).or_insert(0)
    }

    fn set_next_position(&mut self, bus_id: &str, position: u64) {
        self.next_positions.insert(bus_id.to_string(), position);
    }
}

/// WriteOnceAgentBus - AgentBus backed by a write-once address space.
///
/// This is a lightweight handle that can be cloned. All state is shared
/// via Rc<RefCell<...>>.
///
/// Generic over:
/// - `W: WriteOnceSpace + Clone` - the write-once storage backend (cloneable for async ops)
/// - `E: Environment` - the environment for clock access
pub struct WriteOnceAgentBus<W: WriteOnceSpace + Clone, E: Environment> {
    state: Rc<RefCell<WriteOnceAgentBusState<W, E>>>,
}

impl<W: WriteOnceSpace + Clone, E: Environment> Clone for WriteOnceAgentBus<W, E> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<W: WriteOnceSpace + Clone + 'static, E: Environment> WriteOnceAgentBus<W, E> {
    /// Create a new WriteOnceAgentBus with the given WriteOnceSpace and Environment.
    ///
    /// `blocking_poll_interval` controls how often the backing store is polled
    /// inside `blocking_poll`. Defaults to 50ms if `None`.
    pub fn new(
        space: W,
        environment: Rc<E>,
        blocking_poll_interval: Option<std::time::Duration>,
    ) -> Self {
        Self {
            state: Rc::new(RefCell::new(WriteOnceAgentBusState {
                space,
                environment,
                next_positions: HashMap::new(),
                blocking_poll_interval: blocking_poll_interval
                    .unwrap_or(DEFAULT_BLOCKING_POLL_INTERVAL),
            })),
        }
    }
}

impl<W: WriteOnceSpace + Clone, E: Environment + 'static> agentbus_api::AgentBus
    for WriteOnceAgentBus<W, E>
{
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        validate_bus_id(&bus_id).map_err(AgentBusError::InvalidArgument)?;

        let payload = request.payload.ok_or_else(|| {
            AgentBusError::InvalidArgument(anyhow::anyhow!("Missing or unknown payload in request"))
        })?;

        // Get the space, environment, and current position, releasing the borrow before await
        let (mut space, environment, mut position) = {
            let mut state = self.state.borrow_mut();
            let position = state.get_next_position(&bus_id);
            (state.space.clone(), state.environment.clone(), position)
        };

        let rt_timestamp_ms = environment.with_clock(|c| c.unsafe_wall_time().as_millis() as i64);

        // Try to write at position; if slot is taken, advance and retry.
        // This handles:
        // 1. Crash recovery: on restart, we discover the actual tail
        // 2. Contention: when multiple instances share the same WriteOnceSpace,
        //    they race to claim slots and retry on conflict
        loop {
            let serialized = serialize_entry(&payload, position as i64, rt_timestamp_ms);
            let error = match space
                .write(&prefixed_space_id(&bus_id), position, serialized)
                .await
            {
                Ok(()) => {
                    // Update next_position in state
                    self.state
                        .borrow_mut()
                        .set_next_position(&bus_id, position + 1);

                    debug!(
                        agent_bus_id = bus_id,
                        position = position,
                        rt_timestamp_ms = rt_timestamp_ms,
                        "Intention added to AgentBus"
                    );

                    return Ok(AppendResponse {
                        log_position: position as i64,
                    });
                }
                Err(
                    WriteOnceError::AddressAlreadyExists(_)
                    | WriteOnceError::TransactionConflict(_),
                ) => {
                    // Slot already taken (or likely taken); use tail() to find the first unwritten position
                    let new_position = space
                        .tail(&prefixed_space_id(&bus_id), TAIL_WINDOW_SIZE)
                        .await
                        .map_err(tail_error_to_bus)?;
                    debug!(
                        "WriteOnceAgentBus: position {} taken, retrying at {}",
                        position, new_position
                    );
                    position = new_position;
                    continue;
                }
                Err(error @ WriteOnceError::Timeout(_)) => AgentBusError::Timeout(
                    anyhow::Error::new(error).context("WriteOnceSpace write failed"),
                ),
                Err(error @ WriteOnceError::BackendUnavailable(_)) => AgentBusError::Unavailable(
                    anyhow::Error::new(error).context("WriteOnceSpace write failed"),
                ),
                Err(error @ WriteOnceError::InternalError(_)) => AgentBusError::Internal(
                    anyhow::Error::new(error).context("WriteOnceSpace write failed"),
                ),
            };

            error!(
                agent_bus_id = bus_id,
                error = %format!("{error:#}"),
                "WriteOnceSpace write failed"
            );
            return Err(error);
        }
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        validate_bus_id(&bus_id).map_err(AgentBusError::InvalidArgument)?;
        let max_entries = (request.max_entries as usize).min(MAX_POLL_ENTRIES);
        let start_position = request.start_log_position.max(0) as u64;

        let payload_types = request.filter.as_ref().map(|f| f.payload_types.clone());

        // If filter is set to Some(vec![]), return no entries
        if let Some(ref filter) = payload_types {
            if filter.is_empty() {
                return Ok(PollResponse {
                    entries: vec![],
                    complete: true,
                });
            }
        }

        // Use tail() to find the first unwritten position
        let space = { self.state.borrow().space.clone() };
        let end_position = space
            .tail(&prefixed_space_id(&bus_id), TAIL_WINDOW_SIZE)
            .await
            .map_err(tail_error_to_bus)?;

        // Update next_position in state
        self.state
            .borrow_mut()
            .set_next_position(&bus_id, end_position);

        if end_position == 0 {
            return Ok(PollResponse {
                entries: vec![],
                complete: true,
            });
        }

        let mut entries = Vec::new();
        let mut complete = true;
        let mut pos = start_position;

        while pos < end_position {
            let serialized = space.read(&prefixed_space_id(&bus_id), pos).await;

            if let Some(serialized) = serialized {
                if let Some(stored_entry) = deserialize_entry(&serialized) {
                    // Verify the stored log_position matches the slot we read from
                    if let Some(ref header) = stored_entry.header {
                        if header.log_position != pos as i64 {
                            error!(
                                agent_bus_id = bus_id,
                                expected_position = pos,
                                stored_position = header.log_position,
                                "Stored log_position does not match read position"
                            );
                        }
                    }

                    if let Some(ref payload) = stored_entry.payload {
                        // Apply filter if present
                        if !payload_matches_filter(payload, &payload_types) {
                            pos += 1;
                            continue;
                        }
                    } else {
                        pos += 1;
                        continue;
                    }

                    if entries.len() >= max_entries {
                        complete = false;
                        break;
                    }

                    entries.push(stored_entry);
                }
            }
            pos += 1;
        }

        Ok(PollResponse { entries, complete })
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

        let (entries, next_start) = self
            .read_filtered_entries(
                bus_id,
                request.start_log_position as u64,
                request.max_entries as usize,
                &payload_types,
                request.end_log_position as u64,
            )
            .await?;
        Ok(ReadNextResponse {
            entries,
            next_start_position: next_start as i64,
        })
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        let space = { self.state.borrow().space.clone() };
        let tail_position = space
            .tail(&prefixed_space_id(bus_id), TAIL_WINDOW_SIZE)
            .await
            .map_err(tail_error_to_bus)?;
        Ok(CheckTailResponse {
            tail_position: tail_position as i64,
        })
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        let environment = { self.state.borrow().environment.clone() };
        let interval = { self.state.borrow().blocking_poll_interval };
        agentbus_api::helpers::blocking_poll_default(
            self,
            &*environment,
            &request,
            Some(interval),
            None,
        )
        .await
    }
}

impl<W: WriteOnceSpace + Clone, E: Environment + 'static> WriteOnceAgentBus<W, E> {
    /// Read filtered entries from [start_position, end_position) in the write-once space.
    /// Returns (entries, next_start_position) where next_start_position is the position
    /// after the last returned entry, or end_position if the scan completed without
    /// hitting max_entries.
    async fn read_filtered_entries(
        &self,
        agent_bus_id: &str,
        start_position: u64,
        max_entries: usize,
        payload_types: &Option<Vec<i32>>,
        end_position: u64,
    ) -> BusResult<(Vec<BusEntry>, u64)> {
        let max_entries = max_entries.min(MAX_POLL_ENTRIES);

        if start_position > end_position {
            return Err(AgentBusError::Internal(anyhow::anyhow!(
                "start_position {} is beyond end_position {}",
                start_position,
                end_position
            )));
        }
        if start_position == end_position {
            return Ok((vec![], end_position));
        }

        let space = { self.state.borrow().space.clone() };
        let mut entries = Vec::new();
        let mut pos = start_position;

        while pos < end_position {
            let serialized = space.read(&prefixed_space_id(agent_bus_id), pos).await;

            if let Some(serialized) = serialized {
                if let Some(stored_entry) = deserialize_entry(&serialized) {
                    if let Some(ref header) = stored_entry.header {
                        if header.log_position != pos as i64 {
                            error!(
                                agent_bus_id = agent_bus_id,
                                expected_position = pos,
                                stored_position = header.log_position,
                                "Stored log_position does not match read position"
                            );
                        }
                    }

                    if let Some(ref payload) = stored_entry.payload {
                        if !payload_matches_filter(payload, payload_types) {
                            pos += 1;
                            continue;
                        }
                    } else {
                        pos += 1;
                        continue;
                    }

                    entries.push(stored_entry);
                    if entries.len() >= max_entries {
                        return Ok((entries, pos + 1));
                    }
                }
            }
            pos += 1;
        }

        Ok((entries, end_position))
    }
}

fn serialize_entry(payload: &Payload, log_position: i64, rt_timestamp_ms: i64) -> Bytes {
    let entry = BusEntry {
        header: Some(Header {
            log_position,
            rt_timestamp_ms,
        }),
        payload: Some(payload.clone()),
    };
    Bytes::from(ProstMessage::encode_to_vec(&entry))
}

fn deserialize_entry(bytes: &Bytes) -> Option<BusEntry> {
    <BusEntry as ProstMessage>::decode(&bytes[..]).ok()
}
