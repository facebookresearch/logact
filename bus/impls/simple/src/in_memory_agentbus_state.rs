/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBusError;
use agentbus_api::BusResult;
use agentbus_api::payload_matches_filter;
use agentbus_api::resolve_bus_id;
use agentbus_api::validate_bus_id;
use anyhow::Result;

/// Maximum number of entries that can be returned in a single poll request
const MAX_POLL_ENTRIES: usize = 64;

/// In-memory state implementation for AgentBus service
#[derive(Debug, Default)]
pub struct InMemoryAgentBusState {
    // Server side state
    // TODO: Consider Vec<Option<Payload>> if we need to model
    // holes in the logs.
    buses: HashMap<String, Vec<BusEntry>>,
}

impl InMemoryAgentBusState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(
        &mut self,
        request: AppendRequest,
        rt_timestamp_ms: i64,
    ) -> BusResult<AppendResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        // Validate bus ID
        validate_bus_id(&bus_id).map_err(AgentBusError::InvalidArgument)?;

        let payload = request.payload.ok_or_else(|| {
            AgentBusError::InvalidArgument(anyhow::anyhow!("Missing or unknown payload in request"))
        })?;

        let bus = self.buses.entry(bus_id).or_insert_with(Vec::new);
        let position = bus.len() as i64;

        let entry = BusEntry {
            header: Some(Header {
                log_position: position,
                rt_timestamp_ms,
            }),
            payload: Some(payload),
        };

        bus.push(entry);

        Ok(AppendResponse {
            log_position: position,
        })
    }

    pub fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        validate_bus_id(&bus_id).map_err(AgentBusError::InvalidArgument)?;
        let max_entries = (request.max_entries as usize).min(MAX_POLL_ENTRIES);

        let payload_types = request.filter.as_ref().map(|f| f.payload_types.clone());

        let (entries, complete) = self
            .fetch_entries(
                bus_id,
                request.start_log_position,
                max_entries,
                &payload_types,
            )
            .map_err(AgentBusError::Internal)?;

        Ok(PollResponse { entries, complete })
    }

    // TODO: this is terribly inefficient right now; implement indexes for the entry filters
    // Returns:
    //   Vec<BusEntry>: the entries matching the request
    //   bool `complete`: whether all matching entries were returned, false if more available
    fn fetch_entries(
        &self,
        agent_bus_id: String,
        start_position: i64,
        max_entries: usize,
        payload_types: &Option<Vec<i32>>,
    ) -> Result<(Vec<BusEntry>, bool)> {
        let entries = self
            .buses
            .get(&agent_bus_id)
            .map(|bus| {
                let start_idx = start_position.max(0) as usize;

                let slice = if start_idx < bus.len() {
                    &bus[start_idx..]
                } else {
                    &[]
                };

                // If filter is set to Some(vec![]), return no entries
                if let Some(filter) = payload_types {
                    if filter.is_empty() {
                        return (vec![], true);
                    }
                }

                let mut result = Vec::new();
                let mut complete = true;

                for entry in slice {
                    // Apply filter if present
                    if let Some(ref payload) = entry.payload {
                        if !payload_matches_filter(payload, payload_types) {
                            continue;
                        }
                    } else {
                        continue; // Skip entries with no payload
                    }

                    if result.len() >= max_entries {
                        // complete=false because there are more entries matching the request to add
                        complete = false;
                        break;
                    }

                    result.push(entry.clone());
                }

                (result, complete)
            })
            .unwrap_or((Vec::new(), true));

        Ok(entries)
    }

    pub fn get_tail(&self, agent_bus_id: &str) -> i64 {
        self.buses
            .get(agent_bus_id)
            .map_or(0, |bus| bus.len() as i64)
    }

    /// Read filtered entries from [start_position, end_position).
    /// Returns (matching entries, next_start_position).
    /// Assumes argument validation has already been performed.
    pub fn read_filtered_entries(
        &self,
        agent_bus_id: &str,
        start_position: i64,
        max_entries: usize,
        payload_types: &Option<Vec<i32>>,
        end_position: i64,
    ) -> Result<(Vec<BusEntry>, i64)> {
        if start_position > end_position {
            return Err(anyhow::anyhow!(
                "start_position {} is beyond end_position {}",
                start_position,
                end_position
            ));
        }
        if start_position == end_position {
            return Ok((vec![], end_position));
        }

        let bus = match self.buses.get(agent_bus_id) {
            Some(bus) => bus,
            None => return Ok((vec![], end_position)),
        };

        let start_idx = start_position as usize;
        let end_idx = (end_position as usize).min(bus.len());
        let slice = &bus[start_idx..end_idx];

        let mut entries = Vec::new();
        for entry in slice {
            if let Some(ref payload) = entry.payload {
                if !payload_matches_filter(payload, payload_types) {
                    continue;
                }
            } else {
                continue;
            }

            entries.push(entry.clone());
            if entries.len() >= max_entries {
                let next = entry
                    .header
                    .as_ref()
                    .expect("entry should have header")
                    .log_position
                    + 1;
                return Ok((entries, next));
            }
        }

        Ok((entries, end_position))
    }
}
