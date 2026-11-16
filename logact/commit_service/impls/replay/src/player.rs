/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::Payload;
use agentbus_api::AgentBus;
use agentbus_api::helpers::read_range;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitResult;

const READ_BATCH_SIZE: i32 = 64;

/// Plays one bus forward from a start position in bounded batches.
pub struct LogPlayer<'a, T> {
    bus: &'a T,
    bus_id: &'a str,
    next: i64,
    /// The tail last observed with `check_tail`.
    tail: i64,
}

impl<'a, T: AgentBus> LogPlayer<'a, T> {
    pub fn new(bus: &'a T, bus_id: &'a str, start: i64) -> Self {
        Self {
            bus,
            bus_id,
            next: start,
            tail: start,
        }
    }

    /// The next batch of entries, or an empty batch once the player is at the
    /// tail.
    pub async fn next_batch(&mut self) -> CommitResult<Vec<BusEntry>> {
        loop {
            if self.next >= self.tail {
                self.tail = self
                    .bus
                    .check_tail(CheckTailRequest {
                        bus_id: Some(BusId {
                            agent_bus_id: self.bus_id.to_owned(),
                        }),
                    })
                    .await
                    .map_err(CommitError::from_bus_error)?
                    .tail_position;
                if self.next >= self.tail {
                    return Ok(Vec::new());
                }
            }
            let (entries, next) = read_range(
                self.bus,
                self.bus_id,
                self.next,
                self.tail,
                READ_BATCH_SIZE,
                None,
            )
            .await
            .map_err(CommitError::from_bus_error)?;
            self.next = next;
            if !entries.is_empty() {
                return Ok(entries);
            }
        }
    }
}

pub(crate) async fn append(
    bus: &impl AgentBus,
    bus_id: &str,
    payload: Payload,
) -> CommitResult<i64> {
    Ok(bus
        .append(AppendRequest {
            bus_id: Some(BusId {
                agent_bus_id: bus_id.to_owned(),
            }),
            payload: Some(payload),
        })
        .await
        .map_err(CommitError::from_bus_error)?
        .log_position)
}

pub fn entry_position(entry: &BusEntry) -> CommitResult<i64> {
    entry
        .header
        .as_ref()
        .map(|header| header.log_position)
        .ok_or_else(|| {
            CommitError::Internal(anyhow::anyhow!(
                "bus entry has no header; cannot determine its log position"
            ))
        })
}
