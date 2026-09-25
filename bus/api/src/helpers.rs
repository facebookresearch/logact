/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Helper functions for AgentBus implementations

use std::collections::BTreeMap;

use agent_bus_proto_rust::agent_bus::BlockingPollRequest;
use agent_bus_proto_rust::agent_bus::BlockingPollResponse;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::PayloadTypeFilter;
use agent_bus_proto_rust::agent_bus::ReadNextRequest;
use agent_bus_proto_rust::agent_bus::SelectivePollType;
use agent_bus_proto_rust::agent_bus::payload;
use serde_json;

use crate::environment::Clock;
use crate::environment::Environment;

/// Returns the typed bus ID when present, otherwise the legacy string ID.
pub fn resolve_bus_id<'a>(agent_bus_id: &'a str, bus_id: Option<&'a BusId>) -> &'a str {
    bus_id.map_or(agent_bus_id, |bus_id| bus_id.agent_bus_id.as_str())
}

/// Returns the SelectivePollType for a payload, or None if the payload is empty.
pub fn get_payload_type(payload: &Payload) -> Option<i32> {
    match &payload.payload {
        Some(payload::Payload::Intention(_)) => Some(SelectivePollType::Intention as i32),
        Some(payload::Payload::Vote(_)) => Some(SelectivePollType::Vote as i32),
        Some(payload::Payload::DeciderPolicy(_)) => Some(SelectivePollType::DeciderPolicy as i32),
        Some(payload::Payload::VoterPolicy(_)) => Some(SelectivePollType::VoterPolicy as i32),
        Some(payload::Payload::Commit(_)) => Some(SelectivePollType::Commit as i32),
        Some(payload::Payload::Abort(_)) => Some(SelectivePollType::Abort as i32),
        Some(payload::Payload::Control(_)) => Some(SelectivePollType::Control as i32),
        Some(payload::Payload::InferenceInput(_)) => Some(SelectivePollType::InferenceInput as i32),
        Some(payload::Payload::InferenceOutput(_)) => {
            Some(SelectivePollType::InferenceOutput as i32)
        }
        Some(payload::Payload::ActionOutput(_)) => Some(SelectivePollType::ActionOutput as i32),
        Some(payload::Payload::AgentInput(_)) => Some(SelectivePollType::AgentInput as i32),
        Some(payload::Payload::AgentOutput(_)) => Some(SelectivePollType::AgentOutput as i32),
        Some(payload::Payload::Mail(_)) => Some(SelectivePollType::Mail as i32),
        None => None,
    }
}

/// Checks if a payload matches the given filter.
/// Returns true if filter is None (no filtering) or if the payload type is in the filter.
/// Returns false if the payload has no type or doesn't match the filter.
pub fn payload_matches_filter(payload: &Payload, filter: &Option<Vec<i32>>) -> bool {
    match filter {
        None => true,
        Some(types) => match get_payload_type(payload) {
            Some(payload_type) => types.contains(&payload_type),
            None => false,
        },
    }
}

/// Parsed bus entry — payload type name and string content.
///
/// Structured payloads (vote, commit, abort, mail) are JSON-serialized.
/// String payloads (intention, inference_input, etc.) are returned as-is.
pub struct ParsedEntry {
    pub entry_type: &'static str,
    pub content: String,
}

fn voter_config_type(config: Option<&agent_bus_proto_rust::agent_bus::VoterConfig>) -> String {
    config
        .and_then(|config| match config.config.as_ref() {
            Some(agent_bus_proto_rust::agent_bus::voter_config::Config::Llm(_)) => {
                Some("llm".to_string())
            }
            Some(agent_bus_proto_rust::agent_bus::voter_config::Config::RuleBased(_)) => {
                Some("rule_based".to_string())
            }
            Some(agent_bus_proto_rust::agent_bus::voter_config::Config::Custom(any)) => {
                Some(any.type_url.clone())
            }
            None => None,
        })
        .unwrap_or_else(|| "none".to_string())
}

/// Parse a BusEntry's payload into a type name and content string.
///
/// This is the canonical conversion used by both the bus CLI and the kernel
/// server's HTTP API. Adding a new payload type? Add a branch here.
pub fn parse_entry(entry: &BusEntry) -> ParsedEntry {
    use crate::PayloadType;

    let payload = match &entry.payload {
        Some(p) => p,
        None => {
            return ParsedEntry {
                entry_type: "unknown",
                content: String::new(),
            };
        }
    };

    let inner = match &payload.payload {
        Some(p) => p,
        None => {
            return ParsedEntry {
                entry_type: "unknown",
                content: String::new(),
            };
        }
    };

    match inner {
        payload::Payload::Intention(i) => {
            let content = match &i.intention {
                Some(agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(s)) => {
                    s.clone()
                }
                None => String::new(),
            };
            ParsedEntry {
                entry_type: PayloadType::Intention.as_str(),
                content,
            }
        }
        payload::Payload::Vote(v) => {
            let mut fields = serde_json::Map::new();
            fields.insert(
                "intention_id".to_string(),
                serde_json::json!(v.intention_id),
            );
            if let Some(vt) = &v.abstract_vote {
                if let Some(agent_bus_proto_rust::agent_bus::vote_type::VoteType::BooleanVote(b)) =
                    &vt.vote_type
                {
                    fields.insert("boolean_vote".to_string(), serde_json::json!(b));
                }
            }
            if !v.reason.is_empty() {
                fields.insert("reason".to_string(), serde_json::json!(&v.reason));
            }
            fields.insert("voter_id".to_string(), serde_json::json!(&v.voter_id));
            if v.voter_config.is_some() {
                fields.insert(
                    "voter_config_type".to_string(),
                    serde_json::json!(voter_config_type(v.voter_config.as_ref())),
                );
            }
            ParsedEntry {
                entry_type: PayloadType::Vote.as_str(),
                content: serde_json::to_string(&fields).unwrap_or_default(),
            }
        }
        payload::Payload::Commit(c) => ParsedEntry {
            entry_type: PayloadType::Commit.as_str(),
            content: serde_json::json!({
                "intention_id": c.intention_id,
                "reason": c.reason,
            })
            .to_string(),
        },
        payload::Payload::Abort(a) => ParsedEntry {
            entry_type: PayloadType::Abort.as_str(),
            content: serde_json::json!({
                "intention_id": a.intention_id,
                "reason": a.reason,
            })
            .to_string(),
        },
        payload::Payload::InferenceInput(ii) => {
            let content = match &ii.inference_input {
                Some(
                    agent_bus_proto_rust::agent_bus::inference_input::InferenceInput::StringInferenceInput(s),
                ) => s.clone(),
                None => String::new(),
            };
            ParsedEntry {
                entry_type: PayloadType::InferenceInput.as_str(),
                content,
            }
        }
        payload::Payload::InferenceOutput(io) => {
            let content = match &io.inference_output {
                Some(
                    agent_bus_proto_rust::agent_bus::inference_output::InferenceOutput::StringInferenceOutput(
                        s,
                    ),
                ) => s.clone(),
                None => String::new(),
            };
            ParsedEntry {
                entry_type: PayloadType::InferenceOutput.as_str(),
                content,
            }
        }
        payload::Payload::ActionOutput(ao) => {
            let content = match &ao.action_output {
                Some(
                    agent_bus_proto_rust::agent_bus::action_output::ActionOutput::StringActionOutput(
                        s,
                    ),
                ) => s.clone(),
                None => String::new(),
            };
            ParsedEntry {
                entry_type: PayloadType::ActionOutput.as_str(),
                content,
            }
        }
        payload::Payload::AgentInput(ai) => {
            let content = match &ai.agent_input {
                Some(
                    agent_bus_proto_rust::agent_bus::agent_input::AgentInput::StringAgentInput(s),
                ) => s.clone(),
                None => String::new(),
            };
            ParsedEntry {
                entry_type: PayloadType::AgentInput.as_str(),
                content,
            }
        }
        payload::Payload::AgentOutput(ao) => {
            let content = match &ao.agent_output {
                Some(
                    agent_bus_proto_rust::agent_bus::agent_output::AgentOutput::StringAgentOutput(
                        s,
                    ),
                ) => s.clone(),
                None => String::new(),
            };
            ParsedEntry {
                entry_type: PayloadType::AgentOutput.as_str(),
                content,
            }
        }
        payload::Payload::VoterPolicy(vp) => {
            let content = vp
                .config
                .as_ref()
                .map(|any| format!("type={}", any.type_url))
                .unwrap_or_else(|| "no config".to_string());
            ParsedEntry {
                entry_type: PayloadType::VoterPolicy.as_str(),
                content,
            }
        }
        payload::Payload::Control(c) => {
            let content = match &c.control {
                Some(agent_bus_proto_rust::agent_bus::control::Control::BaseEngineControl(
                    control,
                )) => match control.control.as_ref() {
                    Some(
                        agent_bus_proto_rust::agent_bus::base_engine_control::Control::AddVoter(
                            add,
                        ),
                    ) => serde_json::json!({
                        "control_type": "base_engine_control",
                        "base_engine_control_type": "add_voter",
                        "config_type": voter_config_type(add.config.as_ref()),
                    })
                    .to_string(),
                    Some(
                        agent_bus_proto_rust::agent_bus::base_engine_control::Control::RemoveVoter(
                            remove,
                        ),
                    ) => serde_json::json!({
                        "control_type": "base_engine_control",
                        "base_engine_control_type": "remove_voter",
                        "voter_id": remove.voter_id,
                    })
                    .to_string(),
                    Some(
                        agent_bus_proto_rust::agent_bus::base_engine_control::Control::PolicyBatch(
                            batch,
                        ),
                    ) => {
                        let voter_ops = batch
                            .voter_ops
                            .iter()
                            .map(|(voter_id, op)| {
                                let value = match op.op.as_ref() {
                                    Some(agent_bus_proto_rust::agent_bus::voter_op::Op::Add(
                                        add,
                                    )) => serde_json::json!({
                                        "operation": "add",
                                        "config_type": voter_config_type(add.config.as_ref()),
                                    }),
                                    Some(
                                        agent_bus_proto_rust::agent_bus::voter_op::Op::Remove(_),
                                    ) => serde_json::json!({ "operation": "remove" }),
                                    None => serde_json::json!({ "operation": "none" }),
                                };
                                (voter_id, value)
                            })
                            .collect::<BTreeMap<_, _>>();
                        serde_json::json!({
                            "control_type": "base_engine_control",
                            "base_engine_control_type": "policy_batch",
                            "expected_current_version": batch.expected_current_version,
                            "new_version": batch.new_version,
                            "decider_policy": batch.decider_policy,
                            "voter_ops": voter_ops,
                        })
                        .to_string()
                    }
                    None => serde_json::json!({
                        "control_type": "base_engine_control",
                        "base_engine_control_type": "none",
                    })
                    .to_string(),
                },
                None => String::new(),
            };
            ParsedEntry {
                entry_type: PayloadType::Control.as_str(),
                content,
            }
        }
        payload::Payload::DeciderPolicy(dp) => ParsedEntry {
            entry_type: PayloadType::DeciderPolicy.as_str(),
            content: dp.to_string(),
        },
        payload::Payload::Mail(m) => {
            let (body, msg_id, reply_to) = match &m.content {
                Some(agent_bus_proto_rust::agent_bus::mail::Content::Message(msg)) => {
                    (msg.body.clone(), msg.message_id.clone(), String::new())
                }
                Some(agent_bus_proto_rust::agent_bus::mail::Content::Reply(r)) => {
                    let (body, mid) = r
                        .message
                        .as_ref()
                        .map(|msg| (msg.body.clone(), msg.message_id.clone()))
                        .unwrap_or_default();
                    (body, mid, r.in_reply_to.clone())
                }
                None => (String::new(), String::new(), String::new()),
            };
            ParsedEntry {
                entry_type: PayloadType::Mail.as_str(),
                content: serde_json::json!({
                    "from": m.sender_agent_id,
                    "content": body,
                    "message_id": msg_id,
                    "in_reply_to": reply_to,
                })
                .to_string(),
            }
        }
    }
}

const DEFAULT_MAX_BLOCKING_TIMEOUT_MS: i32 = 30_000;
const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// Drain up to `max_entries` entries from `[start_log_position, end_log_position)`
/// via a loop of `read_next` calls. Returns `(entries, final_cursor)`.
pub async fn read_range<B: crate::AgentBus>(
    bus: &B,
    agent_bus_id: &str,
    start_log_position: i64,
    end_log_position: i64,
    max_entries: i32,
    filter: Option<PayloadTypeFilter>,
) -> crate::BusResult<(Vec<BusEntry>, i64)> {
    let mut cursor = start_log_position;
    let mut remaining = max_entries;
    let mut entries: Vec<BusEntry> = Vec::new();
    while cursor < end_log_position && remaining > 0 {
        let resp = bus
            .read_next(ReadNextRequest {
                agent_bus_id: agent_bus_id.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: agent_bus_id.to_string(),
                }),
                start_log_position: cursor,
                end_log_position,
                max_entries: remaining,
                filter: filter.clone(),
            })
            .await?;
        // Defensive: read_next guarantees forward progress.
        if resp.next_start_position <= cursor {
            return Err(crate::AgentBusError::Internal(anyhow::anyhow!(
                "read_next did not advance cursor (stuck at {})",
                cursor
            )));
        }
        remaining -= resp.entries.len() as i32;
        cursor = resp.next_start_position;
        entries.extend(resp.entries);
    }
    Ok((entries, cursor))
}

/// Default implementation of blocking_poll.
/// Polls check_tail at `interval` until tail > start_log_position or timeout expires,
/// then drains [start, tail) via read_next.
pub async fn blocking_poll_default<B: crate::AgentBus, E: Environment>(
    bus: &B,
    env: &E,
    request: &BlockingPollRequest,
    interval: Option<std::time::Duration>,
    max_timeout_ms: Option<i32>,
) -> crate::BusResult<BlockingPollResponse> {
    if request.timeout_ms < 0 {
        return Err(crate::AgentBusError::InvalidArgument(anyhow::anyhow!(
            "timeout_ms must be >= 0"
        )));
    }
    if request.max_entries <= 0 {
        return Err(crate::AgentBusError::InvalidArgument(anyhow::anyhow!(
            "max_entries must be > 0"
        )));
    }

    let max = max_timeout_ms.unwrap_or(DEFAULT_MAX_BLOCKING_TIMEOUT_MS);
    let timeout_ms = request.timeout_ms.min(max);
    let deadline = env.with_clock(|c| c.monotonic_time())
        + std::time::Duration::from_millis(timeout_ms as u64);

    // Wait for tail to advance past start_log_position
    loop {
        let tail = bus
            .check_tail(CheckTailRequest {
                agent_bus_id: request.agent_bus_id.clone(),
                bus_id: request.bus_id.clone(),
            })
            .await?
            .tail_position;

        if tail > request.start_log_position {
            // New data
            let resp = bus
                .read_next(ReadNextRequest {
                    agent_bus_id: request.agent_bus_id.clone(),
                    bus_id: request.bus_id.clone(),
                    start_log_position: request.start_log_position,
                    end_log_position: tail,
                    max_entries: request.max_entries,
                    filter: request.filter.clone(),
                })
                .await?;
            return Ok(BlockingPollResponse {
                entries: resp.entries,
                next_start_position: resp.next_start_position,
            });
        }

        let now = env.with_clock(|c| c.monotonic_time());
        if now >= deadline {
            return Ok(BlockingPollResponse {
                entries: vec![],
                next_start_position: request.start_log_position,
            });
        }
        let interval = interval.unwrap_or(DEFAULT_POLL_INTERVAL);
        env.sleep(interval.min(deadline - now)).await;
    }
}
