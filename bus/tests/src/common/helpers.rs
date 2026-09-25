/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBus;
use agentbus_api::environment::Environment;
use rand::RngExt as _;

pub const ALL_PAYLOAD_SELECTIVE_POLL_TYPES: &[i32] = &[
    SelectivePollType::Intention as i32,
    SelectivePollType::Vote as i32,
    SelectivePollType::DeciderPolicy as i32,
    SelectivePollType::Commit as i32,
    SelectivePollType::Abort as i32,
    SelectivePollType::VoterPolicy as i32,
    SelectivePollType::Control as i32,
    SelectivePollType::InferenceInput as i32,
    SelectivePollType::InferenceOutput as i32,
    SelectivePollType::ActionOutput as i32,
    SelectivePollType::AgentInput as i32,
    SelectivePollType::AgentOutput as i32,
    SelectivePollType::Mail as i32,
];

pub async fn append_string_intention<T: AgentBus>(
    impl_instance: &T,
    agent_bus_id: String,
    content: String,
) -> i64 {
    let payload = Payload {
        payload: Some(payload::Payload::Intention(Intention {
            intention: Some(intention::Intention::StringIntention(content)),
            ..Default::default()
        })),
    };
    let request = AppendRequest {
        agent_bus_id: agent_bus_id.clone(),
        bus_id: Some(BusId { agent_bus_id }),
        payload: Some(payload),
        ..Default::default()
    };
    impl_instance
        .append(request)
        .await
        .expect("Append should succeed")
        .log_position
}

pub async fn append_decider_policy<T: AgentBus>(
    impl_instance: &T,
    agent_bus_id: String,
    decider_policy: i32,
) -> i64 {
    let payload = Payload {
        payload: Some(payload::Payload::DeciderPolicy(decider_policy)),
    };
    let request = AppendRequest {
        agent_bus_id: agent_bus_id.clone(),
        bus_id: Some(BusId { agent_bus_id }),
        payload: Some(payload),
        ..Default::default()
    };
    impl_instance
        .append(request)
        .await
        .expect("Append should succeed")
        .log_position
}

pub async fn append_vote<T: AgentBus>(
    impl_instance: &T,
    agent_bus_id: String,
    intention_id: i64,
    vote: bool,
) -> i64 {
    let payload = Payload {
        payload: Some(payload::Payload::Vote(Vote {
            intention_id,
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(vote)),
            }),
            ..Default::default()
        })),
    };
    let request = AppendRequest {
        agent_bus_id: agent_bus_id.clone(),
        bus_id: Some(BusId { agent_bus_id }),
        payload: Some(payload),
        ..Default::default()
    };
    impl_instance
        .append(request)
        .await
        .expect("Vote should succeed")
        .log_position
}

pub async fn append_commit<T: AgentBus>(
    impl_instance: &T,
    agent_bus_id: String,
    intention_id: i64,
    reason: &str,
) -> i64 {
    let payload = Payload {
        payload: Some(payload::Payload::Commit(Commit {
            intention_id,
            reason: reason.to_string(),
            ..Default::default()
        })),
    };
    let request = AppendRequest {
        agent_bus_id: agent_bus_id.clone(),
        bus_id: Some(BusId { agent_bus_id }),
        payload: Some(payload),
        ..Default::default()
    };
    impl_instance
        .append(request)
        .await
        .expect("Commit should succeed")
        .log_position
}

pub async fn poll<T: AgentBus>(
    impl_instance: &T,
    agent_bus_id: String,
    start_log_position: i64,
    max_entries: i16,
) -> PollResponse {
    let poll_request = PollRequest {
        agent_bus_id: agent_bus_id.clone(),
        bus_id: Some(BusId { agent_bus_id }),
        start_log_position,
        max_entries: max_entries as i32,
        filter: None, // No filter means return all entries
        ..Default::default()
    };
    impl_instance
        .poll(poll_request)
        .await
        .expect("Poll should succeed")
}

pub async fn poll_selective<T: AgentBus>(
    impl_instance: &T,
    agent_bus_id: String,
    start_log_position: i64,
    max_entries: i16,
    payload_types: Vec<i32>,
) -> PollResponse {
    let poll_request = PollRequest {
        agent_bus_id: agent_bus_id.clone(),
        bus_id: Some(BusId { agent_bus_id }),
        start_log_position,
        max_entries: max_entries as i32,
        filter: Some(PayloadTypeFilter { payload_types }),
        ..Default::default()
    };
    impl_instance
        .poll(poll_request)
        .await
        .expect("Poll should succeed")
}

/// Reads all entries from [start, tail) via check_tail + read_next.
/// Establishes tail upfront, drains until cursor == tail.
/// If `max_page_size` is None, page size is randomized per iteration.
/// Returns (entries, tail).
pub async fn read_linearizable_snapshot<T: AgentBus, E: Environment>(
    bus: &T,
    agent_bus_id: String,
    start: i64,
    filter: Option<PayloadTypeFilter>,
    env: &E,
    max_page_size: Option<i32>,
) -> (Vec<BusEntry>, i64) {
    let tail = bus
        .check_tail(CheckTailRequest {
            agent_bus_id: agent_bus_id.clone(),
            bus_id: Some(BusId {
                agent_bus_id: agent_bus_id.clone(),
            }),
        })
        .await
        .expect("check_tail should succeed")
        .tail_position;

    let mut entries = Vec::new();
    let mut cursor = start;
    while cursor < tail {
        let use_blocking_poll = env.with_rng(|rng| rng.random_bool(0.5));
        let page_size =
            max_page_size.unwrap_or_else(|| env.with_rng(|rng| 1 << rng.random_range(0..8)));

        if use_blocking_poll {
            let resp = bus
                .blocking_poll(BlockingPollRequest {
                    agent_bus_id: agent_bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: agent_bus_id.clone(),
                    }),
                    start_log_position: cursor,
                    max_entries: page_size,
                    filter: filter.clone(),
                    timeout_ms: 100,
                })
                .await
                .expect("blocking_poll should succeed");
            assert_read_response_invariants(&resp.entries, cursor, resp.next_start_position, None);
            entries.extend(resp.entries);
            cursor = resp.next_start_position;
        } else {
            let resp = bus
                .read_next(ReadNextRequest {
                    agent_bus_id: agent_bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: agent_bus_id.clone(),
                    }),
                    start_log_position: cursor,
                    end_log_position: tail,
                    max_entries: page_size,
                    filter: filter.clone(),
                })
                .await
                .expect("read_next should succeed");
            assert_read_response_invariants(
                &resp.entries,
                cursor,
                resp.next_start_position,
                Some(tail),
            );
            entries.extend(resp.entries);
            cursor = resp.next_start_position;
        }
    }
    assert_eq!(
        cursor, tail,
        "read_linearizable_snapshot: cursor should equal tail after draining"
    );
    (entries, tail)
}

/// Asserts structural invariants on a read_next / blocking_poll response:
/// - entries fit within [start_log_position, next_start_position)
/// - next_start_position <= end_log_position (when provided, i.e. read_next)
/// - First entry's position >= start_log_position
/// - Last entry's position < next_start_position
/// - Log positions are strictly increasing
pub fn assert_read_response_invariants(
    entries: &[BusEntry],
    start_log_position: i64,
    next_start_position: i64,
    end_log_position: Option<i64>,
) {
    assert!(
        (entries.len() as i64) <= next_start_position - start_log_position,
        "entries.len() ({}) > next_start_position ({}) - start_log_position ({})",
        entries.len(),
        next_start_position,
        start_log_position,
    );
    if let Some(end) = end_log_position {
        assert!(
            next_start_position <= end,
            "next_start_position ({}) > end_log_position ({})",
            next_start_position,
            end,
        );
    }
    if entries.is_empty() {
        return;
    }
    let first_pos = entries[0]
        .header
        .as_ref()
        .expect("entry should have header")
        .log_position;
    assert!(
        first_pos >= start_log_position,
        "first entry position ({}) < start_log_position ({})",
        first_pos,
        start_log_position,
    );
    let last_pos = entries
        .last()
        .unwrap()
        .header
        .as_ref()
        .expect("entry should have header")
        .log_position;
    assert!(
        last_pos < next_start_position,
        "last entry position ({}) >= next_start_position ({})",
        last_pos,
        next_start_position,
    );
    for window in entries.windows(2) {
        let a = window[0]
            .header
            .as_ref()
            .expect("entry should have header")
            .log_position;
        let b = window[1]
            .header
            .as_ref()
            .expect("entry should have header")
            .log_position;
        assert!(
            a < b,
            "log positions not strictly increasing: {} >= {}",
            a,
            b
        );
    }
}

/// Maps a Payload to its corresponding SelectivePollType with exhaustive matching.
/// This ensures compile-time checking - if a new Payload variant is added, this match will fail to compile.
pub fn payload_to_selective_poll_type(payload: &Payload) -> i32 {
    if let Some(ref p) = payload.payload {
        match p {
            payload::Payload::Intention(_) => SelectivePollType::Intention as i32,
            payload::Payload::Vote(_) => SelectivePollType::Vote as i32,
            payload::Payload::DeciderPolicy(_) => SelectivePollType::DeciderPolicy as i32,
            payload::Payload::Commit(_) => SelectivePollType::Commit as i32,
            payload::Payload::Abort(_) => SelectivePollType::Abort as i32,
            payload::Payload::VoterPolicy(_) => SelectivePollType::VoterPolicy as i32,
            payload::Payload::Control(_) => SelectivePollType::Control as i32,
            payload::Payload::InferenceInput(_) => SelectivePollType::InferenceInput as i32,
            payload::Payload::InferenceOutput(_) => SelectivePollType::InferenceOutput as i32,
            payload::Payload::ActionOutput(_) => SelectivePollType::ActionOutput as i32,
            payload::Payload::AgentInput(_) => SelectivePollType::AgentInput as i32,
            payload::Payload::AgentOutput(_) => SelectivePollType::AgentOutput as i32,
            payload::Payload::Mail(_) => SelectivePollType::Mail as i32,
        }
    } else {
        panic!("Cannot map missing or unknown payload to SelectivePollType")
    }
}

/// Creates a payload from a variant name string (non-exhaustive match).
pub fn variant_name_to_payload(variant_name: &str) -> Payload {
    let payload_inner = match variant_name {
        "intention" => payload::Payload::Intention(Intention {
            intention: Some(intention::Intention::StringIntention("dummy".to_string())),
            ..Default::default()
        }),
        "vote" => payload::Payload::Vote(Vote::default()),
        "deciderPolicy" => payload::Payload::DeciderPolicy(DeciderPolicy::OffByDefault as i32),
        "commit" => payload::Payload::Commit(Commit::default()),
        "abort" => payload::Payload::Abort(Abort::default()),
        "voterPolicy" => payload::Payload::VoterPolicy(VoterPolicy::default()),
        "control" => payload::Payload::Control(Control {
            control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                control: Some(base_engine_control::Control::RemoveVoter(RemoveVoter {
                    voter_id: "dummy".to_string(),
                })),
            })),
        }),
        "inferenceInput" => payload::Payload::InferenceInput(InferenceInput {
            inference_input: Some(inference_input::InferenceInput::StringInferenceInput(
                "dummy".to_string(),
            )),
        }),
        "inferenceOutput" => payload::Payload::InferenceOutput(InferenceOutput {
            inference_output: Some(inference_output::InferenceOutput::StringInferenceOutput(
                "dummy".to_string(),
            )),
        }),
        "actionOutput" => payload::Payload::ActionOutput(ActionOutput {
            intention_id: 0,
            action_output: Some(action_output::ActionOutput::StringActionOutput(
                "dummy".to_string(),
            )),
        }),
        "agentInput" => payload::Payload::AgentInput(AgentInput {
            agent_input: Some(agent_input::AgentInput::StringAgentInput(
                "dummy".to_string(),
            )),
        }),
        "agentOutput" => payload::Payload::AgentOutput(AgentOutput {
            agent_output: Some(agent_output::AgentOutput::StringAgentOutput(
                "dummy".to_string(),
            )),
        }),
        "mail" => payload::Payload::Mail(Mail {
            sender_agent_id: "dummy-sender".to_string(),
            content: Some(mail::Content::Message(Message {
                body: "dummy".to_string(),
                message_id: "dummy-message".to_string(),
            })),
        }),
        _ => unreachable!(
            "Unknown Payload variant '{}' - please add a mapping",
            variant_name
        ),
    };
    Payload {
        payload: Some(payload_inner),
    }
}

/// Creates a sample payload for each SelectivePollType (non-exhaustive match).
/// Returns None for unknown SelectivePollType variants.
pub fn selective_poll_type_to_payload(poll_type: i32) -> Option<Payload> {
    let poll_type_enum = SelectivePollType::try_from(poll_type).ok()?;
    let payload_inner = match poll_type_enum {
        SelectivePollType::Intention => payload::Payload::Intention(Intention {
            intention: Some(intention::Intention::StringIntention("test".to_string())),
            ..Default::default()
        }),
        SelectivePollType::Vote => payload::Payload::Vote(Vote {
            intention_id: 0,
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(true)),
            }),
            ..Default::default()
        }),
        SelectivePollType::DeciderPolicy => {
            payload::Payload::DeciderPolicy(DeciderPolicy::OnByDefault as i32)
        }
        SelectivePollType::Commit => payload::Payload::Commit(Commit {
            intention_id: 0,
            reason: "test".to_string(),
            ..Default::default()
        }),
        SelectivePollType::Abort => payload::Payload::Abort(Abort {
            intention_id: 0,
            reason: "test".to_string(),
            ..Default::default()
        }),
        SelectivePollType::VoterPolicy => payload::Payload::VoterPolicy(VoterPolicy::default()),
        SelectivePollType::Control => payload::Payload::Control(Control {
            control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                control: Some(base_engine_control::Control::RemoveVoter(RemoveVoter {
                    voter_id: "test".to_string(),
                })),
            })),
        }),
        SelectivePollType::InferenceInput => payload::Payload::InferenceInput(InferenceInput {
            inference_input: Some(inference_input::InferenceInput::StringInferenceInput(
                "test".to_string(),
            )),
        }),
        SelectivePollType::InferenceOutput => payload::Payload::InferenceOutput(InferenceOutput {
            inference_output: Some(inference_output::InferenceOutput::StringInferenceOutput(
                "test".to_string(),
            )),
        }),
        SelectivePollType::ActionOutput => payload::Payload::ActionOutput(ActionOutput {
            intention_id: 0,
            action_output: Some(action_output::ActionOutput::StringActionOutput(
                "test".to_string(),
            )),
            ..Default::default()
        }),
        SelectivePollType::AgentInput => payload::Payload::AgentInput(AgentInput {
            agent_input: Some(agent_input::AgentInput::StringAgentInput(
                "test".to_string(),
            )),
        }),
        SelectivePollType::AgentOutput => payload::Payload::AgentOutput(AgentOutput {
            agent_output: Some(agent_output::AgentOutput::StringAgentOutput(
                "test".to_string(),
            )),
        }),
        SelectivePollType::Mail => payload::Payload::Mail(Mail {
            sender_agent_id: "sender".to_string(),
            content: Some(mail::Content::Message(Message {
                body: "test".to_string(),
                message_id: String::new(),
            })),
        }),
        SelectivePollType::Unspecified => {
            return None; // Unspecified doesn't have a meaningful payload
        }
    };
    Some(Payload {
        payload: Some(payload_inner),
    })
}
