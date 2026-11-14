/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Storage-free decider transitions. Storage-backed decider applicators persist
//! the state these functions mutate.

use agent_bus_proto_rust::agent_bus::Abort;
use agent_bus_proto_rust::agent_bus::Commit;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::Vote;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::vote_type;

/// `ON_BY_DEFAULT`: commit every intention.
pub fn on_by_default(position: i64, payload: &payload::Payload) -> Option<Payload> {
    matches!(payload, payload::Payload::Intention(_))
        .then(|| decision_payload(position, true, "ON_BY_DEFAULT policy".into()))
}

/// `OFF_BY_DEFAULT`: abort every intention.
pub fn off_by_default(position: i64, payload: &payload::Payload) -> Option<Payload> {
    matches!(payload, payload::Payload::Intention(_))
        .then(|| decision_payload(position, false, "OFF_BY_DEFAULT policy".into()))
}

/// `FIRST_BOOLEAN_WINS`: the latest intention becomes `pending`, superseding any
/// undecided one, and the first boolean vote for it decides it.
pub fn first_boolean_wins(
    pending: &mut Option<i64>,
    position: i64,
    payload: &payload::Payload,
) -> Option<Payload> {
    match payload {
        payload::Payload::Intention(_) => {
            *pending = Some(position);
            None
        }
        payload::Payload::Vote(vote) if *pending == Some(vote.intention_id) => {
            let approved = vote_approved(vote)?;
            *pending = None;
            Some(decision_payload(
                vote.intention_id,
                approved,
                vote.reason.clone(),
            ))
        }
        _ => None,
    }
}

fn vote_approved(vote: &Vote) -> Option<bool> {
    match vote.abstract_vote.as_ref()?.vote_type.as_ref()? {
        vote_type::VoteType::BooleanVote(approved) => Some(*approved),
        _ => None,
    }
}

fn decision_payload(intention_id: i64, approved: bool, reason: String) -> Payload {
    let reason = if reason.is_empty() {
        "decided by policy".to_string()
    } else {
        reason
    };
    let inner = if approved {
        payload::Payload::Commit(Commit {
            intention_id,
            reason,
        })
    } else {
        payload::Payload::Abort(Abort {
            intention_id,
            reason,
        })
    };
    Payload {
        payload: Some(inner),
    }
}
