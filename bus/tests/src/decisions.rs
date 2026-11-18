/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Decisions recorded in a bus log.

use std::collections::HashMap;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::payload;

/// A decision for an intention, at the log position that records it.
#[derive(Clone, Debug, PartialEq)]
pub struct LoggedDecision {
    pub intention_id: i64,
    pub position: i64,
    pub approved: bool,
    pub reason: String,
}

/// Reads decisions from a bus log in order, starting at position 0.
#[derive(Default)]
pub struct DecisionReader {}

impl DecisionReader {
    /// The decision `entry` records, if any. Entries must be observed in log
    /// order, starting at position 0.
    pub fn observe(&mut self, entry: &BusEntry) -> Option<LoggedDecision> {
        let position = entry.header.as_ref()?.log_position;
        match entry.payload.as_ref()?.payload.as_ref()? {
            payload::Payload::Commit(commit) => Some(LoggedDecision {
                intention_id: commit.intention_id,
                position,
                approved: true,
                reason: commit.reason.clone(),
            }),
            payload::Payload::Abort(abort) => Some(LoggedDecision {
                intention_id: abort.intention_id,
                position,
                approved: false,
                reason: abort.reason.clone(),
            }),
            _ => None,
        }
    }
}

/// The first decision for each intention, by intention position. `entries`
/// must start at position 0.
pub fn first_decisions(entries: &[BusEntry]) -> HashMap<i64, LoggedDecision> {
    let mut reader = DecisionReader::default();
    let mut decisions = HashMap::new();
    for decision in entries.iter().filter_map(|entry| reader.observe(entry)) {
        decisions.entry(decision.intention_id).or_insert(decision);
    }
    decisions
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::Abort;
    use agent_bus_proto_rust::agent_bus::Commit;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;

    use super::*;

    fn entry(position: i64, payload: payload::Payload) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload),
            }),
        }
    }

    fn commit(intention_id: i64) -> payload::Payload {
        payload::Payload::Commit(Commit {
            intention_id,
            reason: "approved".to_string(),
        })
    }

    fn abort(intention_id: i64) -> payload::Payload {
        payload::Payload::Abort(Abort {
            intention_id,
            reason: "denied".to_string(),
        })
    }

    #[test]
    fn logged_decisions_are_read_at_their_positions() {
        let decisions = first_decisions(&[
            entry(0, payload::Payload::Intention(Intention::default())),
            entry(1, payload::Payload::Intention(Intention::default())),
            entry(2, payload::Payload::Intention(Intention::default())),
            entry(3, commit(0)),
            entry(4, abort(1)),
        ]);
        assert_eq!(
            decisions[&0],
            LoggedDecision {
                intention_id: 0,
                position: 3,
                approved: true,
                reason: "approved".to_string(),
            }
        );
        assert!(!decisions[&1].approved);
        assert_eq!(decisions[&1].position, 4);
        assert!(!decisions.contains_key(&2));
    }

    #[test]
    fn the_first_decision_wins() {
        let decisions = first_decisions(&[
            entry(0, payload::Payload::Intention(Intention::default())),
            entry(1, abort(0)),
            entry(2, commit(0)),
        ]);
        assert!(!decisions[&0].approved);
        assert_eq!(decisions[&0].position, 1);
    }
}
