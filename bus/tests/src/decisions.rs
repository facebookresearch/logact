/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Decisions recorded in a bus log, including the speculative decisions that
//! intentions carry.

use std::collections::HashMap;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::base_engine_control;
use agent_bus_proto_rust::agent_bus::control;
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
///
/// Besides explicit `Commit` and `Abort` entries, an intention's speculative
/// decision is a decision at the intention's own position when its policy
/// version is the applied policy version there. Checking that needs only the
/// versions in `PolicyBatch` entries, not the policy itself.
#[derive(Default)]
pub struct DecisionReader {
    applied_policy_version: Option<i64>,
}

impl DecisionReader {
    /// The decision `entry` records, if any. Entries must be observed in log
    /// order, starting at position 0.
    pub fn observe(&mut self, entry: &BusEntry) -> Option<LoggedDecision> {
        let position = entry.header.as_ref()?.log_position;
        match entry.payload.as_ref()?.payload.as_ref()? {
            payload::Payload::Control(control) => {
                if let Some(batch) = policy_batch(control)
                    && self.applied_policy_version == batch.expected_current_version
                    && batch
                        .expected_current_version
                        .is_none_or(|current| batch.new_version > current)
                {
                    self.applied_policy_version = Some(batch.new_version);
                }
                None
            }
            payload::Payload::Intention(intention) => {
                let speculation = intention.speculative_decision.as_ref()?;
                (self.applied_policy_version == Some(speculation.policy_version)).then(|| {
                    LoggedDecision {
                        intention_id: position,
                        position,
                        approved: speculation.approved,
                        reason: speculation.reason.clone(),
                    }
                })
            }
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

fn policy_batch(control: &agent_bus_proto_rust::agent_bus::Control) -> Option<&PolicyBatch> {
    let control::Control::BaseEngineControl(base) = control.control.as_ref()?;
    match base.control.as_ref()? {
        base_engine_control::Control::PolicyBatch(batch) => Some(batch),
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::BaseEngineControl;
    use agent_bus_proto_rust::agent_bus::Commit;
    use agent_bus_proto_rust::agent_bus::Control;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::SpeculativeDecision;

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

    fn batch(expected: Option<i64>, new: i64) -> payload::Payload {
        payload::Payload::Control(Control {
            control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                control: Some(base_engine_control::Control::PolicyBatch(PolicyBatch {
                    expected_current_version: expected,
                    new_version: new,
                    ..Default::default()
                })),
            })),
        })
    }

    fn speculative(version: i64, approved: bool) -> payload::Payload {
        payload::Payload::Intention(Intention {
            speculative_decision: Some(SpeculativeDecision {
                policy_version: version,
                approved,
                reason: "speculated".to_string(),
            }),
            ..Default::default()
        })
    }

    #[test]
    fn a_speculation_is_correct_only_at_its_policy_version() {
        let decisions = first_decisions(&[
            entry(0, batch(None, 1)),
            entry(1, speculative(1, true)),
            entry(2, batch(Some(1), 2)),
            entry(3, speculative(1, false)),
            entry(4, speculative(2, false)),
        ]);
        assert!(decisions[&1].approved);
        assert_eq!(decisions[&1].position, 1);
        assert!(
            !decisions.contains_key(&3),
            "a stale speculation is not a decision"
        );
        assert!(!decisions[&4].approved);
    }

    #[test]
    fn a_batch_that_does_not_apply_leaves_the_version() {
        let decisions = first_decisions(&[
            entry(0, batch(None, 1)),
            entry(1, batch(Some(7), 8)),
            entry(2, speculative(8, true)),
            entry(3, speculative(1, true)),
        ]);
        assert!(!decisions.contains_key(&2));
        assert!(decisions.contains_key(&3));
    }

    #[test]
    fn an_intention_without_a_speculation_waits_for_a_logged_decision() {
        let decisions = first_decisions(&[
            entry(0, batch(None, 1)),
            entry(1, payload::Payload::Intention(Intention::default())),
        ]);
        assert!(
            decisions.is_empty(),
            "a V1 intention is not decided by itself"
        );

        let decisions = first_decisions(&[
            entry(0, batch(None, 1)),
            entry(1, payload::Payload::Intention(Intention::default())),
            entry(
                2,
                payload::Payload::Commit(Commit {
                    intention_id: 1,
                    reason: String::new(),
                }),
            ),
        ]);
        assert_eq!(decisions[&1].position, 2);
    }

    #[test]
    fn the_first_decision_wins() {
        let decisions = first_decisions(&[
            entry(0, batch(None, 1)),
            entry(1, speculative(1, false)),
            entry(
                2,
                payload::Payload::Commit(Commit {
                    intention_id: 1,
                    reason: String::new(),
                }),
            ),
        ]);
        assert!(!decisions[&1].approved);
    }
}
