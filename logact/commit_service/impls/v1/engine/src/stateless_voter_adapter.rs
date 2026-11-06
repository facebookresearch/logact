/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Adapter for using existing agentbus `Voter` trait implementations within the
//! engine's `Applicator` framework. Bridges `Voter::evaluate()` to
//! `Applicator::apply()`.
//!
//! Intention evaluation proposes a vote without changing durable state. The
//! adapter has no durable state. Evaluation may be repeated, including across
//! process restarts; the log orders those proposals for the decider.

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::Vote;
use agent_bus_proto_rust::agent_bus::VoteType;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::vote_type;
use agentbus_api::voter::Voter;
use agentbus_api::voter::VoterContext;

use crate::Applicator;
use crate::ApplyError;

pub struct ImmutableVoter {
    voter_id: String,
    config: VoterConfig,
    voter: Rc<dyn Voter>,
}

impl ImmutableVoter {
    pub fn new(voter_id: String, config: VoterConfig, voter: Rc<dyn Voter>) -> Self {
        Self {
            voter_id,
            config,
            voter,
        }
    }

    pub async fn vote(&self, bus_id: &str, intention_id: i64, intention: &str) -> Payload {
        let (approved, reason) = self
            .voter
            .evaluate(VoterContext::new(bus_id, intention))
            .await;
        Payload {
            payload: Some(payload::Payload::Vote(Vote {
                abstract_vote: Some(VoteType {
                    vote_type: Some(vote_type::VoteType::BooleanVote(approved)),
                }),
                intention_id,
                voter_config: Some(self.config.clone()),
                voter_id: self.voter_id.clone(),
                reason,
            })),
        }
    }
}

pub struct StatelessVoterAdapter {
    voter: ImmutableVoter,
}

impl StatelessVoterAdapter {
    pub fn new(voter: ImmutableVoter) -> Self {
        Self { voter }
    }
}

#[async_trait::async_trait(?Send)]
impl Applicator for StatelessVoterAdapter {
    async fn apply(
        &self,
        bus_id: &str,
        entry: &BusEntry,
        _context: &mut (),
    ) -> Result<Option<Payload>, ApplyError> {
        let position = entry
            .header
            .as_ref()
            .map(|h| h.log_position)
            .ok_or(ApplyError::MissingHeader)?;
        let Some(payload::Payload::Intention(intention)) =
            entry.payload.as_ref().and_then(|p| p.payload.as_ref())
        else {
            return Ok(None);
        };
        let Some(intention::Intention::StringIntention(text)) = &intention.intention else {
            return Ok(None);
        };
        Ok(Some(self.voter.vote(bus_id, position, text).await))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use agent_bus_proto_rust::agent_bus::BusEntry;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Vote;
    use agent_bus_proto_rust::agent_bus::VoteType;
    use futures::executor::block_on;

    use super::*;

    /// Votes a fixed verdict regardless of the intention text.
    struct FixedVoter(bool);

    #[async_trait::async_trait(?Send)]
    impl Voter for FixedVoter {
        async fn evaluate(&self, _context: VoterContext<'_>) -> (bool, String) {
            (self.0, String::new())
        }

        fn apply_policy(&mut self, _config: &prost_types::Any) {}

        fn describe(&self) -> String {
            "FixedVoter".to_string()
        }
    }

    struct BusIdVoter;

    #[async_trait::async_trait(?Send)]
    impl Voter for BusIdVoter {
        async fn evaluate(&self, context: VoterContext<'_>) -> (bool, String) {
            (context.bus_id == "allowed-bus", context.bus_id.to_string())
        }

        fn apply_policy(&mut self, _config: &prost_types::Any) {}

        fn describe(&self) -> String {
            "BusIdVoter".to_string()
        }
    }

    /// Produces a different reason on each evaluation.
    struct MetadataVoter {
        calls: Cell<u8>,
    }

    #[async_trait::async_trait(?Send)]
    impl Voter for MetadataVoter {
        async fn evaluate(&self, _context: VoterContext<'_>) -> (bool, String) {
            let n = self.calls.get() + 1;
            self.calls.set(n);
            (true, format!("reason {n}"))
        }

        fn apply_policy(&mut self, _config: &prost_types::Any) {}

        fn describe(&self) -> String {
            "MetadataVoter".to_string()
        }
    }

    fn intention_entry(position: i64, text: &str) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention(text.to_string())),
                    ..Default::default()
                })),
            }),
        }
    }

    fn vote_entry(position: i64, intention_id: i64, voter_id: &str, approved: bool) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Vote(Vote {
                    intention_id,
                    voter_id: voter_id.to_string(),
                    voter_config: Some(VoterConfig { config: None }),
                    abstract_vote: Some(VoteType {
                        vote_type: Some(vote_type::VoteType::BooleanVote(approved)),
                    }),
                    ..Default::default()
                })),
            }),
        }
    }

    fn vote_verdict(out: Option<Payload>) -> Option<bool> {
        match out.and_then(|p| p.payload) {
            Some(payload::Payload::Vote(v)) => v
                .abstract_vote
                .and_then(|vt| vt.vote_type)
                .and_then(|vt| match vt {
                    vote_type::VoteType::BooleanVote(b) => Some(b),
                    _ => None,
                }),
            _ => None,
        }
    }

    fn test_adapter(voter: impl Voter + 'static, voter_id: &str) -> StatelessVoterAdapter {
        StatelessVoterAdapter::new(ImmutableVoter::new(
            voter_id.to_string(),
            VoterConfig { config: None },
            Rc::new(voter),
        ))
    }

    #[test]
    fn intention_produces_vote_carrying_voter_verdict() {
        let adapter = test_adapter(FixedVoter(true), "42");
        let out = block_on(adapter.apply("bus-1", &intention_entry(7, "do X"), &mut ())).unwrap();
        let vote = match out.and_then(|p| p.payload) {
            Some(payload::Payload::Vote(v)) => v,
            other => panic!("expected a Vote payload, got {other:?}"),
        };
        assert_eq!(
            vote.intention_id, 7,
            "vote should reference the intention's log position"
        );
        assert_eq!(
            vote.abstract_vote
                .and_then(|vt| vt.vote_type)
                .and_then(|vt| match vt {
                    vote_type::VoteType::BooleanVote(b) => Some(b),
                    _ => None,
                }),
            Some(true),
            "vote should carry the voter's verdict"
        );
        assert_eq!(
            vote.voter_id, "42",
            "vote should identify the voter that produced it"
        );
        assert!(
            vote.voter_config.is_some(),
            "vote should carry the voter's canonical config"
        );
    }

    #[test]
    fn rejecting_voter_produces_false_vote() {
        let adapter = test_adapter(FixedVoter(false), "42");
        let out = block_on(adapter.apply("bus-1", &intention_entry(1, "do Y"), &mut ())).unwrap();
        assert_eq!(vote_verdict(out), Some(false));
    }

    #[test]
    fn voter_receives_bus_id() {
        let adapter = test_adapter(BusIdVoter, "42");
        let allowed =
            block_on(adapter.apply("allowed-bus", &intention_entry(1, "do Y"), &mut ())).unwrap();
        let denied =
            block_on(adapter.apply("other-bus", &intention_entry(1, "do Y"), &mut ())).unwrap();
        assert_eq!(vote_verdict(allowed), Some(true));
        assert_eq!(vote_verdict(denied), Some(false));
    }

    #[test]
    fn unrelated_vote_is_ignored() {
        let adapter = test_adapter(FixedVoter(true), "42");
        let entry = BusEntry {
            header: Some(Header {
                log_position: 2,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Vote(Vote {
                    intention_id: 0,
                    abstract_vote: Some(VoteType {
                        vote_type: Some(vote_type::VoteType::BooleanVote(true)),
                    }),
                    ..Default::default()
                })),
            }),
        };
        let out = block_on(adapter.apply("bus-1", &entry, &mut ())).unwrap();
        assert!(
            out.is_none(),
            "non-Intention entries should produce no payload"
        );
    }

    #[test]
    fn non_string_intention_is_ignored() {
        let adapter = test_adapter(FixedVoter(true), "42");
        let entry = BusEntry {
            header: Some(Header {
                log_position: 3,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: None,
                    ..Default::default()
                })),
            }),
        };
        assert!(
            block_on(adapter.apply("bus-1", &entry, &mut ()))
                .unwrap()
                .is_none(),
            "a non-string intention should produce no payload"
        );
    }

    #[test]
    fn missing_header_is_rejected() {
        let adapter = test_adapter(FixedVoter(true), "42");
        let entry = BusEntry {
            header: None,
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention("x".to_string())),
                    ..Default::default()
                })),
            }),
        };
        assert!(
            matches!(
                block_on(adapter.apply("bus-1", &entry, &mut ())).unwrap_err(),
                ApplyError::MissingHeader
            ),
            "an entry without a header must be rejected"
        );
    }

    #[test]
    fn evaluation_may_be_repeated_after_restart() {
        let adapter = test_adapter(
            MetadataVoter {
                calls: Cell::new(0),
            },
            "7",
        );
        let entry = intention_entry(4, "x");
        let first = block_on(adapter.apply("bus-1", &entry, &mut ())).unwrap();
        let repeated = block_on(adapter.apply("bus-1", &entry, &mut ())).unwrap();
        assert_ne!(first, repeated);

        let restarted = test_adapter(FixedVoter(false), "7");
        let retry = block_on(restarted.apply("bus-1", &entry, &mut ())).unwrap();
        assert_eq!(vote_verdict(first), Some(true));
        assert_eq!(vote_verdict(retry), Some(false));
    }

    #[test]
    fn logged_votes_do_not_evaluate_or_prevent_intention_replay() {
        let voter = Rc::new(MetadataVoter {
            calls: Cell::new(0),
        });
        let adapter = StatelessVoterAdapter::new(ImmutableVoter::new(
            "7".to_string(),
            VoterConfig { config: None },
            voter.clone(),
        ));
        for entry in [
            vote_entry(8, 4, "7", true),
            vote_entry(8, 4, "7", true),
            vote_entry(9, 4, "other", false),
        ] {
            assert!(
                block_on(adapter.apply("bus-1", &entry, &mut ()))
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(voter.calls.get(), 0);
        let replay = block_on(adapter.apply("bus-1", &intention_entry(4, "x"), &mut ())).unwrap();
        assert_eq!(vote_verdict(replay), Some(true));
        assert_eq!(voter.calls.get(), 1);
    }
}
