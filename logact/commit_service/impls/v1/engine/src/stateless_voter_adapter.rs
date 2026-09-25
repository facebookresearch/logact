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
//! Voters may be non-deterministic (an LLM voter can return different verdicts
//! for the same intention), so the adapter persists the last `(position, vote)`
//! per bus via the `Storage` engine.
//!
//! NOTE: A re-delivery of the last intention replays the stored vote instead of
//! re-evaluating, satisfying the `Applicator` duplication-tolerance contract
//! across process restarts.

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
use anyhow::Context;
use bytes::Bytes;
use prost::Message;

use crate::Applicator;
use crate::ApplyError;
use crate::Storage;
use crate::applicator::StorageWriteResultExt;

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

    fn state_key(&self, bus_id: &str) -> String {
        format!("voter:{}:{bus_id}", self.voter_id)
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

/// Persisted per-bus state: the full `Payload` this voter produced for its last
/// applied intention. Persisting the whole payload (not just the boolean
/// verdict) lets a replay return the byte-identical result without re-invoking
/// the (possibly non-deterministic) voter. The applied position is not stored
/// here: it is the `Storage` slot's position, which `load` returns alongside
/// the decoded state.
#[derive(Clone, PartialEq, prost::Message)]
struct VoterApplicatorState {
    #[prost(message, optional, tag = "1")]
    resultant_payload: Option<Payload>,
}

pub struct StatelessVoterAdapter<S> {
    voter: ImmutableVoter,
    storage: Rc<S>,
}

impl<S: Storage> StatelessVoterAdapter<S> {
    pub fn new(voter: ImmutableVoter, storage: Rc<S>) -> Self {
        Self { voter, storage }
    }

    fn state_key(&self, bus_id: &str) -> String {
        self.voter.state_key(bus_id)
    }

    async fn load(
        &self,
        bus_id: &str,
    ) -> std::result::Result<(VoterApplicatorState, Option<i64>), ApplyError> {
        let stored = self
            .storage
            .get(&self.state_key(bus_id))
            .await
            .map_err(ApplyError::Storage)?;
        Ok(match stored {
            Some((b, pos)) => (
                VoterApplicatorState::decode(b.as_ref()).context("decoding voter state")?,
                Some(pos),
            ),
            None => (VoterApplicatorState::default(), None),
        })
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage> Applicator for StatelessVoterAdapter<S> {
    async fn apply(&self, bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
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

        let (state, stored_position) = self.load(bus_id).await?;
        if let Some(last) = stored_position {
            if position < last {
                return Err(ApplyError::StalePosition {
                    requested: position,
                    last,
                });
            }
            if position == last {
                // Re-delivery of the last intention: replay the persisted payload
                // verbatim (info/config included) without re-invoking the voter.
                return Ok(state.resultant_payload);
            }
        }

        let payload = self.voter.vote(bus_id, position, text).await;
        let new_state = VoterApplicatorState {
            resultant_payload: Some(payload.clone()),
        };
        self.storage
            .put(
                &self.state_key(bus_id),
                Bytes::from(new_state.encode_to_vec()),
                stored_position,
                position,
            )
            .await
            .into_voter_apply_result(bus_id, position)?;
        Ok(Some(payload))
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
    use crate::ConcurrencyError;
    use crate::InMemoryStorage;
    use crate::StorageError;
    use crate::storage::FaultyStorage;

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

    /// A voter whose reason changes on every call, so a replay that re-invoked
    /// the voter would observe different output.
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

    fn test_adapter(
        voter: impl Voter + 'static,
        voter_id: &str,
    ) -> StatelessVoterAdapter<InMemoryStorage> {
        StatelessVoterAdapter::new(
            ImmutableVoter::new(
                voter_id.to_string(),
                VoterConfig { config: None },
                Rc::new(voter),
            ),
            Rc::new(InMemoryStorage::new()),
        )
    }

    fn fixed_adapter<S: Storage + 'static>(storage: S) -> StatelessVoterAdapter<S> {
        StatelessVoterAdapter::new(
            ImmutableVoter::new(
                "42".to_string(),
                VoterConfig { config: None },
                Rc::new(FixedVoter(true)),
            ),
            Rc::new(storage),
        )
    }

    #[test]
    fn intention_produces_vote_carrying_voter_verdict() {
        let adapter = test_adapter(FixedVoter(true), "42");
        let out = block_on(adapter.apply("bus-1", &intention_entry(7, "do X"))).unwrap();
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
        let out = block_on(adapter.apply("bus-1", &intention_entry(1, "do Y"))).unwrap();
        assert_eq!(vote_verdict(out), Some(false));
    }

    #[test]
    fn voter_receives_bus_id() {
        let adapter = test_adapter(BusIdVoter, "42");
        let allowed = block_on(adapter.apply("allowed-bus", &intention_entry(1, "do Y"))).unwrap();
        let denied = block_on(adapter.apply("other-bus", &intention_entry(1, "do Y"))).unwrap();
        assert_eq!(vote_verdict(allowed), Some(true));
        assert_eq!(vote_verdict(denied), Some(false));
    }

    #[test]
    fn non_intention_entry_is_ignored() {
        let adapter = test_adapter(FixedVoter(true), "42");
        // A Vote entry is not an Intention — the adapter must not respond to it.
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
        let out = block_on(adapter.apply("bus-1", &entry)).unwrap();
        assert!(
            out.is_none(),
            "non-Intention entries should produce no payload"
        );
    }

    #[test]
    fn non_string_intention_is_ignored() {
        let adapter = test_adapter(FixedVoter(true), "42");
        // An Intention with no string body is not something the voter evaluates;
        // it must produce no vote and must not advance/persist any state.
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
            block_on(adapter.apply("bus-1", &entry)).unwrap().is_none(),
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
                block_on(adapter.apply("bus-1", &entry)).unwrap_err(),
                ApplyError::MissingHeader
            ),
            "an entry without a header must be rejected"
        );
    }

    #[test]
    fn replay_returns_persisted_payload() {
        let adapter = test_adapter(
            MetadataVoter {
                calls: Cell::new(0),
            },
            "7",
        );
        let first = block_on(adapter.apply("bus-1", &intention_entry(4, "x"))).unwrap();
        let replay = block_on(adapter.apply("bus-1", &intention_entry(4, "x"))).unwrap();

        // The full payload is replayed verbatim. If the replay had re-invoked
        // the voter, the reason would carry the second call's value and this
        // equality would fail.
        assert_eq!(
            first, replay,
            "replay must return the persisted payload, not a recomputed one"
        );

        let Some(payload::Payload::Vote(vote)) = first.and_then(|p| p.payload) else {
            panic!("expected a Vote payload");
        };
        assert_eq!(
            vote.reason, "reason 1",
            "reason captured at first evaluation"
        );
    }

    #[test]
    fn storage_errors_remain_typed() {
        let get_error = block_on(
            fixed_adapter(FaultyStorage::GetTimeout).apply("bus-1", &intention_entry(7, "x")),
        )
        .expect_err("storage get should fail");
        assert!(
            matches!(get_error, ApplyError::Storage(StorageError::Timeout(_))),
            "get error category should be preserved, got {get_error:?}"
        );

        let put_error = block_on(
            fixed_adapter(FaultyStorage::PutUnavailable).apply("bus-1", &intention_entry(7, "x")),
        )
        .expect_err("storage put should fail");
        assert!(
            matches!(
                put_error,
                ApplyError::Storage(StorageError::BackendUnavailable(_))
            ),
            "put error category should be preserved, got {put_error:?}"
        );
    }

    #[test]
    fn storage_concurrency_is_classified_as_voter() {
        let cas_error = block_on(
            fixed_adapter(FaultyStorage::RejectPut).apply("bus-1", &intention_entry(7, "x")),
        )
        .expect_err("storage CAS should be rejected");
        assert_eq!(
            cas_error.to_string(),
            "voter storage conflict for bus 'bus-1' at position 7"
        );
        assert!(
            matches!(
                cas_error,
                ApplyError::Concurrency(ConcurrencyError::Voter {
                    ref bus_id,
                    position: 7,
                    source: None,
                }) if bus_id == "bus-1"
            ),
            "CAS rejection should retain its context, got {cas_error:?}"
        );

        let conflict_error = block_on(
            fixed_adapter(FaultyStorage::PutConflict).apply("bus-1", &intention_entry(7, "x")),
        )
        .expect_err("storage transaction should conflict");
        assert!(
            matches!(
                conflict_error,
                ApplyError::Concurrency(ConcurrencyError::Voter {
                    ref bus_id,
                    position: 7,
                    source: Some(StorageError::TransactionConflict(_)),
                }) if bus_id == "bus-1"
            ),
            "transaction conflict should retain its context, got {conflict_error:?}"
        );
    }
}
