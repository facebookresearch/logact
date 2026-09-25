/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::Abort;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Commit;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::Vote;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::vote_type;
use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use prost::Message;
use prost_types::Any;

use crate::Applicator;
use crate::ApplyError;
use crate::DeciderState;
use crate::Storage;
use crate::StorageResult;
use crate::applicator::StorageWriteResultExt;

pub struct OnByDefaultApplicator;

pub struct OffByDefaultApplicator;

pub struct FirstBooleanWinsApplicator<S> {
    storage: Rc<S>,
    id: Option<i64>,
}

const FIRST_BOOLEAN_WINS_STATE_TYPE_URL: &str = "logact.decider.FirstBooleanWinsState";

#[derive(Clone, PartialEq, prost::Message)]
struct FirstBooleanWinsState {
    #[prost(int64, optional, tag = "1")]
    pending: Option<i64>,
}

impl OnByDefaultApplicator {
    pub fn new() -> Self {
        Self
    }
}

impl OffByDefaultApplicator {
    pub fn new() -> Self {
        Self
    }
}

impl<S> FirstBooleanWinsApplicator<S> {
    pub fn new(storage: Rc<S>, id: Option<i64>) -> Self {
        Self { storage, id }
    }

    fn decode_state(state: Option<&Any>) -> Result<FirstBooleanWinsState> {
        Ok(match state {
            Some(value) => FirstBooleanWinsState::decode(value.value.as_slice())?,
            None => FirstBooleanWinsState::default(),
        })
    }

    fn encode_state(state: &FirstBooleanWinsState) -> Any {
        Any {
            type_url: FIRST_BOOLEAN_WINS_STATE_TYPE_URL.to_string(),
            value: state.encode_to_vec(),
        }
    }
}

/// `None` gives an unconfigured bus a state identity distinct from legacy state.
fn state_key(bus_id: &str, id: Option<i64>) -> String {
    match id {
        Some(id) => format!("decider:state:{bus_id}:{id}"),
        None => format!("decider:state:{bus_id}:unconfigured"),
    }
}

async fn load_policy_state(
    storage: &impl Storage,
    bus_id: &str,
    id: Option<i64>,
) -> std::result::Result<(DeciderState, Option<i64>), ApplyError> {
    let stored = storage
        .get(&state_key(bus_id, id))
        .await
        .map_err(ApplyError::Storage)?;
    Ok(match stored {
        Some((bytes, position)) => (
            DeciderState::decode(bytes.as_ref()).context("decoding decider state")?,
            Some(position),
        ),
        None => (DeciderState::default(), None),
    })
}

async fn save_policy_state(
    storage: &impl Storage,
    bus_id: &str,
    id: Option<i64>,
    state: &DeciderState,
    expected: Option<i64>,
    position: i64,
) -> StorageResult<bool> {
    storage
        .put(
            &state_key(bus_id, id),
            Bytes::from(state.encode_to_vec()),
            expected,
            position,
        )
        .await
}

#[async_trait::async_trait(?Send)]
impl Applicator for OnByDefaultApplicator {
    async fn apply(&self, _bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
        let position = entry
            .header
            .as_ref()
            .map(|header| header.log_position)
            .ok_or(ApplyError::MissingHeader)?;
        // ON_BY_DEFAULT has no persistent state, so it bypasses persistence.
        Ok(
            match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
                Some(payload::Payload::Intention(_)) => Some(decision_payload(
                    position,
                    true,
                    "ON_BY_DEFAULT policy".into(),
                )),
                _ => None,
            },
        )
    }
}

#[async_trait::async_trait(?Send)]
impl Applicator for OffByDefaultApplicator {
    async fn apply(&self, _bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
        let position = entry
            .header
            .as_ref()
            .map(|header| header.log_position)
            .ok_or(ApplyError::MissingHeader)?;
        // OFF_BY_DEFAULT has no persistent state, so it bypasses persistence.
        Ok(
            match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
                Some(payload::Payload::Intention(_)) => Some(decision_payload(
                    position,
                    false,
                    "OFF_BY_DEFAULT policy".into(),
                )),
                _ => None,
            },
        )
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage> Applicator for FirstBooleanWinsApplicator<S> {
    async fn apply(&self, bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
        let position = entry
            .header
            .as_ref()
            .map(|header| header.log_position)
            .ok_or(ApplyError::MissingHeader)?;
        let inner = match entry
            .payload
            .as_ref()
            .and_then(|payload| payload.payload.as_ref())
        {
            Some(inner) => inner,
            None => return Ok(None),
        };

        let (mut state, stored_position) =
            load_policy_state(&*self.storage, bus_id, self.id).await?;
        if let Some(last) = stored_position {
            if position < last {
                return Err(ApplyError::StalePosition {
                    requested: position,
                    last,
                });
            }
            if position == last {
                return Ok(state.last_payload.clone());
            }
        }

        let mut policy_state = Self::decode_state(state.policy_state.as_ref())?;
        let decision = match inner {
            payload::Payload::Intention(_) => {
                policy_state.pending = Some(position);
                None
            }
            payload::Payload::Vote(vote) => {
                if policy_state.pending != Some(vote.intention_id) {
                    None
                } else if let Some(approved) = vote_approved(vote) {
                    policy_state.pending = None;
                    Some(decision_payload(
                        vote.intention_id,
                        approved,
                        vote.reason.clone(),
                    ))
                } else {
                    None
                }
            }
            _ => {
                tracing::warn!(
                    bus_id,
                    position,
                    "unexpected payload for FIRST_BOOLEAN_WINS applicator"
                );
                None
            }
        };
        state.policy_state = Some(Self::encode_state(&policy_state));
        state.last_payload = decision.clone();

        save_policy_state(
            &*self.storage,
            bus_id,
            self.id,
            &state,
            stored_position,
            position,
        )
        .await
        .into_decider_apply_result(bus_id, position)?;

        Ok(decision)
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

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::BusEntry;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Vote;
    use agent_bus_proto_rust::agent_bus::VoteType;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agent_bus_proto_rust::agent_bus::vote_type;
    use futures::executor::block_on;

    use super::*;
    use crate::ConcurrencyError;
    use crate::DeciderFactory;
    use crate::DeciderFactoryImpl;
    use crate::InMemoryStorage;
    use crate::StateMachineSpec;
    use crate::StorageError;
    use crate::storage::FaultyStorage;

    fn entry(position: i64, p: payload::Payload) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload { payload: Some(p) }),
        }
    }

    fn intention_payload(text: &str) -> payload::Payload {
        payload::Payload::Intention(Intention {
            intention: Some(intention::Intention::StringIntention(text.to_string())),
            ..Default::default()
        })
    }

    fn decider(policy: DeciderPolicy) -> (Rc<InMemoryStorage>, Rc<dyn Applicator>) {
        let storage = Rc::new(InMemoryStorage::new());
        let applicator = DeciderFactoryImpl::new(storage.clone())
            .create_decider(StateMachineSpec::new(Some(0), policy));
        (storage, applicator)
    }

    fn vote_payload(intention_id: i64, approved: bool) -> payload::Payload {
        payload::Payload::Vote(Vote {
            intention_id,
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(approved)),
            }),
            ..Default::default()
        })
    }

    fn vote_payload_with_reason(
        intention_id: i64,
        approved: bool,
        reason: &str,
    ) -> payload::Payload {
        payload::Payload::Vote(Vote {
            intention_id,
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(approved)),
            }),
            reason: reason.to_string(),
            ..Default::default()
        })
    }

    fn decision_reason(out: &Option<Payload>) -> Option<String> {
        match out.as_ref().and_then(|p| p.payload.as_ref()) {
            Some(payload::Payload::Commit(c)) => Some(c.reason.clone()),
            Some(payload::Payload::Abort(a)) => Some(a.reason.clone()),
            _ => None,
        }
    }

    fn is_commit(out: &Option<Payload>) -> bool {
        matches!(
            out.as_ref().and_then(|p| p.payload.as_ref()),
            Some(payload::Payload::Commit(_))
        )
    }

    fn is_abort(out: &Option<Payload>) -> bool {
        matches!(
            out.as_ref().and_then(|p| p.payload.as_ref()),
            Some(payload::Payload::Abort(_))
        )
    }

    #[test]
    fn on_by_default_auto_commits_intention() {
        let (_, d) = decider(DeciderPolicy::OnByDefault);
        let out = block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        assert!(
            is_commit(&out),
            "ON_BY_DEFAULT should auto-commit an intention"
        );
        assert_eq!(
            decision_reason(&out).as_deref(),
            Some("ON_BY_DEFAULT policy")
        );
    }

    #[test]
    fn first_boolean_wins_waits_for_a_vote_then_commits_on_true() {
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        let on_intention = block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        assert!(
            on_intention.is_none(),
            "FIRST_BOOLEAN_WINS must not decide on the intention alone"
        );
        let on_vote = block_on(d.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        assert!(is_commit(&on_vote), "first true vote should commit");
    }

    #[test]
    fn reconstructed_decider_loads_policy_state() {
        let storage = Rc::new(InMemoryStorage::new());
        let first = DeciderFactoryImpl::new(storage.clone()).create_decider(StateMachineSpec::new(
            Some(0),
            DeciderPolicy::FirstBooleanWins,
        ));
        block_on(first.apply("bus", &entry(0, intention_payload("x")))).unwrap();

        let second = DeciderFactoryImpl::new(storage).create_decider(StateMachineSpec::new(
            Some(0),
            DeciderPolicy::FirstBooleanWins,
        ));
        let out = block_on(second.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();

        assert!(is_commit(&out));
    }

    #[test]
    fn first_boolean_wins_aborts_on_false_vote() {
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        let on_vote = block_on(d.apply("bus", &entry(1, vote_payload(0, false)))).unwrap();
        assert!(is_abort(&on_vote), "first false vote should abort");
    }

    #[test]
    fn different_ids_have_independent_state() {
        let storage = Rc::new(InMemoryStorage::new());
        let first = FirstBooleanWinsApplicator::new(storage.clone(), Some(10));
        let second = FirstBooleanWinsApplicator::new(storage, Some(20));

        block_on(first.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        let second_id_vote =
            block_on(second.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        assert!(
            second_id_vote.is_none(),
            "a new id must not see an earlier id's pending intention"
        );

        let first_id_vote = block_on(first.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        assert!(
            is_commit(&first_id_vote),
            "the original id should retain its pending intention"
        );
    }

    #[test]
    fn off_by_default_aborts_intention() {
        // OFF_BY_DEFAULT decides at the intention: abort outright. The decision
        // is sticky and needs no per-intention state, so later votes are ignored.
        let (_, d) = decider(DeciderPolicy::OffByDefault);
        let out = block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        assert!(
            is_abort(&out),
            "OFF_BY_DEFAULT aborts the intention outright"
        );
        assert_eq!(
            decision_reason(&out).as_deref(),
            Some("OFF_BY_DEFAULT policy")
        );
        let after_vote = block_on(d.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        assert!(
            after_vote.is_none(),
            "votes do not affect an already-decided OFF_BY_DEFAULT intention"
        );
    }

    #[test]
    fn missing_header_is_rejected() {
        let (_, d) = decider(DeciderPolicy::OnByDefault);
        let entry = BusEntry {
            header: None,
            payload: Some(Payload {
                payload: Some(intention_payload("x")),
            }),
        };
        assert!(
            matches!(
                block_on(d.apply("bus", &entry)).unwrap_err(),
                ApplyError::MissingHeader
            ),
            "an entry without a header must be rejected"
        );
    }

    #[test]
    fn vote_for_decided_intention_is_ignored() {
        // FIRST_BOOLEAN_WINS: the first vote decides and GCs the intention, so a
        // later vote for the same id finds nothing in flight and is ignored.
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        let first = block_on(d.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        assert!(is_commit(&first), "first vote should commit");
        let second = block_on(d.apply("bus", &entry(2, vote_payload(0, false)))).unwrap();
        assert!(
            second.is_none(),
            "a vote for an already-decided (GC'd) intention is ignored"
        );
    }

    #[test]
    fn vote_for_unknown_intention_is_ignored() {
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        // No intention was ever registered for id 7.
        let out = block_on(d.apply("bus", &entry(0, vote_payload(7, true)))).unwrap();
        assert!(
            out.is_none(),
            "a vote for a never-registered intention is ignored"
        );
    }

    #[test]
    fn replay_of_deciding_vote_returns_same_payload() {
        // Re-delivering the deciding vote must reproduce the commit verbatim from
        // the persisted payload — not re-run the decision, which would now find
        // the intention GC'd and return None.
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        let first = block_on(d.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        let replay = block_on(d.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        assert_eq!(
            first, replay,
            "replaying the last entry must return the identical payload"
        );
        assert!(is_commit(&replay));
    }

    #[test]
    fn newer_intention_supersedes_an_undecided_one() {
        // Weak contract + single-slot optimization: only the most recent
        // undecided FIRST_BOOLEAN_WINS intention is tracked. A second intention
        // abandons the first (simply never decided, which the contract permits);
        // the second still decides normally on its first vote.
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        block_on(d.apply("bus", &entry(0, intention_payload("first")))).unwrap();
        block_on(d.apply("bus", &entry(1, intention_payload("second")))).unwrap();
        let first_vote = block_on(d.apply("bus", &entry(2, vote_payload(0, true)))).unwrap();
        assert!(
            first_vote.is_none(),
            "a superseded intention is abandoned, not decided"
        );
        let second_vote = block_on(d.apply("bus", &entry(3, vote_payload(1, true)))).unwrap();
        assert!(
            is_commit(&second_vote),
            "the most recent intention still decides on its first vote"
        );
    }

    #[test]
    fn deciding_vote_reason_is_carried_into_decision() {
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        let committed = block_on(d.apply(
            "bus",
            &entry(1, vote_payload_with_reason(0, true, "looks safe")),
        ))
        .unwrap();
        assert!(is_commit(&committed));
        assert_eq!(
            decision_reason(&committed).as_deref(),
            Some("looks safe"),
            "the deciding vote's reason should be carried into the commit"
        );

        // A false vote's reason is carried into the abort.
        block_on(d.apply("bus", &entry(2, intention_payload("y")))).unwrap();
        let aborted = block_on(d.apply(
            "bus",
            &entry(3, vote_payload_with_reason(2, false, "dangerous")),
        ))
        .unwrap();
        assert!(is_abort(&aborted));
        assert_eq!(decision_reason(&aborted).as_deref(), Some("dangerous"));
    }

    #[test]
    fn empty_vote_reason_falls_back_to_generic() {
        let (_, d) = decider(DeciderPolicy::FirstBooleanWins);
        block_on(d.apply("bus", &entry(0, intention_payload("x")))).unwrap();
        // `vote_payload` carries no reason — the decision falls back to a generic one.
        let out = block_on(d.apply("bus", &entry(1, vote_payload(0, true)))).unwrap();
        assert_eq!(
            decision_reason(&out).as_deref(),
            Some("decided by policy"),
            "an empty voter reason should fall back to the generic string"
        );
    }

    #[test]
    fn storage_errors_remain_typed() {
        let get_error = block_on(
            FirstBooleanWinsApplicator::new(Rc::new(FaultyStorage::GetTimeout), Some(0))
                .apply("bus", &entry(7, intention_payload("x"))),
        )
        .expect_err("storage get should fail");
        assert!(
            matches!(get_error, ApplyError::Storage(StorageError::Timeout(_))),
            "get error category should be preserved, got {get_error:?}"
        );

        let put_error = block_on(
            FirstBooleanWinsApplicator::new(Rc::new(FaultyStorage::PutUnavailable), Some(0))
                .apply("bus", &entry(7, intention_payload("x"))),
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
    fn storage_concurrency_is_classified_as_decider() {
        let cas_error = block_on(
            FirstBooleanWinsApplicator::new(Rc::new(FaultyStorage::RejectPut), Some(0))
                .apply("bus", &entry(7, intention_payload("x"))),
        )
        .expect_err("storage CAS should be rejected");
        assert_eq!(
            cas_error.to_string(),
            "decider storage conflict for bus 'bus' at position 7"
        );
        assert!(
            matches!(
                cas_error,
                ApplyError::Concurrency(ConcurrencyError::Decider {
                    ref bus_id,
                    position: 7,
                    source: None,
                }) if bus_id == "bus"
            ),
            "CAS rejection should retain its context, got {cas_error:?}"
        );

        let conflict_error = block_on(
            FirstBooleanWinsApplicator::new(Rc::new(FaultyStorage::PutConflict), Some(0))
                .apply("bus", &entry(7, intention_payload("x"))),
        )
        .expect_err("storage transaction should conflict");
        assert!(
            matches!(
                conflict_error,
                ApplyError::Concurrency(ConcurrencyError::Decider {
                    ref bus_id,
                    position: 7,
                    source: Some(StorageError::TransactionConflict(_)),
                }) if bus_id == "bus"
            ),
            "transaction conflict should retain its context, got {conflict_error:?}"
        );
    }
}
