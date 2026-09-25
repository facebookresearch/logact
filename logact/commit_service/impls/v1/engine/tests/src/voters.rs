/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Reusable test voters shared across engine and commit-service test crates.

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::Vote;
use agent_bus_proto_rust::agent_bus::VoteType;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::vote_type;
use agent_bus_proto_rust::agent_bus::voter_config;
use anyhow::Result;
use bytes::Bytes;
use logact_commit_service_engine::Applicator;
use logact_commit_service_engine::ApplyError;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::StateMachineSpec;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::StorageWriteResultExt;
use logact_commit_service_engine::VoterFactory;
use prost::Message;
use prost_types::Any;

/// Custom `VoterConfig` `type_url` selecting the `CountingVoter`.
pub const COUNTING_VOTER_TYPE_URL: &str = "test/counting_voter";

/// When a `CountingVoter` config carries no modulus, deny every third intention.
const DEFAULT_MODULUS: u64 = 3;

/// Build a `BooleanVote` payload for `intention_id` from `voter_id`.
pub fn boolean_vote(intention_id: i64, voter_id: &str, approved: bool) -> Payload {
    Payload {
        payload: Some(payload::Payload::Vote(Vote {
            intention_id,
            voter_id: voter_id.to_string(),
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(approved)),
            }),
            ..Default::default()
        })),
    }
}

#[derive(Clone, prost::Message)]
struct CountingVoterState {
    #[prost(uint64, tag = "1")]
    count: u64,
    #[prost(int64, tag = "2")]
    last_position: i64,
}

/// Config for `CountingVoter`: it rejects every `modulus`-th intention.
#[derive(Clone, prost::Message)]
pub struct CountingVoterConfig {
    #[prost(uint64, tag = "1")]
    pub modulus: u64,
}

/// Votes `false` on every `modulus`-th intention. The per-bus count is persisted
/// in `Storage` so it honors the engine's apply contract: a replay of the last
/// position recomputes the same vote without re-counting, and an older position
/// is rejected as stale.
pub struct CountingVoter<S = InMemoryStorage> {
    storage: Rc<S>,
    voter_id: String,
    modulus: u64,
}

impl<S: Storage> CountingVoter<S> {
    pub fn new(storage: Rc<S>, voter_id: String, modulus: u64) -> Self {
        Self {
            storage,
            voter_id,
            modulus,
        }
    }

    fn state_key(&self, bus_id: &str) -> String {
        format!("counting_voter:{}:{}", self.voter_id, bus_id)
    }

    async fn load_state(&self, bus_id: &str) -> (CountingVoterState, Option<i64>) {
        match self
            .storage
            .get(&self.state_key(bus_id))
            .await
            .ok()
            .flatten()
        {
            Some((b, pos)) => (
                CountingVoterState::decode(b.as_ref()).unwrap_or_default(),
                Some(pos),
            ),
            None => (CountingVoterState::default(), None),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage> Applicator for CountingVoter<S> {
    async fn apply(&self, bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
        let position = entry.header.as_ref().map(|h| h.log_position).unwrap_or(0);
        let inner = match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
            Some(p) => p,
            None => return Ok(None),
        };

        match inner {
            payload::Payload::Intention(_) => {
                let (mut state, stored_pos) = self.load_state(bus_id).await;
                if stored_pos.is_some() {
                    if position < state.last_position {
                        return Err(ApplyError::StalePosition {
                            requested: position,
                            last: state.last_position,
                        });
                    }
                    if position == state.last_position {
                        // Replay of the last entry: recompute from stored state
                        // without re-counting.
                        return Ok(Some(boolean_vote(
                            position,
                            &self.voter_id,
                            state.count % self.modulus != 0,
                        )));
                    }
                }
                state.count += 1;
                state.last_position = position;
                let approved = state.count % self.modulus != 0;
                self.storage
                    .put(
                        &self.state_key(bus_id),
                        Bytes::from(state.encode_to_vec()),
                        stored_pos,
                        position,
                    )
                    .await
                    .into_voter_apply_result(bus_id, position)?;
                Ok(Some(boolean_vote(position, &self.voter_id, approved)))
            }
            _ => Ok(None),
        }
    }
}

/// A `VoterFactory` that builds `CountingVoter`s from a custom `CountingVoterConfig`.
pub struct CountingVoterFactory<S = InMemoryStorage> {
    storage: Rc<S>,
}

impl<S> CountingVoterFactory<S> {
    pub fn new(storage: Rc<S>) -> Self {
        Self { storage }
    }
}

impl<S> Clone for CountingVoterFactory<S> {
    fn clone(&self) -> Self {
        Self::new(self.storage.clone())
    }
}

impl Default for CountingVoterFactory {
    fn default() -> Self {
        Self::new(Rc::new(InMemoryStorage::new()))
    }
}

impl<S: Storage + 'static> VoterFactory for CountingVoterFactory<S> {
    fn validate_config(&self, config: Option<&VoterConfig>) -> Result<VoterConfig> {
        let config = config.ok_or_else(|| anyhow::anyhow!("missing voter config"))?;
        let Some(voter_config::Config::Custom(any)) = config.config.as_ref() else {
            anyhow::bail!("counting voter factory only supports custom voter configs");
        };
        match any.type_url.as_str() {
            COUNTING_VOTER_TYPE_URL => Ok(config.clone()),
            other => anyhow::bail!("unknown test voter type: {other}"),
        }
    }

    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> Result<Rc<dyn Applicator>> {
        let Some(voter_config::Config::Custom(any)) = spec.config.config.as_ref() else {
            anyhow::bail!("counting voter factory only supports custom voter configs");
        };
        match any.type_url.as_str() {
            COUNTING_VOTER_TYPE_URL => {
                let cfg = CountingVoterConfig::decode(any.value.as_slice()).unwrap_or_default();
                let modulus = if cfg.modulus == 0 {
                    DEFAULT_MODULUS
                } else {
                    cfg.modulus
                };
                Ok(Rc::new(CountingVoter::new(
                    self.storage.clone(),
                    spec.id.clone(),
                    modulus,
                )))
            }
            other => anyhow::bail!("unknown test voter type: {other}"),
        }
    }
}

/// Build the `VoterConfig` selecting a `CountingVoter` with `modulus`.
pub fn counting_voter_config(modulus: u64) -> VoterConfig {
    VoterConfig {
        config: Some(voter_config::Config::Custom(Any {
            type_url: COUNTING_VOTER_TYPE_URL.to_string(),
            value: CountingVoterConfig { modulus }.encode_to_vec(),
        })),
    }
}
