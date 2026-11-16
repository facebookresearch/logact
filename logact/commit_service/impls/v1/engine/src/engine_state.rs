/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::Bytes;
pub use logact_commit_service_policy_proto_rust::logact_commit_service_engine_state::PerBusEngineState;
pub use logact_commit_service_policy_proto_rust::logact_commit_service_engine_state::VersionedDecider;
use prost::Message;

use crate::ApplyError;
use crate::Storage;
use crate::StorageError;
use crate::StorageResult;

/// Storage namespace for the v1 engine and its applicators.
pub const ENGINE_STORAGE_PREFIX: &str = "engine/v1/";

/// Return the logical storage key for one bus's engine state.
///
/// Exposed for storage-level administrative tooling that must preserve the
/// serialized state bytes while conditionally advancing the stored position.
pub fn engine_state_key(bus_id: &str) -> String {
    format!("engine:state:{bus_id}")
}

/// Failure while loading persisted engine state.
#[derive(Debug, thiserror::Error)]
pub enum EngineStateLoadError {
    /// Reading the state from storage failed.
    #[error(transparent)]
    Storage(StorageError),

    /// The stored bytes could not be decoded as engine state.
    #[error("decoding engine state: {0}")]
    Decode(#[source] prost::DecodeError),
}

impl From<EngineStateLoadError> for ApplyError {
    fn from(error: EngineStateLoadError) -> Self {
        match error {
            EngineStateLoadError::Storage(error) => Self::Storage(error),
            EngineStateLoadError::Decode(error) => Self::InvalidEngineState {
                message: format!("decoding engine state: {error}"),
            },
        }
    }
}

/// Load a bus's engine state from storage. Returns `(state, stored_position)`,
/// where `stored_position` is `None` if the key doesn't exist (for CAS).
pub async fn load_engine_state(
    storage: &impl Storage,
    bus_id: &str,
) -> std::result::Result<(PerBusEngineState, Option<i64>), EngineStateLoadError> {
    let stored = storage
        .get(&engine_state_key(bus_id))
        .await
        .map_err(EngineStateLoadError::Storage)?;
    Ok(match stored {
        Some((b, pos)) => (
            PerBusEngineState::decode(b.as_ref()).map_err(EngineStateLoadError::Decode)?,
            Some(pos),
        ),
        None => (PerBusEngineState::default(), None),
    })
}

/// Save a bus's engine state to storage by compare-and-swap.
pub async fn save_engine_state(
    state: &PerBusEngineState,
    storage: &impl Storage,
    bus_id: &str,
    expected: Option<i64>,
    new_position: i64,
) -> StorageResult<bool> {
    storage
        .put(
            &engine_state_key(bus_id),
            Bytes::from(state.encode_to_vec()),
            expected,
            new_position,
        )
        .await
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::storage::FaultyStorage;

    #[test]
    fn load_preserves_storage_error_category() {
        let error = block_on(load_engine_state(&FaultyStorage::GetTimeout, "bus-1"))
            .expect_err("storage get should fail");

        assert!(
            matches!(
                error,
                EngineStateLoadError::Storage(StorageError::Timeout(_))
            ),
            "storage category should survive the engine-state boundary, got {error:?}"
        );
    }

    /// The hand-written prost messages these generated types replaced.
    mod legacy {
        use std::collections::HashMap;

        use agent_bus_proto_rust::agent_bus::Payload;
        use agent_bus_proto_rust::agent_bus::VoterConfig;
        use prost_types::Any;

        #[derive(Clone, PartialEq, prost::Message)]
        pub struct PerBusEngineState {
            #[prost(message, repeated, tag = "1")]
            pub voter_configs: Vec<Any>,
            #[prost(map = "string, message", tag = "2")]
            pub voters: HashMap<String, VoterConfig>,
            #[prost(message, optional, tag = "3")]
            pub decider: Option<VersionedDecider>,
            #[prost(int64, optional, tag = "4")]
            pub applied_policy_version: Option<i64>,
        }

        #[derive(Clone, PartialEq, prost::Message)]
        pub struct VersionedDecider {
            #[prost(int32, tag = "1")]
            pub policy: i32,
            #[prost(int64, tag = "2")]
            pub id: i64,
        }

        #[derive(Clone, PartialEq, prost::Message)]
        pub struct DeciderState {
            #[prost(message, optional, tag = "2")]
            pub last_payload: Option<Payload>,
            #[prost(message, optional, tag = "3")]
            pub policy_state: Option<Any>,
        }

        #[derive(Clone, PartialEq, prost::Message)]
        pub struct FirstBooleanWinsState {
            #[prost(int64, optional, tag = "1")]
            pub pending: Option<i64>,
        }
    }

    /// Assert `current` and `legacy` encode identically and decode each other.
    fn assert_same_encoding<
        C: Message + Default + PartialEq + std::fmt::Debug,
        L: Message + Default,
    >(
        current: &C,
        legacy: &L,
    ) {
        assert_eq!(current.encode_to_vec(), legacy.encode_to_vec());
        assert_eq!(
            &C::decode(legacy.encode_to_vec().as_slice()).unwrap(),
            current
        );
        assert_eq!(
            L::decode(current.encode_to_vec().as_slice())
                .unwrap()
                .encode_to_vec(),
            legacy.encode_to_vec()
        );
    }

    #[test]
    fn generated_messages_keep_the_hand_written_encoding() {
        use agent_bus_proto_rust::agent_bus::Commit;
        use agent_bus_proto_rust::agent_bus::Payload;
        use agent_bus_proto_rust::agent_bus::VoterConfig;
        use agent_bus_proto_rust::agent_bus::payload;
        use logact_commit_service_policy_proto_rust::logact_commit_service_engine_state as generated;
        use prost_types::Any;

        let any = Any {
            type_url: "example/type".to_owned(),
            value: vec![1, 2, 3],
        };
        let voter = VoterConfig::default();
        let payload = Payload {
            payload: Some(payload::Payload::Commit(Commit {
                intention_id: 7,
                ..Default::default()
            })),
        };
        for (decider, version) in [(None, None), (Some((2, 9)), Some(4))] {
            assert_same_encoding(
                &PerBusEngineState {
                    voter_configs: vec![any.clone()],
                    voters: [("voter".to_owned(), voter.clone())].into(),
                    decider: decider.map(|(policy, id)| VersionedDecider { policy, id }),
                    applied_policy_version: version,
                    replay_decider_state: None,
                },
                &legacy::PerBusEngineState {
                    voter_configs: vec![any.clone()],
                    voters: [("voter".to_owned(), voter.clone())].into(),
                    decider: decider.map(|(policy, id)| legacy::VersionedDecider { policy, id }),
                    applied_policy_version: version,
                },
            );
        }
        assert_same_encoding(
            &generated::DeciderState {
                last_payload: Some(payload.clone()),
                policy_state: Some(any.clone()),
            },
            &legacy::DeciderState {
                last_payload: Some(payload),
                policy_state: Some(any),
            },
        );
        for pending in [None, Some(0), Some(5)] {
            assert_same_encoding(
                &generated::FirstBooleanWinsState { pending },
                &legacy::FirstBooleanWinsState { pending },
            );
        }
    }
}
