/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use agent_bus_proto_rust::agent_bus::VoterConfig;
use bytes::Bytes;
use prost::Message;
use prost_types::Any;

use crate::ApplyError;
use crate::Storage;
use crate::StorageError;
use crate::StorageResult;

/// Storage namespace for the v1 engine and its applicators.
pub const ENGINE_STORAGE_PREFIX: &str = "engine/v1/";

#[derive(Clone, prost::Message)]
pub struct PerBusEngineState {
    /// Legacy voter policies installed for this bus, in installation order.
    /// This field remains readable for existing stored state, but new engine
    /// handling ignores it.
    #[prost(message, repeated, tag = "1")]
    pub voter_configs: Vec<Any>,
    /// Active voter configs keyed by stable voter ID. Legacy state may use the
    /// original `AddVoter` entry position as the ID.
    #[prost(map = "string, message", tag = "2")]
    pub voters: HashMap<String, VoterConfig>,
    /// Configured policy and state-machine ID, or `None` if unconfigured.
    #[prost(message, optional, tag = "3")]
    pub decider: Option<VersionedPolicy>,
    /// Latest policy generation applied from a `PolicyBatch`, or `None` if no
    /// versioned policy has been applied.
    #[prost(int64, optional, tag = "4")]
    pub applied_policy_version: Option<i64>,
}

/// A configured policy and its state-machine ID.
#[derive(Clone, PartialEq, prost::Message)]
pub struct VersionedPolicy {
    /// Active `DeciderPolicy` value.
    #[prost(int32, tag = "1")]
    pub policy: i32,
    /// Log position of the policy entry that created this state machine.
    #[prost(int64, tag = "2")]
    pub id: i64,
}

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

impl PerBusEngineState {
    /// Load state from storage. Returns `(state, stored_position)` where
    /// `stored_position` is `None` if the key doesn't exist (for CAS).
    pub async fn load(
        storage: &impl Storage,
        bus_id: &str,
    ) -> std::result::Result<(Self, Option<i64>), EngineStateLoadError> {
        let stored = storage
            .get(&engine_state_key(bus_id))
            .await
            .map_err(EngineStateLoadError::Storage)?;
        Ok(match stored {
            Some((b, pos)) => (
                Self::decode(b.as_ref()).map_err(EngineStateLoadError::Decode)?,
                Some(pos),
            ),
            None => (Self::default(), None),
        })
    }

    pub async fn save(
        &self,
        storage: &impl Storage,
        bus_id: &str,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        storage
            .put(
                &engine_state_key(bus_id),
                Bytes::from(self.encode_to_vec()),
                expected,
                new_position,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::storage::FaultyStorage;

    #[test]
    fn load_preserves_storage_error_category() {
        let error = block_on(PerBusEngineState::load(&FaultyStorage::GetTimeout, "bus-1"))
            .expect_err("storage get should fail");

        assert!(
            matches!(
                error,
                EngineStateLoadError::Storage(StorageError::Timeout(_))
            ),
            "storage category should survive the engine-state boundary, got {error:?}"
        );
    }
}
