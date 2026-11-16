/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use anyhow::Context;
use bytes::Bytes;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitResult;
use logact_commit_service_engine::PerBusEngineState;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::StorageError;
use logact_commit_service_engine::engine_state_key;
use prost::Message;

use crate::Checkpoint;
use crate::CheckpointStore;
use crate::ReplayState;

/// Checkpoints in a V1 `Storage` engine, one record per bus, stored as V1's
/// engine state under V1's key, with the decider's state in
/// `replay_decider_state`. Under `ON_BY_DEFAULT` and `OFF_BY_DEFAULT` that field
/// is unset, so a record is exactly what V1 writes. V1 records load
/// as checkpoints, and V1 reads these records, so switching between V1 and this
/// store needs no migration in either direction.
///
/// Servers that share the storage resume from the furthest any of them has
/// saved; a server with its own storage resumes from its own last save.
pub struct StorageCheckpoints<S> {
    storage: Rc<S>,
}

impl<S> StorageCheckpoints<S> {
    pub fn new(storage: Rc<S>) -> Self {
        Self { storage }
    }
}

impl<S> Clone for StorageCheckpoints<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

/// Encode `state` as a V1 engine-state record.
pub(crate) fn encode_engine_record(state: &ReplayState) -> Vec<u8> {
    state.to_engine_state().encode_to_vec()
}

/// Decode a V1 engine-state record, written by V1 or by this store.
pub(crate) fn decode_engine_record(bytes: &[u8]) -> CommitResult<ReplayState> {
    let state = PerBusEngineState::decode(bytes)
        .context("decoding an engine-state record")
        .map_err(CommitError::Internal)?;
    ReplayState::from_engine_state(state)
}

impl<S: Storage> CheckpointStore for StorageCheckpoints<S> {
    async fn load(&self, bus_id: &str) -> CommitResult<Option<Checkpoint>> {
        let Some((bytes, position)) = self
            .storage
            .get(&engine_state_key(bus_id))
            .await
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        Ok(Some(Checkpoint {
            state: decode_engine_record(&bytes)?,
            position,
        }))
    }

    /// The storage CAS rejects the save if another drive saved first.
    async fn save(
        &self,
        bus_id: &str,
        checkpoint: &Checkpoint,
        expected: Option<i64>,
    ) -> CommitResult<()> {
        let saved = self
            .storage
            .put(
                &engine_state_key(bus_id),
                Bytes::from(encode_engine_record(&checkpoint.state)),
                expected,
                checkpoint.position,
            )
            .await
            .map_err(storage_error)?;
        if !saved {
            tracing::info!(
                bus_id,
                position = checkpoint.position,
                ?expected,
                "checkpoint save lost a race; another drive saved first"
            );
        }
        Ok(())
    }
}

pub(crate) fn storage_error(error: StorageError) -> CommitError {
    match error {
        error @ StorageError::TransactionConflict(_) => {
            CommitError::Concurrency(anyhow::Error::new(error))
        }
        error @ StorageError::Timeout(_) => CommitError::Timeout(anyhow::Error::new(error)),
        error @ StorageError::BackendUnavailable(_) => {
            CommitError::Unavailable(anyhow::Error::new(error))
        }
        error @ StorageError::InternalError(_) => CommitError::Internal(anyhow::Error::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use futures::executor::block_on;
    use logact_commit_service_engine::InMemoryStorage;
    use logact_commit_service_engine::PerBusEngineState;
    use logact_commit_service_engine::VersionedDecider;
    use prost::Message;

    use super::*;

    /// An engine-state record as V1 writes it under `policy`.
    fn v1_record(policy: DeciderPolicy) -> Vec<u8> {
        PerBusEngineState {
            decider: Some(VersionedDecider {
                policy: policy as i32,
                id: 0,
            }),
            applied_policy_version: Some(3),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[test]
    fn v1_and_replay_share_the_engine_state_record() {
        for policy in [DeciderPolicy::OnByDefault, DeciderPolicy::OffByDefault] {
            share_the_engine_state_record(policy);
        }
    }

    fn share_the_engine_state_record(policy: DeciderPolicy) {
        block_on(async {
            let storage = Rc::new(InMemoryStorage::new());
            assert!(
                storage
                    .put(
                        &engine_state_key("bus"),
                        Bytes::from(v1_record(policy)),
                        None,
                        7
                    )
                    .await
                    .unwrap()
            );
            let store = StorageCheckpoints::new(storage.clone());

            let checkpoint = store
                .load("bus")
                .await
                .unwrap()
                .expect("V1's record should load as a checkpoint");
            assert_eq!(checkpoint.position, 7);
            assert_eq!(checkpoint.state.policy.applied_policy_version, Some(3));

            let saved = Checkpoint {
                position: 9,
                ..checkpoint
            };
            store.save("bus", &saved, Some(7)).await.unwrap();
            let (bytes, position) = storage
                .get(&engine_state_key("bus"))
                .await
                .unwrap()
                .expect("the save should replace V1's record");
            assert_eq!(position, 9);
            assert_eq!(
                bytes.as_ref(),
                v1_record(policy).as_slice(),
                "{policy:?} has no decider state, so the checkpoint is exactly V1's record"
            );
        });
    }
}
