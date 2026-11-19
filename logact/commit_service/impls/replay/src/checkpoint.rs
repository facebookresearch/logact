/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use logact_commit_service_engine::StorageError;
use thiserror::Error;

use crate::ReplayState;

/// Failure to load or save a checkpoint.
#[derive(Debug, Error)]
pub enum CheckpointError {
    /// The checkpoint's storage backend failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A checkpoint dependency was unavailable.
    #[error(transparent)]
    Unavailable(anyhow::Error),
    /// The checkpoint could not be constructed or decoded.
    #[error(transparent)]
    Internal(anyhow::Error),
}

pub type CheckpointResult<T> = Result<T, CheckpointError>;

/// The state materialized from a bus's log through `position`, inclusive. Every
/// output produced by the entries through `position` is logged before a
/// checkpoint is saved.
#[derive(Clone, Debug)]
pub struct Checkpoint {
    pub state: ReplayState,
    pub position: i64,
}

/// Where a drive's starting state comes from. `load` returns the state as of
/// some position, possibly behind the tail; the drive materializes forward from
/// there. A missing or stale checkpoint never changes an outcome.
pub trait CheckpointStore {
    fn load(
        &self,
        bus_id: &str,
    ) -> impl std::future::Future<Output = CheckpointResult<Option<Checkpoint>>>;

    /// Save `checkpoint` unless the store already holds one for the bus at or
    /// past its position, so a late or out-of-order save can't replace a
    /// further checkpoint.
    fn save(
        &self,
        bus_id: &str,
        checkpoint: &Checkpoint,
    ) -> impl std::future::Future<Output = CheckpointResult<()>>;
}
