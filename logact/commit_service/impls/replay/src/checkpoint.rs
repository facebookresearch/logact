/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use logact_commit_service_api::CommitResult;

use crate::ReplayState;

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
    ) -> impl std::future::Future<Output = CommitResult<Option<Checkpoint>>>;

    /// Save `checkpoint` if the stored checkpoint is still at `expected`; a
    /// store may decline otherwise.
    fn save(
        &self,
        bus_id: &str,
        checkpoint: &Checkpoint,
        expected: Option<i64>,
    ) -> impl std::future::Future<Output = CommitResult<()>>;
}
