/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use logact_commit_service_api::CommitResult;
use logact_commit_service_replay::Checkpoint;
use logact_commit_service_replay::CheckpointStore;

/// One server's private checkpoints, held in memory. Servers never see each
/// other's progress, so each replays whatever the others appended since its own
/// checkpoint.
#[derive(Clone, Default)]
pub struct LocalCheckpoints {
    checkpoints: Rc<RefCell<HashMap<String, Checkpoint>>>,
}

impl CheckpointStore for LocalCheckpoints {
    async fn load(&self, bus_id: &str) -> CommitResult<Option<Checkpoint>> {
        Ok(self.checkpoints.borrow().get(bus_id).cloned())
    }

    async fn save(&self, bus_id: &str, checkpoint: &Checkpoint) -> CommitResult<()> {
        let mut checkpoints = self.checkpoints.borrow_mut();
        if checkpoints
            .get(bus_id)
            .is_none_or(|stored| stored.position < checkpoint.position)
        {
            checkpoints.insert(bus_id.to_owned(), checkpoint.clone());
        }
        Ok(())
    }
}
