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

    async fn save(
        &self,
        bus_id: &str,
        checkpoint: &Checkpoint,
        expected: Option<i64>,
    ) -> CommitResult<()> {
        let mut checkpoints = self.checkpoints.borrow_mut();
        let stored = checkpoints.get(bus_id).map(|stored| stored.position);
        if stored == expected {
            checkpoints.insert(bus_id.to_owned(), checkpoint.clone());
        } else {
            tracing::info!(
                bus_id,
                position = checkpoint.position,
                ?expected,
                ?stored,
                "checkpoint save lost a race; another drive saved first"
            );
        }
        Ok(())
    }
}
