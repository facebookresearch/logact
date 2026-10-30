/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Deliberately stale CommitSvc response positions for checker validation.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::InMemCommitServiceFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

/// Commits through the real service but reports its first intention position
/// for every subsequent call on the same bus.
#[derive(Clone)]
pub struct BuggyStalePositionCommitService<S> {
    inner: S,
    first_positions: Rc<RefCell<HashMap<String, i64>>>,
}

impl<S: CommitSvc> CommitSvc for BuggyStalePositionCommitService<S> {
    type Bus = S::Bus;

    fn agent_bus(&self) -> &Self::Bus {
        self.inner.agent_bus()
    }

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.inner.create_bus(request).await
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        let bus_id = request.bus_id.agent_bus_id.clone();
        let mut outcome = self.inner.commit_intention(request).await?;
        outcome.log_position = *self
            .first_positions
            .borrow_mut()
            .entry(bus_id)
            .or_insert(outcome.log_position);
        Ok(outcome)
    }
}

/// Shares stale response positions across all handles of one in-memory service.
pub struct BuggyStalePositionCommitServiceFixture {
    inner: InMemCommitServiceFixture,
    first_positions: Rc<RefCell<HashMap<String, i64>>>,
}

impl SimulatorFixture for BuggyStalePositionCommitServiceFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: InMemCommitServiceFixture::new(simulator),
            first_positions: Rc::default(),
        }
    }
}

impl ConformanceFixture for BuggyStalePositionCommitServiceFixture {
    type Env = Simulator;
    type Impl =
        BuggyStalePositionCommitService<<InMemCommitServiceFixture as ConformanceFixture>::Impl>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        BuggyStalePositionCommitService {
            inner: self.inner.create_impl(),
            first_positions: self.first_positions.clone(),
        }
    }
}
