/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use logact_commit_service_api::CommitSvc;
use logact_commit_service_core::BufferingCommitService;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

/// Wraps a commit-service fixture with queues shared by all its service handles.
pub struct BufferingCommitServiceFixture<CF>
where
    CF: ConformanceFixture,
    CF::Impl: CommitSvc,
{
    inner: CF,
    service: BufferingCommitService<CF::Impl>,
}

impl<CF> ConformanceFixture for BufferingCommitServiceFixture<CF>
where
    CF: ConformanceFixture,
    CF::Impl: CommitSvc,
{
    type Env = CF::Env;
    type Impl = BufferingCommitService<CF::Impl>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.service.clone()
    }
}

impl<CF> SimulatorFixture for BufferingCommitServiceFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator> + SimulatorFixture,
    CF::Impl: CommitSvc,
{
    fn new(simulator: Simulator) -> Self {
        let inner = CF::new(simulator);
        let service = BufferingCommitService::new(inner.create_impl());
        Self { inner, service }
    }
}

impl<CF> DeciderPolicyFixture for BufferingCommitServiceFixture<CF>
where
    CF: DeciderPolicyFixture,
{
    type Controller = CF::Controller;

    fn policy_controller(&self) -> Self::Controller {
        self.inner.policy_controller()
    }
}
