/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixture that wraps any commit service fixture in `ObservableCommitSvc`.

use std::rc::Rc;

use agentbus_api::InMemoryLogger;
use agentbus_api::InMemoryMetrics;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_observable::ObservableCommitSvc;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

/// Wraps every service from an inner `CommitSvc` fixture in an
/// `ObservableCommitSvc`. All handles share one in-memory metrics store and row
/// logger, so the decorator's recording path runs on every call.
pub struct ObservableCommitServiceFixture<CF> {
    inner: CF,
    metrics: Rc<InMemoryMetrics>,
    logger: InMemoryLogger,
}

impl<CF> ConformanceFixture for ObservableCommitServiceFixture<CF>
where
    CF: ConformanceFixture,
    CF::Impl: CommitSvc,
{
    type Env = CF::Env;
    type Impl = ObservableCommitSvc<CF::Impl, CF::Env>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        ObservableCommitSvc::new(
            self.inner.create_impl(),
            self.metrics.clone(),
            Rc::new(self.logger.clone()),
            self.inner.get_env(),
        )
    }
}

impl<CF: SimulatorFixture> SimulatorFixture for ObservableCommitServiceFixture<CF> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: CF::new(simulator),
            metrics: Rc::new(InMemoryMetrics::new()),
            logger: InMemoryLogger::new(),
        }
    }
}

impl<CF> DeciderPolicyFixture for ObservableCommitServiceFixture<CF>
where
    CF: DeciderPolicyFixture,
{
    type Controller = CF::Controller;

    fn policy_controller(&self) -> Self::Controller {
        self.inner.policy_controller()
    }
}
