/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixture that wraps any commit service fixture in the `channeled_svc`
//! `ChanneledCommitService`.

use std::rc::Rc;

use agentbus_api::ExtremelyUnsafeNoOpGate;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_channeled::ChanneledCommitService;

use crate::fixtures::CheckpointFixture;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

type Channeled<CF> = ChanneledCommitService<<<CF as ConformanceFixture>::Impl as CommitSvc>::Bus>;

/// Wraps any `CommitSvc` fixture (with `Env = Simulator`) in a `channeled_svc`
/// `ChanneledCommitService`. The inner fixture's underlying service runs on
/// the simulator's local executor; the channeled handle is `Clone`, so every
/// `create_impl` call hands out a fresh sender for the same worker.
pub struct ChanneledSvcCommitServiceFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator, Impl: CommitSvc>,
{
    channeled: Channeled<CF>,
    inner: CF,
}

impl<CF> ConformanceFixture for ChanneledSvcCommitServiceFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator, Impl: CommitSvc<Bus: Clone>>,
{
    type Env = Simulator;
    type Impl = Channeled<CF>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.channeled.clone()
    }
}

impl<CF> SimulatorFixture for ChanneledSvcCommitServiceFixture<CF>
where
    CF: SimulatorFixture
        + ConformanceFixture<Env = Simulator, Impl: CommitSvc<Bus: Clone> + 'static>,
{
    fn new(simulator: Simulator) -> Self {
        Self::from_inner(<CF as SimulatorFixture>::new(simulator))
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self::from_inner(CF::try_new_with_config(
            simulator, config,
        )?))
    }
}

impl<CF> ChanneledSvcCommitServiceFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator, Impl: CommitSvc<Bus: Clone> + 'static>,
{
    fn from_inner(inner: CF) -> Self {
        let service = inner.create_impl();
        let bus = service.agent_bus().clone();
        let channeled = ChanneledCommitService::spawn_on(
            inner.get_env(),
            bus,
            service,
            ExtremelyUnsafeNoOpGate,
        );
        Self { channeled, inner }
    }
}

impl<CF> CheckpointFixture for ChanneledSvcCommitServiceFixture<CF>
where
    CF: CheckpointFixture<Env = Simulator, Impl: CommitSvc<Bus: Clone>>,
{
    async fn checkpoint_position(&self, _service: &Self::Impl, bus_id: &str) -> Result<i64> {
        self.inner
            .checkpoint_position(&self.inner.create_impl(), bus_id)
            .await
    }

    fn is_checkpoint_durable(&self) -> bool {
        self.inner.is_checkpoint_durable()
    }
}

impl<CF> DeciderPolicyFixture for ChanneledSvcCommitServiceFixture<CF>
where
    CF: DeciderPolicyFixture<Env = Simulator, Impl: CommitSvc<Bus: Clone>>,
{
    type Controller = CF::Controller;

    fn policy_controller(&self) -> Self::Controller {
        self.inner.policy_controller()
    }

    fn supports_first_boolean_wins(&self) -> bool {
        self.inner.supports_first_boolean_wins()
    }
}
