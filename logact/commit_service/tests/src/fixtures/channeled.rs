/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixture that wraps any commit service fixture in `ChanneledCommitService`.

use std::rc::Rc;

use logact_commit_service_api::CommitSvc;
use logact_commit_service_core::ChanneledCommitService;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

/// Wraps any `CommitSvc` fixture (with `Env = Simulator`) in a
/// `ChanneledCommitService`. The inner fixture's underlying service runs on
/// the simulator's local executor; the channeled handle is `Clone`, so every
/// `create_impl` call hands out a fresh sender for the same worker.
pub struct ChanneledCommitServiceFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator>,
    CF::Impl: CommitSvc,
{
    channeled: ChanneledCommitService<<CF::Impl as CommitSvc>::Bus>,
    env: Rc<Simulator>,
    // Kept alive so the inner fixture's shared state outlives the channeled handle.
    _inner: CF,
}

impl<CF> ConformanceFixture for ChanneledCommitServiceFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator>,
    CF::Impl: CommitSvc,
    <CF::Impl as CommitSvc>::Bus: Clone,
{
    type Env = Simulator;
    type Impl = ChanneledCommitService<<CF::Impl as CommitSvc>::Bus>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.channeled.clone()
    }
}

impl<CF> SimulatorFixture for ChanneledCommitServiceFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator> + SimulatorFixture,
    CF::Impl: CommitSvc + 'static,
    <CF::Impl as CommitSvc>::Bus: Clone + 'static,
{
    fn new(simulator: Simulator) -> Self {
        let inner = <CF as SimulatorFixture>::new(simulator);
        let env = inner.get_env();
        let inner_impl = inner.create_impl();
        let bus = inner_impl.agent_bus().clone();
        let channeled =
            ChanneledCommitService::new_on_environment(env.clone(), bus, move |_| inner_impl);
        Self {
            channeled,
            env,
            _inner: inner,
        }
    }
}
