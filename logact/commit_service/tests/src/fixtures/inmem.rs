/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixture for the standalone in-memory `CommitSvc` (`InMemCommitService`).
//!
//! `create_impl` clones a handle onto one shared in-memory bus, so repeated calls
//! behave as a single logical service.

use std::rc::Rc;

use anyhow::Context as _;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_inmem::InMemCommitService;
use logact_commit_service_v1::SynchronousRegisterProvider;

use crate::fixtures::CheckpointFixture;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::simulator::Simulator;

pub struct InMemCommitServiceFixture<P = SynchronousRegisterProvider<InMemoryStorage>>
where
    P: TestPolicyProvider,
{
    env: Rc<Simulator>,
    service: InMemCommitService<Simulator, P>,
    policy_controller: P::Controller,
}

impl<P: TestPolicyProvider> ConformanceFixture for InMemCommitServiceFixture<P> {
    type Env = Simulator;
    type Impl = InMemCommitService<Simulator, P>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.service.clone()
    }
}

impl<P: TestPolicyProvider> SimulatorFixture for InMemCommitServiceFixture<P> {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    /// `ttl` expires entries on the service's own bus.
    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let env = Rc::new(simulator);
        let (policy_provider, policy_controller) = P::for_test(env.clone());
        let service = InMemCommitService::with_policy_provider(env.clone(), policy_provider);
        Ok(Self {
            service: match config.ttl {
                Some(ttl) => service.with_ttl(ttl),
                None => service,
            },
            policy_controller,
            env,
        })
    }
}

impl<P: TestPolicyProvider> CheckpointFixture for InMemCommitServiceFixture<P> {
    async fn checkpoint_position(&self, service: &Self::Impl, bus_id: &str) -> Result<i64> {
        service
            .checkpoint_position(bus_id)
            .context("no replay state")
    }

    fn is_checkpoint_durable(&self) -> bool {
        true
    }
}

impl<P> DeciderPolicyFixture for InMemCommitServiceFixture<P>
where
    P: TestPolicyProvider,
    P::Controller: crate::fixtures::SettableDeciderPolicy,
{
    type Controller = P::Controller;

    fn policy_controller(&self) -> Self::Controller {
        self.policy_controller.clone()
    }
}
