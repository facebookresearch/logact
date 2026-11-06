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

use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::PolicyRegister;
use logact_commit_service_inmem::InMemCommitService;
use logact_commit_service_v1::SynchronousRegisterProvider;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::simulator::Simulator;

type PolicyProvider = SynchronousRegisterProvider<InMemoryStorage>;
type PolicyController = PolicyRegister<InMemoryStorage>;

pub struct InMemCommitServiceFixture {
    env: Rc<Simulator>,
    service: InMemCommitService<Simulator, PolicyProvider>,
    policy_controller: PolicyController,
}

impl ConformanceFixture for InMemCommitServiceFixture {
    type Env = Simulator;
    type Impl = InMemCommitService<Simulator, PolicyProvider>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.service.clone()
    }
}

impl SimulatorFixture for InMemCommitServiceFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    /// `ttl` expires entries on the service's own bus.
    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let env = Rc::new(simulator);
        let (policy_provider, policy_controller) = PolicyProvider::for_test();
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

impl DeciderPolicyFixture for InMemCommitServiceFixture {
    type Controller = PolicyController;

    fn policy_controller(&self) -> Self::Controller {
        self.policy_controller.clone()
    }
}
