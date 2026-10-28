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

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::PolicyRegister;
use logact_commit_service_inmem::InMemCommitService;
use logact_commit_service_v1::SynchronousRegisterProvider;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SettableDeciderPolicy;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::simulator::Simulator;

pub struct InMemCommitServiceFixture {
    env: Rc<Simulator>,
    service: InMemCommitService<Simulator, SynchronousRegisterProvider<InMemoryStorage>>,
    policy_controller: PolicyRegister<InMemoryStorage>,
}

impl ConformanceFixture for InMemCommitServiceFixture {
    type Env = Simulator;
    type Impl = InMemCommitService<Simulator, SynchronousRegisterProvider<InMemoryStorage>>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.service.clone()
    }
}

impl SimulatorFixture for InMemCommitServiceFixture {
    fn new(simulator: Simulator) -> Self {
        let env = Rc::new(simulator);
        let (policy, policy_controller) = SynchronousRegisterProvider::for_test();
        Self {
            service: InMemCommitService::with_policy_provider(env.clone(), policy),
            policy_controller,
            env,
        }
    }
}

impl DeciderPolicyFixture for InMemCommitServiceFixture {
    async fn set_decider_policy(&self, policy: DeciderPolicy) -> anyhow::Result<()> {
        self.policy_controller.set_decider_policy(policy).await
    }
}
