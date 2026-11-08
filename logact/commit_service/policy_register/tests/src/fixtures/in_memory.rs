/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_simulator::Simulator;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_policy_register::StoragePolicyRegister;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;

pub struct InMemoryPolicyRegisterFixture {
    env: Rc<Simulator>,
    register: StoragePolicyRegister<InMemoryStorage>,
}

impl ConformanceFixture for InMemoryPolicyRegisterFixture {
    type Env = Simulator;
    type Impl = StoragePolicyRegister<InMemoryStorage>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.register.clone()
    }
}

impl SimulatorFixture for InMemoryPolicyRegisterFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            env: Rc::new(simulator),
            register: StoragePolicyRegister::new(Rc::new(InMemoryStorage::new()), "test-policy"),
        }
    }
}
