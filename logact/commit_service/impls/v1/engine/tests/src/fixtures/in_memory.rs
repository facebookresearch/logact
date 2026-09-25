/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_simulator::Simulator;
use logact_commit_service_engine::InMemoryStorage;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;

pub struct InMemoryStorageFixture {
    env: Rc<Simulator>,
}

impl ConformanceFixture for InMemoryStorageFixture {
    type Env = Simulator;
    type Impl = InMemoryStorage;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        InMemoryStorage::new()
    }
}

impl SimulatorFixture for InMemoryStorageFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            env: Rc::new(simulator),
        }
    }
}
