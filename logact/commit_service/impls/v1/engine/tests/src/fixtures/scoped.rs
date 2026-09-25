/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_simulator::Simulator;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::ScopedStorage;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;

/// Runs the generic Storage conformance suite against a `ScopedStorage` wrapping
/// an `InMemoryStorage`, so the prefix adapter is exercised as a full `Storage`.
pub struct ScopedStorageFixture {
    env: Rc<Simulator>,
}

impl ConformanceFixture for ScopedStorageFixture {
    type Env = Simulator;
    type Impl = ScopedStorage<InMemoryStorage>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        ScopedStorage::new("scope/", InMemoryStorage::new())
    }
}

impl SimulatorFixture for ScopedStorageFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            env: Rc::new(simulator),
        }
    }
}
