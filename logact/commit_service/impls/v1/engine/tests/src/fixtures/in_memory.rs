/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_simulator::Simulator;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::RetryConfig;

use crate::fixtures::Backend;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::EnvStorageFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::backend;

pub struct InMemoryStorageFixture {
    env: Rc<Simulator>,
    retry: RetryConfig,
}

impl ConformanceFixture for InMemoryStorageFixture {
    type Env = Simulator;
    type Impl = Backend<InMemoryStorage>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        backend(InMemoryStorage::new(), &self.env, self.retry)
    }
}

impl EnvStorageFixture for InMemoryStorageFixture {
    /// `InMemoryStorage` has no TTL, so `ttl` is ignored.
    fn try_new_on_env(env: Rc<Simulator>, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            env,
            retry: config.retry_config(),
        })
    }
}

impl SimulatorFixture for InMemoryStorageFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Self::try_new_on_env(Rc::new(simulator), config)
    }
}
