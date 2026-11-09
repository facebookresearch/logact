/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Simulator-based test fixtures for ConditionalWriteSpace implementations

use std::rc::Rc;

use agentbus_conditional_write_space::InMemoryConditionalWriteSpace;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::TestConfig;

use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

pub struct InMemoryConditionalWriteSpaceFixture {
    space: InMemoryConditionalWriteSpace,
    env: Rc<Simulator>,
}

impl ConformanceFixture for InMemoryConditionalWriteSpaceFixture {
    type Env = Simulator;
    type Impl = InMemoryConditionalWriteSpace;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.space.clone()
    }
}

impl SimulatorFixture for InMemoryConditionalWriteSpaceFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let env = Rc::new(simulator);
        let space = match config.ttl {
            Some(ttl) => InMemoryConditionalWriteSpace::with_ttl(env.clone(), ttl),
            None => InMemoryConditionalWriteSpace::new(env.clone()),
        };
        Ok(Self { space, env })
    }
}
