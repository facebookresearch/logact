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

use logact_commit_service_inmem::InMemCommitService;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

pub struct InMemCommitServiceFixture {
    env: Rc<Simulator>,
    service: InMemCommitService<Simulator>,
}

impl ConformanceFixture for InMemCommitServiceFixture {
    type Env = Simulator;
    type Impl = InMemCommitService<Simulator>;

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
        Self {
            service: InMemCommitService::new(env.clone()),
            env,
        }
    }
}
