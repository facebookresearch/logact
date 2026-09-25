/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agentbus_simulator::Simulator;
use logact_commit_service_engine::FirstBooleanWinsApplicator;
use logact_commit_service_engine::InMemoryStorage;

use crate::applicator::fixtures::ApplicatorTestFixture;
use crate::applicator::fixtures::intention_entry;
use crate::fixtures::SimulatorFixture;

pub struct DeciderApplicatorFixture {
    env: Rc<Simulator>,
}

impl ApplicatorTestFixture for DeciderApplicatorFixture {
    type Env = Simulator;
    type Impl = FirstBooleanWinsApplicator<InMemoryStorage>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        FirstBooleanWinsApplicator::new(Rc::new(InMemoryStorage::new()), None)
    }

    fn make_entry(&self, position: i64) -> BusEntry {
        // FIRST_BOOLEAN_WINS persists each intention position.
        intention_entry(position)
    }
}

impl SimulatorFixture for DeciderApplicatorFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            env: Rc::new(simulator),
        }
    }
}
