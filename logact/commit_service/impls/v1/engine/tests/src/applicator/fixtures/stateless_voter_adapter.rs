/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agentbus_api::voter::Voter;
use agentbus_api::voter::VoterContext;
use agentbus_simulator::Simulator;
use logact_commit_service_engine::ImmutableVoter;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::StatelessVoterAdapter;

use crate::applicator::fixtures::ApplicatorTestFixture;
use crate::applicator::fixtures::intention_entry;
use crate::fixtures::SimulatorFixture;

/// A non-deterministic voter that flips its verdict on every call, so the replay
/// scenario also proves the adapter replays its stored vote rather than
/// re-evaluating.
struct ToggleVoter {
    next: Cell<bool>,
}

#[async_trait::async_trait(?Send)]
impl Voter for ToggleVoter {
    async fn evaluate(&self, _context: VoterContext<'_>) -> (bool, String) {
        let v = self.next.get();
        self.next.set(!v);
        (v, String::new())
    }

    fn apply_policy(&mut self, _config: &prost_types::Any) {}

    fn describe(&self) -> String {
        "ToggleVoter".to_string()
    }
}

pub struct StatelessVoterAdapterFixture {
    env: Rc<Simulator>,
}

impl ApplicatorTestFixture for StatelessVoterAdapterFixture {
    type Env = Simulator;
    type Impl = StatelessVoterAdapter<InMemoryStorage>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        StatelessVoterAdapter::new(
            ImmutableVoter::new(
                "1".to_string(),
                VoterConfig { config: None },
                Rc::new(ToggleVoter {
                    next: Cell::new(true),
                }),
            ),
            Rc::new(InMemoryStorage::new()),
        )
    }

    fn make_entry(&self, position: i64) -> BusEntry {
        intention_entry(position)
    }
}

impl SimulatorFixture for StatelessVoterAdapterFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            env: Rc::new(simulator),
        }
    }
}
