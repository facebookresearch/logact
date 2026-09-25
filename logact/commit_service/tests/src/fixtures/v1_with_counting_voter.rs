/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A `CommitServiceV1` fixture whose safety pipeline denies every other
//! intention, for exercising the abort path under linearizability testing.
//!
//! The denial logic is the shared `CountingVoter` from the engine tests. It is
//! wired into v1 via the engine tests' `CountingVoterFactory` plus a
//! `StaticConfigPolicyProvider` that installs `FIRST_BOOLEAN_WINS` and the voter
//! config, so a denied vote turns into an `Abort` and `commit_intention` returns
//! `approved = false`.

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::SimulatorFixture as BusSimulatorFixture;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_engine_tests::voters::CountingVoterFactory;
use logact_commit_service_engine_tests::voters::counting_voter_config;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::StaticConfigPolicyProvider;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::build_v1;
use crate::simulator::Simulator;

/// Deny every other intention.
const MODULUS: u64 = 2;

/// A `CommitServiceV1` fixture that denies every `MODULUS`-th intention.
pub struct CommitServiceV1CountingVoterFixture<BF: AgentBusTestFixture> {
    bus_fixture: BF,
}

impl<BF> ConformanceFixture for CommitServiceV1CountingVoterFixture<BF>
where
    BF: AgentBusTestFixture,
    BF::Impl: Clone + 'static,
{
    type Env = BF::Env;
    type Impl = CommitServiceV1<
        BF::Impl,
        CountingVoterFactory,
        InMemoryStorage,
        StaticConfigPolicyProvider,
        DeciderFactoryImpl,
        BF::Env,
    >;

    fn get_env(&self) -> Rc<Self::Env> {
        self.bus_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let policy = StaticConfigPolicyProvider::new(
            DeciderPolicy::FirstBooleanWins,
            vec![counting_voter_config(MODULUS)],
            |policy| validate_voter_configs(&CountingVoterFactory::default(), policy),
        );
        build_v1(
            self.bus_fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            CountingVoterFactory::new,
            policy,
            self.bus_fixture.get_env(),
        )
    }
}

impl<BF> SimulatorFixture for CommitServiceV1CountingVoterFixture<BF>
where
    BF: AgentBusTestFixture<Env = Simulator> + BusSimulatorFixture,
{
    fn new(simulator: Simulator) -> Self {
        Self {
            bus_fixture: BF::new(simulator),
        }
    }
}
