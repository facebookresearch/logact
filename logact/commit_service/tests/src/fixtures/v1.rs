/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixture that builds a `CommitServiceV1` over any AgentBus fixture
//! from `agentbus_tests`.

use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::NoopLogger;
use agentbus_api::RealEnvironment;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::SimulatorFixture as BusSimulatorFixture;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_engine_tests::fixtures::fault_injecting::FaultInjectingStorage;
use logact_commit_service_engine_tests::fixtures::fault_injecting::StorageFaultConfig;
use logact_commit_service_engine_tests::voters::CountingVoterFactory;
use logact_commit_service_engine_tests::voters::counting_voter_config;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::StaticConfigPolicyProvider;
use logact_commit_service_v1::SynchronousRegisterProvider;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SettableDeciderPolicy;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::fixtures::build_v1;
use crate::simulator::Simulator;

const IN_FLIGHT_WRITE_DELAY: Duration = Duration::from_millis(50);
const COUNTING_VOTER_MODULUS: u64 = 2;

/// The engine reads its decider policy from `policy`, a `TestPolicyProvider` the
/// fixture owns. Generic over the provider `P`; the variant list pins it to the
/// default `SynchronousRegisterProvider`, while `#[scenario_for]` tests can pin a
/// different provider. Runtime updates use the provider's separate test controller.
pub struct CommitServiceV1Fixture<
    BF: AgentBusTestFixture,
    P = SynchronousRegisterProvider<InMemoryStorage>,
    S = InMemoryStorage,
> where
    P: TestPolicyProvider,
{
    bus_fixture: BF,
    policy: P,
    policy_controller: P::Controller,
    make_storage: Rc<dyn Fn(Rc<BF::Env>) -> S>,
}

impl<BF, P, S> CommitServiceV1Fixture<BF, P, S>
where
    BF: AgentBusTestFixture,
    P: TestPolicyProvider,
{
    pub fn new_with_bus_fixture_and_storage(
        bus_fixture: BF,
        make_storage: impl Fn(Rc<BF::Env>) -> S + 'static,
    ) -> Self {
        let (policy, policy_controller) = P::for_test();
        Self {
            bus_fixture,
            policy,
            policy_controller,
            make_storage: Rc::new(make_storage),
        }
    }
}

impl<BF, P> CommitServiceV1Fixture<BF, P>
where
    BF: AgentBusTestFixture,
    P: TestPolicyProvider,
{
    pub fn new_with_bus_fixture(bus_fixture: BF) -> Self {
        Self::new_with_bus_fixture_and_storage(bus_fixture, |_| InMemoryStorage::new())
    }
}

impl<BF, P, S> ConformanceFixture for CommitServiceV1Fixture<BF, P, S>
where
    BF: AgentBusTestFixture,
    BF::Impl: Clone + 'static,
    P: TestPolicyProvider,
    S: Storage + 'static,
{
    type Env = BF::Env;
    type Impl = CommitServiceV1<
        BF::Impl,
        DelegatingVoterFactory<NoopLogger, RealEnvironment, S>,
        S,
        P,
        DeciderFactoryImpl<S>,
        BF::Env,
    >;

    fn get_env(&self) -> Rc<Self::Env> {
        self.bus_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let environment = self.bus_fixture.get_env();
        let storage = Rc::new((self.make_storage)(environment.clone()));
        build_v1(
            self.bus_fixture.create_impl(),
            storage,
            DelegatingVoterFactory::with_storage,
            self.policy.clone(),
            environment,
        )
    }
}

impl<BF, P> SimulatorFixture for CommitServiceV1Fixture<BF, P>
where
    BF: AgentBusTestFixture<Env = Simulator> + BusSimulatorFixture,
    P: TestPolicyProvider,
{
    fn new(simulator: Simulator) -> Self {
        Self::new_with_bus_fixture(BF::new(simulator))
    }
}

impl<BF, P, S> DeciderPolicyFixture for CommitServiceV1Fixture<BF, P, S>
where
    BF: AgentBusTestFixture<Env = Simulator>,
    BF::Impl: Clone,
    P: TestPolicyProvider,
    P::Controller: SettableDeciderPolicy,
    S: Storage + 'static,
{
    async fn set_decider_policy(&self, policy: DeciderPolicy) -> anyhow::Result<()> {
        self.policy_controller.set_decider_policy(policy).await
    }
}

/// A V1 fixture that exercises storage-conflict recovery through `CommitSvc`.
pub struct CommitServiceV1StorageConflictFixture<BF> {
    bus_fixture: BF,
    storage: Rc<FaultInjectingStorage<InMemoryStorage, Simulator>>,
}

impl<BF> ConformanceFixture for CommitServiceV1StorageConflictFixture<BF>
where
    BF: AgentBusTestFixture<Env = Simulator>,
    BF::Impl: Clone + 'static,
{
    type Env = Simulator;
    type Impl = CommitServiceV1<
        BF::Impl,
        CountingVoterFactory<FaultInjectingStorage<InMemoryStorage, Simulator>>,
        FaultInjectingStorage<InMemoryStorage, Simulator>,
        StaticConfigPolicyProvider,
        DeciderFactoryImpl<FaultInjectingStorage<InMemoryStorage, Simulator>>,
        Simulator,
    >;

    fn get_env(&self) -> Rc<Self::Env> {
        self.bus_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let storage = self.storage.clone();
        let voter_factory = CountingVoterFactory::new(storage.clone());
        let policy_validator = voter_factory.clone();
        let policy = StaticConfigPolicyProvider::new(
            DeciderPolicy::FirstBooleanWins,
            vec![counting_voter_config(COUNTING_VOTER_MODULUS)],
            move |policy| validate_voter_configs(&policy_validator, policy),
        );
        let decider_factory = DeciderFactoryImpl::new(storage.clone());
        let environment = self.bus_fixture.get_env();
        let bus = self.bus_fixture.create_impl();

        CommitServiceV1::new(bus, move |bus| {
            BaseEngine::new(
                bus,
                storage,
                voter_factory,
                policy,
                decider_factory,
                environment,
            )
        })
    }
}

impl<BF> SimulatorFixture for CommitServiceV1StorageConflictFixture<BF>
where
    BF: AgentBusTestFixture<Env = Simulator> + BusSimulatorFixture,
{
    fn new(simulator: Simulator) -> Self {
        let bus_fixture = BF::new(simulator);
        let storage = Rc::new(FaultInjectingStorage::new(
            InMemoryStorage::new(),
            bus_fixture.get_env(),
            StorageFaultConfig {
                put_delay: IN_FLIGHT_WRITE_DELAY,
                conflict_on_overlapping_put: true,
                ..Default::default()
            },
        ));
        Self {
            bus_fixture,
            storage,
        }
    }
}
