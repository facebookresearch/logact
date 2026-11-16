/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixture that builds a `ReplayCommitService` over any AgentBus fixture. Each
//! `create_impl` is one server, with the checkpoint store `C` gives it.

use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::NoopLogger;
use agentbus_api::RealEnvironment;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::SimulatorFixture as BusSimulatorFixture;
use anyhow::Context as _;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::VoterFactory as _;
use logact_commit_service_policy_register::StoragePolicyRegister;
use logact_commit_service_replay::CheckpointStore;
use logact_commit_service_replay::ReplayCommitService;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::SynchronousRegisterProvider;

use crate::fixtures::CheckpointFixture;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::simulator::Simulator;
use crate::stores::NoStoreCheckpoints;

type PolicyProvider = SynchronousRegisterProvider<InMemoryStorage>;
type PolicyController = StoragePolicyRegister<InMemoryStorage>;
type VoterFactory = DelegatingVoterFactory<NoopLogger, RealEnvironment>;

/// The checkpoint storage a fixture's servers use, over bus `B`.
pub trait TestCheckpoints<B>: 'static {
    type Store: CheckpointStore;

    /// Whether checkpoints are kept independently of the bus.
    const DURABLE: bool;

    fn new() -> Self;

    /// The store for one more server, with that server's bus and voters.
    fn for_server(&self, bus: &B, voter_factory: &VoterFactory) -> Self::Store;
}

/// Each server loads by materializing its own bus.
pub struct NoStore;

impl<B: AgentBus + Clone + 'static> TestCheckpoints<B> for NoStore {
    type Store = NoStoreCheckpoints<B>;

    const DURABLE: bool = false;

    fn new() -> Self {
        NoStore
    }

    fn for_server(&self, bus: &B, voter_factory: &VoterFactory) -> Self::Store {
        let voter_factory = voter_factory.clone();
        NoStoreCheckpoints::new(
            bus.clone(),
            Rc::new(move |config| voter_factory.validate_config(config)),
        )
    }
}

/// Every server shares the bus and the policy register, plus whatever `C`
/// shares.
pub struct ReplayCommitServiceFixture<BF, C> {
    bus_fixture: BF,
    policy: PolicyProvider,
    policy_controller: PolicyController,
    checkpoints: C,
}

impl<BF, C> ConformanceFixture for ReplayCommitServiceFixture<BF, C>
where
    BF: AgentBusTestFixture,
    C: TestCheckpoints<BF::Impl>,
{
    type Env = BF::Env;
    type Impl = ReplayCommitService<BF::Impl, VoterFactory, PolicyProvider, C::Store>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.bus_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let bus = self.bus_fixture.create_impl();
        let voter_factory = VoterFactory::default();
        let checkpoints = self.checkpoints.for_server(&bus, &voter_factory);
        ReplayCommitService::new(bus, voter_factory, self.policy.clone(), checkpoints)
    }
}

impl<BF, C> SimulatorFixture for ReplayCommitServiceFixture<BF, C>
where
    BF: AgentBusTestFixture<Env = Simulator> + BusSimulatorFixture,
    C: TestCheckpoints<BF::Impl>,
{
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let (policy, policy_controller) = PolicyProvider::for_test();
        Ok(Self {
            bus_fixture: BF::try_new_with_config(simulator, config)?,
            policy,
            policy_controller,
            checkpoints: C::new(),
        })
    }
}

impl<BF, C> CheckpointFixture for ReplayCommitServiceFixture<BF, C>
where
    BF: AgentBusTestFixture,
    C: TestCheckpoints<BF::Impl>,
{
    async fn checkpoint_position(&self, service: &Self::Impl, bus_id: &str) -> Result<i64> {
        let checkpoint = service.checkpoints().load(bus_id).await?;
        Ok(checkpoint.context("no checkpoint")?.position)
    }

    fn is_checkpoint_durable(&self) -> bool {
        C::DURABLE
    }
}

impl<BF, C> DeciderPolicyFixture for ReplayCommitServiceFixture<BF, C>
where
    BF: AgentBusTestFixture,
    C: TestCheckpoints<BF::Impl>,
{
    type Controller = PolicyController;

    fn policy_controller(&self) -> Self::Controller {
        self.policy_controller.clone()
    }
}
