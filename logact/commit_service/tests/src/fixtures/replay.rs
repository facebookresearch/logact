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
use agentbus_tests::fixtures::TailWindowFixture;
use anyhow::Context as _;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::VoterFactory as _;
use logact_commit_service_policy_register::StoragePolicyRegister;
use logact_commit_service_replay::BackgroundCheckpoints;
use logact_commit_service_replay::CheckpointStore;
use logact_commit_service_replay::ReplayCommitService;
use logact_commit_service_replay::StorageCheckpoints;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::SynchronousRegisterProvider;

use crate::fixtures::CheckpointFixture;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::simulator::Simulator;
pub use crate::stores::LocalCheckpoints;
use crate::stores::NoStoreCheckpoints;

type PolicyProvider = SynchronousRegisterProvider<InMemoryStorage>;
type PolicyController = StoragePolicyRegister<InMemoryStorage>;
type VoterFactory = DelegatingVoterFactory<NoopLogger, RealEnvironment>;

/// The checkpoint storage a fixture's servers use, over bus `B`.
pub trait TestCheckpoints<B>: 'static {
    type Store: CheckpointStore;

    /// Whether checkpoints are kept independently of the bus.
    const DURABLE: bool;

    /// For a fixture whose servers run on `environment`.
    fn new(environment: Rc<Simulator>) -> Self;

    /// The store for one more server, with that server's bus and voters.
    fn for_server(&self, bus: &B, voter_factory: &VoterFactory) -> Self::Store;
}

/// Each server loads by materializing its own bus.
pub struct NoStore;

impl<B: AgentBus + Clone + 'static> TestCheckpoints<B> for NoStore {
    type Store = NoStoreCheckpoints<B>;

    const DURABLE: bool = false;

    fn new(_environment: Rc<Simulator>) -> Self {
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

/// Each server gets its own empty store.
impl<B> TestCheckpoints<B> for LocalCheckpoints {
    type Store = LocalCheckpoints;

    const DURABLE: bool = false;

    fn new(_environment: Rc<Simulator>) -> Self {
        LocalCheckpoints::default()
    }

    fn for_server(&self, _bus: &B, _voter_factory: &VoterFactory) -> LocalCheckpoints {
        LocalCheckpoints::default()
    }
}

/// Each server gets its own in-memory storage.
pub struct IndependentStorage;

impl<B> TestCheckpoints<B> for IndependentStorage {
    type Store = StorageCheckpoints<InMemoryStorage>;

    const DURABLE: bool = false;

    fn new(_environment: Rc<Simulator>) -> Self {
        IndependentStorage
    }

    fn for_server(&self, _bus: &B, _voter_factory: &VoterFactory) -> Self::Store {
        StorageCheckpoints::new(Rc::new(InMemoryStorage::new()))
    }
}

/// Every server shares one in-memory storage.
pub struct SharedStorage(Rc<InMemoryStorage>);

impl<B> TestCheckpoints<B> for SharedStorage {
    type Store = StorageCheckpoints<InMemoryStorage>;

    const DURABLE: bool = true;

    fn new(_environment: Rc<Simulator>) -> Self {
        SharedStorage(Rc::new(InMemoryStorage::new()))
    }

    fn for_server(&self, _bus: &B, _voter_factory: &VoterFactory) -> Self::Store {
        StorageCheckpoints::new(self.0.clone())
    }
}

/// Every server shares one in-memory storage and saves to it in the background.
pub struct BackgroundSharedStorage {
    storage: Rc<InMemoryStorage>,
    environment: Rc<Simulator>,
}

impl<B> TestCheckpoints<B> for BackgroundSharedStorage {
    type Store = BackgroundCheckpoints<StorageCheckpoints<InMemoryStorage>, Simulator>;

    const DURABLE: bool = true;

    fn new(environment: Rc<Simulator>) -> Self {
        Self {
            storage: Rc::new(InMemoryStorage::new()),
            environment,
        }
    }

    fn for_server(&self, _bus: &B, _voter_factory: &VoterFactory) -> Self::Store {
        BackgroundCheckpoints::new(
            StorageCheckpoints::new(self.storage.clone()),
            self.environment.clone(),
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
            .with_speculative_decisions(true)
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
        let bus_fixture = BF::try_new_with_config(simulator, config)?;
        let (policy, policy_controller) =
            <PolicyProvider as TestPolicyProvider>::for_test(bus_fixture.get_env());
        let checkpoints = C::new(bus_fixture.get_env());
        Ok(Self {
            bus_fixture,
            policy,
            policy_controller,
            checkpoints,
        })
    }
}

impl<BF, C> CheckpointFixture for ReplayCommitServiceFixture<BF, C>
where
    BF: TailWindowFixture,
    C: TestCheckpoints<BF::Impl>,
{
    async fn checkpoint_position(&self, service: &Self::Impl, bus_id: &str) -> Result<i64> {
        let checkpoint = service.checkpoints().load(bus_id).await?;
        Ok(checkpoint.context("no checkpoint")?.position)
    }

    fn is_checkpoint_durable(&self) -> bool {
        C::DURABLE
    }

    fn tail_window_size(&self) -> u64 {
        self.bus_fixture.tail_window_size()
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
