/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixture whose servers are a random mix of commit-service implementations
//! over one bus and one policy register. Each `create_impl` picks one, so
//! scenarios that create several servers run them side by side.

use std::rc::Rc;

use agentbus_api::Environment as _;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::SimulatorFixture as BusSimulatorFixture;
use anyhow::Context as _;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;
use logact_commit_service_engine::CachingStorage;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::load_engine_state;
use logact_commit_service_engine_tests::fixtures::EnvStorageFixture;
use logact_commit_service_engine_tests::fixtures::InMemoryStorageFixture;
use logact_commit_service_replay::CheckpointStore;
use logact_commit_service_replay::ReplayCommitService;
use logact_commit_service_replay::StorageCheckpoints;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::SynchronousRegisterProvider;
use rand::RngExt as _;

use crate::fixtures::CheckpointFixture;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestCheckpoints;
use crate::fixtures::TestPolicyProvider;
use crate::fixtures::build_v1;
use crate::fixtures::replay::NoStore;
use crate::simulator::Simulator;
use crate::stores::LocalCheckpoints;
use crate::stores::NoStoreCheckpoints;

type PolicyProvider = SynchronousRegisterProvider<InMemoryStorage>;
type PolicyController = <PolicyProvider as TestPolicyProvider>::Controller;
type VoterFactory = DelegatingVoterFactory<agentbus_api::NoopLogger, agentbus_api::RealEnvironment>;

type EngineStorage = <InMemoryStorageFixture as ConformanceFixture>::Impl;
type V1Service<B> = CommitServiceV1<
    B,
    VoterFactory,
    CachingStorage<EngineStorage, Simulator>,
    PolicyProvider,
    DeciderFactoryImpl<EngineStorage>,
    Simulator,
>;
type ReplayService<B, C> = ReplayCommitService<B, VoterFactory, PolicyProvider, C>;

/// One server of a hybrid fixture.
pub enum HybridCommitSvc<B> {
    V1(V1Service<B>),
    NoStore(ReplayService<B, NoStoreCheckpoints<B>>),
    Local(ReplayService<B, LocalCheckpoints>),
    IndependentStorage(ReplayService<B, StorageCheckpoints<InMemoryStorage>>),
    /// Shares V1's engine storage, and so V1's engine-state records.
    Shared(ReplayService<B, StorageCheckpoints<EngineStorage>>),
}

/// Evaluate `$body` with `$service` bound to whichever service `$hybrid` holds.
macro_rules! on_service {
    ($hybrid:expr, $service:ident => $body:expr) => {
        match $hybrid {
            HybridCommitSvc::V1($service) => $body,
            HybridCommitSvc::NoStore($service) => $body,
            HybridCommitSvc::Local($service) => $body,
            HybridCommitSvc::IndependentStorage($service) => $body,
            HybridCommitSvc::Shared($service) => $body,
        }
    };
}

impl<B: Clone> Clone for HybridCommitSvc<B>
where
    V1Service<B>: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::V1(service) => Self::V1(service.clone()),
            Self::NoStore(service) => Self::NoStore(service.clone()),
            Self::Local(service) => Self::Local(service.clone()),
            Self::IndependentStorage(service) => Self::IndependentStorage(service.clone()),
            Self::Shared(service) => Self::Shared(service.clone()),
        }
    }
}

impl<B: agentbus_api::AgentBus + Clone + 'static> CommitSvc for HybridCommitSvc<B> {
    type Bus = B;

    fn agent_bus(&self) -> &B {
        on_service!(self, service => service.agent_bus())
    }

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        on_service!(self, service => service.create_bus(request).await)
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        on_service!(self, service => service.commit_intention(request).await)
    }
}

/// Servers are V1 services or replay services with any checkpoint store,
/// chosen at random. V1 and shared-storage replay servers share one engine
/// storage.
pub struct HybridCommitServiceFixture<BF> {
    bus_fixture: BF,
    policy: PolicyProvider,
    policy_controller: PolicyController,
    storage: Rc<EngineStorage>,
}

impl<BF> ConformanceFixture for HybridCommitServiceFixture<BF>
where
    BF: AgentBusTestFixture<Env = Simulator>,
    BF::Impl: Clone + 'static,
{
    type Env = Simulator;
    type Impl = HybridCommitSvc<BF::Impl>;

    fn get_env(&self) -> Rc<Simulator> {
        self.bus_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let environment = self.get_env();
        let bus = self.bus_fixture.create_impl();
        let voter_factory = VoterFactory::default();
        let policy = self.policy.clone();
        match environment.with_rng(|rng| rng.random_range(0..5)) {
            0 => HybridCommitSvc::V1(build_v1(
                bus,
                self.storage.clone(),
                |_| voter_factory,
                policy,
                environment,
            )),
            1 => {
                let checkpoints = NoStore.for_server(&bus, &voter_factory);
                HybridCommitSvc::NoStore(ReplayCommitService::new(
                    bus,
                    voter_factory,
                    policy,
                    checkpoints,
                ))
            }
            2 => HybridCommitSvc::Local(ReplayCommitService::new(
                bus,
                voter_factory,
                policy,
                LocalCheckpoints::default(),
            )),
            3 => HybridCommitSvc::IndependentStorage(ReplayCommitService::new(
                bus,
                voter_factory,
                policy,
                StorageCheckpoints::new(Rc::new(InMemoryStorage::new())),
            )),
            _ => HybridCommitSvc::Shared(ReplayCommitService::new(
                bus,
                voter_factory,
                policy,
                StorageCheckpoints::new(self.storage.clone()),
            )),
        }
    }
}

impl<BF> SimulatorFixture for HybridCommitServiceFixture<BF>
where
    BF: AgentBusTestFixture<Env = Simulator> + BusSimulatorFixture,
    BF::Impl: Clone + 'static,
{
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let bus_fixture = BF::try_new_with_config(simulator, config)?;
        let storage =
            InMemoryStorageFixture::try_new_on_env(bus_fixture.get_env(), config)?.create_impl();
        let (policy, policy_controller) = PolicyProvider::for_test();
        Ok(Self {
            bus_fixture,
            policy,
            policy_controller,
            storage: Rc::new(storage),
        })
    }
}

impl<BF> CheckpointFixture for HybridCommitServiceFixture<BF>
where
    BF: AgentBusTestFixture<Env = Simulator>,
    BF::Impl: Clone + 'static,
{
    async fn checkpoint_position(&self, service: &Self::Impl, bus_id: &str) -> Result<i64> {
        match service {
            HybridCommitSvc::V1(_) => load_engine_state(&*self.storage, bus_id)
                .await?
                .1
                .context("no engine state"),
            HybridCommitSvc::NoStore(service) => {
                replay_position(service.checkpoints(), bus_id).await
            }
            HybridCommitSvc::Local(service) => replay_position(service.checkpoints(), bus_id).await,
            HybridCommitSvc::IndependentStorage(service) => {
                replay_position(service.checkpoints(), bus_id).await
            }
            HybridCommitSvc::Shared(service) => {
                replay_position(service.checkpoints(), bus_id).await
            }
        }
    }

    /// A fresh server may have a store that isn't durable.
    fn is_checkpoint_durable(&self) -> bool {
        false
    }
}

impl<BF> DeciderPolicyFixture for HybridCommitServiceFixture<BF>
where
    BF: AgentBusTestFixture<Env = Simulator>,
    BF::Impl: Clone + 'static,
{
    type Controller = PolicyController;

    fn policy_controller(&self) -> Self::Controller {
        self.policy_controller.clone()
    }

    /// V1 and the shared replay servers share one engine-state record, which
    /// only works for deciders without state.
    fn supports_first_boolean_wins(&self) -> bool {
        false
    }
}

async fn replay_position(checkpoints: &impl CheckpointStore, bus_id: &str) -> Result<i64> {
    Ok(checkpoints
        .load(bus_id)
        .await?
        .context("no checkpoint")?
        .position)
}
