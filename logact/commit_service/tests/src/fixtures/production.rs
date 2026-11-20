/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The commit service as production servers build it, with in-memory stand-ins
//! for ZippyDB. Like the server, each replica reads the environment's feature
//! flags to pick V1 or replay, buffering, background checkpoints and speculative
//! decisions, and runs behind `ChanneledCommitService`.

use std::rc::Rc;

use agentbus_api::Environment;
use agentbus_api::FeatureFlags;
use agentbus_api::NoopLogger;
use agentbus_api::RealEnvironment;
use agentbus_tests::BusProductionShape;
use anyhow::Context as _;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_core::BufferingCommitService;
use logact_commit_service_core::ChanneledCommitService;
use logact_commit_service_engine::CachingStorage;
use logact_commit_service_engine::load_engine_state;
use logact_commit_service_engine_tests::fixtures::EnvStorageFixture;
use logact_commit_service_engine_tests::fixtures::ScopedStorageFixture;
use logact_commit_service_replay::BackgroundCheckpoints;
use logact_commit_service_replay::CheckpointStore as _;
use logact_commit_service_replay::ReplayCommitService;
use logact_commit_service_replay::StorageCheckpoints;
use logact_commit_service_v1::DelegatingVoterFactory;

use crate::fixtures::CheckpointFixture;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::ENGINE_CACHE;
use crate::fixtures::ObservableTestPolicyProvider;
use crate::fixtures::RandomRoutingFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::fixtures::build_v1;
use crate::simulator::Simulator;

type Bus = <BusProductionShape as ConformanceFixture>::Impl;
type EngineStorage = <ScopedStorageFixture as ConformanceFixture>::Impl;
type PolicyProvider = ObservableTestPolicyProvider;
type PolicyController = <PolicyProvider as TestPolicyProvider>::Controller;
type VoterFactory = DelegatingVoterFactory<NoopLogger, RealEnvironment>;

/// Requests routed across replicas, as production routes them.
pub type CommitServiceProductionShape = RandomRoutingFixture<FeatureFlagProductionFixture>;

/// Each `create_impl` is one production server: a [`BusProductionShape`] bus
/// stack, its own engine cache over the one shared scoped engine storage, and
/// the service the simulator's feature flags select.
pub struct FeatureFlagProductionFixture {
    bus_fixture: BusProductionShape,
    storage: Rc<EngineStorage>,
    policy: PolicyProvider,
    policy_controller: PolicyController,
}

/// Runs the service `make_service` builds behind a channel, serializing commit
/// intentions per bus if `buffering` is set.
fn channeled<S: CommitSvc + 'static>(
    env: Rc<Simulator>,
    bus: Bus,
    buffering: bool,
    make_service: impl FnOnce(Bus) -> S + 'static,
) -> ChanneledCommitService<Bus> {
    if buffering {
        ChanneledCommitService::new_on_environment(env, bus, move |bus| {
            BufferingCommitService::new(make_service(bus))
        })
    } else {
        ChanneledCommitService::new_on_environment(env, bus, make_service)
    }
}

impl ConformanceFixture for FeatureFlagProductionFixture {
    type Env = Simulator;
    type Impl = ChanneledCommitService<Bus>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.bus_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let env = self.get_env();
        let FeatureFlags {
            enable_replay_commit_service,
            enable_background_replay_checkpoints,
            enable_speculative_decisions: speculative,
            enable_commit_buffering: buffering,
            disable_direct_agentbus_appends: _,
            enable_leased_policy_reads: _,
        } = env.feature_flags();
        let bus = self.bus_fixture.create_impl();
        let storage = self.storage.clone();
        let policy = self.policy.clone();
        if !enable_replay_commit_service {
            let service_env = env.clone();
            return channeled(env, bus, buffering, move |bus| {
                build_v1(
                    bus,
                    storage,
                    |_| VoterFactory::default(),
                    policy,
                    service_env,
                )
            });
        }
        let checkpoints = StorageCheckpoints::new(Rc::new(CachingStorage::new(
            storage,
            env.clone(),
            ENGINE_CACHE,
        )));
        if enable_background_replay_checkpoints {
            let checkpoints = BackgroundCheckpoints::new(checkpoints, env.clone());
            return channeled(env, bus, buffering, move |bus| {
                ReplayCommitService::new(bus, VoterFactory::default(), policy, checkpoints)
                    .with_speculative_decisions(speculative)
            });
        }
        channeled(env, bus, buffering, move |bus| {
            ReplayCommitService::new(bus, VoterFactory::default(), policy, checkpoints)
                .with_speculative_decisions(speculative)
        })
    }
}

impl SimulatorFixture for FeatureFlagProductionFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let bus_fixture = BusProductionShape::try_new_with_config(simulator, config)?;
        let storage = ScopedStorageFixture::try_new_on_env(bus_fixture.get_env(), config)?;
        let (policy, policy_controller) = PolicyProvider::for_test(bus_fixture.get_env());
        Ok(Self {
            bus_fixture,
            storage: Rc::new(storage.create_impl()),
            policy,
            policy_controller,
        })
    }
}

impl CheckpointFixture for FeatureFlagProductionFixture {
    /// The checkpoint in the shared engine storage: V1's engine-state cursor,
    /// or the replay service's checkpoint record.
    async fn checkpoint_position(&self, _service: &Self::Impl, bus_id: &str) -> Result<i64> {
        if !self.get_env().feature_flags().enable_replay_commit_service {
            return load_engine_state(&*self.storage, bus_id)
                .await?
                .1
                .context("no engine state");
        }
        let checkpoint = StorageCheckpoints::new(self.storage.clone())
            .load(bus_id)
            .await?;
        Ok(checkpoint.context("no checkpoint")?.position)
    }

    fn is_checkpoint_durable(&self) -> bool {
        true
    }
}

impl DeciderPolicyFixture for FeatureFlagProductionFixture {
    type Controller = PolicyController;

    fn policy_controller(&self) -> Self::Controller {
        self.policy_controller.clone()
    }
}
