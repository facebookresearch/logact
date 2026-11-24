/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixtures for LogAct commit service tests — one `CommitSvc` builder per file.
//!
//! Each fixture is generic over an AgentBus fixture from `agentbus_tests`, so the
//! suite composes with the full AgentBus backend matrix (see `variants.rs`). The
//! environment-agnostic traits live in the shared `conformance` crate.

use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::AgentBus;
use agentbus_api::AgentbusLogger;
use agentbus_api::Environment;
use agentbus_api::InMemoryLogger;
use agentbus_api::InMemoryMetrics;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::CachingStorageConfig;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::Observability;
use logact_commit_service_engine::PolicyProvider;
use logact_commit_service_engine::PolicyState;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::VoterFactory;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_policy_register::ObservablePolicyRegister;
use logact_commit_service_policy_register::PolicyRegister;
use logact_commit_service_policy_register::RegisterPolicyProvider;
use logact_commit_service_policy_register::StoragePolicyRegister;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::StaticConfigPolicyProvider;
use logact_commit_service_v1::SynchronousRegisterProvider;

use crate::simulator::Simulator;

pub mod buffering;
pub mod buggy_stale_position;
pub mod channeled;
pub mod channeled_svc;
pub mod grpc;
pub mod hybrid;
pub mod inmem;
pub mod lease;
pub mod observable;
pub mod production;
pub mod random_routing;
pub mod replay;
pub mod v1;
pub mod v1_latency;
pub mod v1_with_counting_voter;

pub use buffering::BufferingCommitServiceFixture;
pub use buggy_stale_position::BuggyStalePositionCommitServiceFixture;
pub use channeled::ChanneledCommitServiceFixture;
pub use channeled_svc::ChanneledSvcCommitServiceFixture;
pub use conformance::ConformanceFixture;
pub use conformance::SimulatorFixture;
pub use grpc::GrpcCommitServiceFixture;
pub use hybrid::HybridCommitServiceFixture;
pub use inmem::InMemCommitServiceFixture;
pub use lease::LeaseBasedTestPolicyProvider;
pub use observable::ObservableCommitServiceFixture;
pub use production::CommitServiceProductionShape;
pub use random_routing::CommitServiceV1ProductionShape;
pub use random_routing::RandomRoutingCommitServiceFixture;
pub use random_routing::RandomRoutingFixture;
pub use replay::ReplayCommitServiceFixture;
pub use replay::TestCheckpoints;
pub use v1::CommitServiceV1Fixture;
pub use v1::CommitServiceV1StorageConflictFixture;
pub use v1_latency::CommitServiceV1LatencyFixture;
pub use v1_with_counting_voter::CommitServiceV1CountingVoterFixture;

/// The engine checkpoint cache production servers default to: each server's
/// own cache over the shared engine storage.
pub const ENGINE_CACHE: CachingStorageConfig = CachingStorageConfig {
    ttl: Duration::from_secs(60),
    max_entries: 1024,
    max_value_bytes: 1024 * 1024,
};

/// Build a `CommitServiceV1` over `bus` and `storage` with the engine's default
/// decider and uncached engine storage, leaving only the voter factory and policy
/// to vary.
pub fn build_v1<B, VF, S, P, E>(
    bus: B,
    storage: Rc<S>,
    make_voter_factory: impl FnOnce(Rc<S>) -> VF,
    policy: P,
    environment: Rc<E>,
) -> CommitServiceV1<B, VF, S, P, DeciderFactoryImpl<S>, E>
where
    B: AgentBus + Clone + 'static,
    VF: VoterFactory + 'static,
    S: Storage + 'static,
    P: TestPolicyProvider<E>,
    E: Environment + 'static,
{
    let voter_factory = make_voter_factory(storage.clone());
    let decider_factory = DeciderFactoryImpl::new(storage.clone());
    CommitServiceV1::new(bus, |bus| {
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

/// The `CommitSvc`-specific view of a fixture: a `ConformanceFixture` whose `Impl`
/// is a `CommitSvc`. Scenarios bind on this so they can call `CommitSvc` methods.
/// The blanket impl means every `ConformanceFixture<Impl: CommitSvc>` qualifies —
/// no fixture implements it directly.
pub trait CommitServiceTestFixture: ConformanceFixture<Impl: CommitSvc> {}
impl<F: ConformanceFixture<Impl: CommitSvc>> CommitServiceTestFixture for F {}

/// A fixture with a controllable, register-backed policy. A completed policy
/// update must be visible to subsequent commits, including through reader caches.
pub trait DeciderPolicyFixture: CommitServiceTestFixture {
    type Controller: SettableDeciderPolicy;

    fn policy_controller(&self) -> Self::Controller;

    fn set_decider_policy(
        &self,
        policy: DeciderPolicy,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> {
        async move {
            self.policy_controller().set_decider_policy(policy).await?;
            Ok(())
        }
    }

    /// Whether its servers can share a bus under `FIRST_BOOLEAN_WINS`. A fleet
    /// mixing V1 with replay servers on V1's engine-state record can't: each
    /// keeps the pending intention where the other doesn't read it.
    fn supports_first_boolean_wins(&self) -> bool {
        true
    }
}

/// A fixture that can observe where its services' checkpoints end.
pub trait CheckpointFixture: CommitServiceTestFixture {
    /// The last position covered by `service`'s checkpoint for `bus_id`.
    fn checkpoint_position(
        &self,
        service: &Self::Impl,
        bus_id: &str,
    ) -> impl std::future::Future<Output = anyhow::Result<i64>>;

    /// Whether checkpoints are kept independently of the log, so the entries
    /// they cover can expire.
    fn is_checkpoint_durable(&self) -> bool;

    /// How far back its services' buses look for the tail, as
    /// `TailWindowFixture::tail_window_size`.
    fn tail_window_size(&self) -> u64 {
        1
    }
}

/// A `PolicyProvider` and its test-only controller, constructed together.
pub trait TestPolicyProvider<E: Environment = Simulator>:
    PolicyProvider<Error: Into<anyhow::Error>> + Clone + 'static
{
    type Controller: Clone + 'static;

    fn for_test(environment: Rc<E>) -> (Self, Self::Controller);
}

/// Bootstrap `register` with `OnByDefault` and pair it, as the controller, with a
/// provider that validates voter configs.
pub fn register_provider_for_test<R: PolicyRegister + Clone>(
    register: R,
) -> (RegisterPolicyProvider<R>, R) {
    futures::executor::block_on(register.set_policy(
        &PolicyState {
            decider_policy: Some(DeciderPolicy::OnByDefault as i32),
            ..Default::default()
        },
        None,
    ))
    .expect("test policy register should initialize");
    let voter_factory = DelegatingVoterFactory::default();
    (
        RegisterPolicyProvider::new(register.clone(), move |policy| {
            validate_voter_configs(&voter_factory, policy)
        }),
        register,
    )
}

impl<E: Environment> TestPolicyProvider<E> for SynchronousRegisterProvider<InMemoryStorage> {
    type Controller = StoragePolicyRegister<InMemoryStorage>;

    fn for_test(_environment: Rc<E>) -> (Self, Self::Controller) {
        register_provider_for_test(StoragePolicyRegister::new(
            Rc::new(InMemoryStorage::new()),
            "test-policy",
        ))
    }
}

/// Register-backed policy observability shared by production-shape replicas and controllers.
pub type ObservableTestPolicyProvider = RegisterPolicyProvider<
    ObservablePolicyRegister<StoragePolicyRegister<InMemoryStorage>, InMemoryLogger, Simulator>,
>;

impl TestPolicyProvider for ObservableTestPolicyProvider {
    type Controller =
        ObservablePolicyRegister<StoragePolicyRegister<InMemoryStorage>, InMemoryLogger, Simulator>;

    fn for_test(environment: Rc<Simulator>) -> (Self, Self::Controller) {
        register_provider_for_test(ObservablePolicyRegister::new(
            StoragePolicyRegister::new(Rc::new(InMemoryStorage::new()), "test-policy"),
            Observability {
                metrics: Rc::new(InMemoryMetrics::new()),
                logger: Rc::new(InMemoryLogger::new()),
                environment,
            },
        ))
    }
}

impl<E: Environment> TestPolicyProvider<E> for StaticConfigPolicyProvider {
    type Controller = ();

    /// Fixed at construction to deny — `OFF_BY_DEFAULT` with no voters — so the
    /// construction-time policy is observable as a denied `commit_intention`.
    fn for_test(_environment: Rc<E>) -> (Self, Self::Controller) {
        (
            StaticConfigPolicyProvider::new(DeciderPolicy::OffByDefault, Vec::new(), |_| Ok(())),
            (),
        )
    }
}

/// A test controller that can update the desired decider policy.
pub trait SettableDeciderPolicy {
    fn set_decider_policy(
        &self,
        policy: DeciderPolicy,
    ) -> impl std::future::Future<Output = anyhow::Result<i64>>;

    fn read_decider_policy(
        &self,
    ) -> impl std::future::Future<Output = anyhow::Result<(DeciderPolicy, i64)>>;
}

async fn set_register_decider_policy<R: PolicyRegister>(
    register: &R,
    policy: DeciderPolicy,
) -> anyhow::Result<i64> {
    let mut current = register.read_validated().await?;
    current.state.decider_policy = Some(policy as i32);
    Ok(register
        .set_policy(&current.state, Some(current.version))
        .await?)
}

async fn read_register_decider_policy<R: PolicyRegister>(
    register: &R,
) -> anyhow::Result<(DeciderPolicy, i64)> {
    let versioned = register.read_validated().await?;
    let raw = versioned
        .state
        .decider_policy
        .expect("PolicyRegister validation requires a decider policy");
    Ok((DeciderPolicy::try_from(raw)?, versioned.version))
}

impl<S: Storage> SettableDeciderPolicy for StoragePolicyRegister<S> {
    async fn set_decider_policy(&self, policy: DeciderPolicy) -> anyhow::Result<i64> {
        set_register_decider_policy(self, policy).await
    }

    async fn read_decider_policy(&self) -> anyhow::Result<(DeciderPolicy, i64)> {
        read_register_decider_policy(self).await
    }
}

impl<S: Storage, L: AgentbusLogger, E: Environment> SettableDeciderPolicy
    for ObservablePolicyRegister<StoragePolicyRegister<S>, L, E>
{
    async fn set_decider_policy(&self, policy: DeciderPolicy) -> anyhow::Result<i64> {
        set_register_decider_policy(self, policy).await
    }

    async fn read_decider_policy(&self) -> anyhow::Result<(DeciderPolicy, i64)> {
        read_register_decider_policy(self).await
    }
}
