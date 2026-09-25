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

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::AgentBus;
use agentbus_api::Environment;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::PolicyProvider;
use logact_commit_service_engine::PolicyRegister;
use logact_commit_service_engine::PolicyState;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::VoterFactory;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::StaticConfigPolicyProvider;
use logact_commit_service_v1::SynchronousRegisterProvider;

pub mod bus_id_encoding;
pub mod channeled;
pub mod grpc;
mod grpc_bus_id_encoding;
pub mod inmem;
pub mod random_routing;
pub mod v1;
pub mod v1_latency;
pub mod v1_with_counting_voter;

pub use bus_id_encoding::BusIdEncoding;
pub use bus_id_encoding::BusIdEncodingFixture;
pub use bus_id_encoding::BusIdEncodingFixtureFactory;
pub use bus_id_encoding::LegacyBusIdEncoding;
pub use bus_id_encoding::TypedBusIdEncoding;
pub use channeled::ChanneledCommitServiceFixture;
pub use conformance::ConformanceFixture;
pub use conformance::SimulatorFixture;
pub use grpc::GrpcCommitServiceFixture;
pub use inmem::InMemCommitServiceFixture;
pub use random_routing::RandomRoutingCommitServiceFixture;
pub use v1::CommitServiceV1Fixture;
pub use v1::CommitServiceV1StorageConflictFixture;
pub use v1_latency::CommitServiceV1LatencyFixture;
pub use v1_with_counting_voter::CommitServiceV1CountingVoterFixture;

pub type LegacyBusIdFixture<F> = BusIdEncodingFixture<F, LegacyBusIdEncoding>;
pub type TypedBusIdFixture<F> = BusIdEncodingFixture<F, TypedBusIdEncoding>;

/// Build a `CommitServiceV1` over `bus` and `storage` with the engine's default
/// decider, leaving only the voter factory and policy to vary.
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
    P: TestPolicyProvider,
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

/// A fixture whose commit service sources its decider policy from a register the
/// test controls (a `SynchronousRegisterProvider`). `set_decider_policy` installs a
/// policy service-wide; because the engine reads the register on the critical path
/// of every `propose_intention`, the next `commit_intention` reflects it — so the
/// switch is linearizable against probes. Only fixtures wired with a register
/// provider implement it, so it is a bespoke subtrait rather than a generic
/// scenario bound.
pub trait DeciderPolicyFixture: CommitServiceTestFixture {
    fn set_decider_policy(
        &self,
        policy: DeciderPolicy,
    ) -> impl std::future::Future<Output = anyhow::Result<()>>;
}

/// A `PolicyProvider` and its test-only controller, constructed together.
pub trait TestPolicyProvider: PolicyProvider<Error: Into<anyhow::Error>> + Clone + 'static {
    type Controller: Clone + 'static;

    fn for_test() -> (Self, Self::Controller);
}

impl TestPolicyProvider for SynchronousRegisterProvider<InMemoryStorage> {
    type Controller = PolicyRegister<InMemoryStorage>;

    fn for_test() -> (Self, Self::Controller) {
        let voter_factory = DelegatingVoterFactory::default();
        let register = PolicyRegister::new(Rc::new(InMemoryStorage::new()), "test-policy");
        futures::executor::block_on(register.set_policy(
            &PolicyState {
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
            None,
        ))
        .expect("test policy register should initialize");
        (
            SynchronousRegisterProvider::new(register.clone(), move |policy| {
                validate_voter_configs(&voter_factory, policy)
            }),
            register,
        )
    }
}

impl TestPolicyProvider for StaticConfigPolicyProvider {
    type Controller = ();

    /// Fixed at construction to deny — `OFF_BY_DEFAULT` with no voters — so the
    /// construction-time policy is observable as a denied `commit_intention`.
    fn for_test() -> (Self, Self::Controller) {
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
    ) -> impl std::future::Future<Output = anyhow::Result<()>>;
}

impl<S: Storage> SettableDeciderPolicy for PolicyRegister<S> {
    async fn set_decider_policy(&self, policy: DeciderPolicy) -> anyhow::Result<()> {
        let mut current = self.read_validated().await?;
        current.state.decider_policy = Some(policy as i32);
        self.set_policy(&current.state, Some(current.version))
            .await?;
        Ok(())
    }
}
