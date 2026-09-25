/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A test-only `CommitSvc` that fans requests out to one of N underlying
//! instances at random, plus a fixture that runs the generic suite against it.
//!
//! Each `create_impl` fans `NUM_SERVICES` engine front-ends onto one shared bus
//! (from the injected `AgentBusTestFixture`, whose `create_impl` hands out
//! handles to a single shared log), one shared in-memory storage, and one shared
//! decider-policy register, wrapped in a router that dispatches each request to a
//! randomly picked one. Because the engines share the bus (one total order), the
//! storage (one cursor / decider state, advanced under CAS), and the policy
//! register, they behave as one logical service with several front-ends — so
//! routing requests across them stays linearizable.

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::AgentBus;
use agentbus_api::Environment;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::SimulatorFixture as BusSimulatorFixture;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::SynchronousRegisterProvider;
use rand::RngExt as _;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::SettableDeciderPolicy;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::TestPolicyProvider;
use crate::simulator::Simulator;

/// Routes each commit request to one of `services`, picked uniformly at random.
/// Randomness comes from the `Environment` RNG so simulator tests stay
/// deterministic.
pub struct RandomRoutingCommitService<S, E> {
    services: Vec<S>,
    env: Rc<E>,
}

impl<S: Clone, E> Clone for RandomRoutingCommitService<S, E> {
    fn clone(&self) -> Self {
        Self {
            services: self.services.clone(),
            env: self.env.clone(),
        }
    }
}

impl<S: CommitSvc, E: Environment> RandomRoutingCommitService<S, E> {
    /// Build a router over `services`. Panics if `services` is empty, since there
    /// would be nowhere to route requests.
    pub fn new(services: Vec<S>, env: Rc<E>) -> Self {
        assert!(
            !services.is_empty(),
            "RandomRoutingCommitService needs at least one service"
        );
        Self { services, env }
    }

    fn pick(&self) -> &S {
        let index = self
            .env
            .with_rng(|rng| rng.random_range(0..self.services.len()));
        &self.services[index]
    }
}

impl<S: CommitSvc, E: Environment> CommitSvc for RandomRoutingCommitService<S, E> {
    type Bus = S::Bus;

    fn agent_bus(&self) -> &Self::Bus {
        // `new` rejects an empty service list, and every routed service is
        // constructed over the same logical bus.
        self.services[0].agent_bus()
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.pick().commit_intention(request).await
    }
}

/// Number of underlying instances to route across.
const NUM_SERVICES: usize = 3;

type RoutedV1<T, P> =
    CommitServiceV1<T, DelegatingVoterFactory, InMemoryStorage, P, DeciderFactoryImpl, Simulator>;

/// Runs the generic suite against a router over `NUM_SERVICES` v1 services, with
/// the bus from the injected `AgentBusTestFixture`, a shared in-memory storage, and
/// one shared policy provider. Generic over the provider `P`; the variant list pins
/// it to the default `SynchronousRegisterProvider`, while `#[scenario_for]` tests
/// can pin a different provider.
pub struct RandomRoutingCommitServiceFixture<BF, P = SynchronousRegisterProvider<InMemoryStorage>>
where
    P: TestPolicyProvider,
{
    bus_fixture: BF,
    policy: P,
    policy_controller: P::Controller,
}

/// Build a v1 engine over a `bus` front-end, the shared `storage`, and the shared
/// `policy` provider. Each engine gets a fresh bus handle (onto the same shared
/// log) and a clone of the one `Rc` storage handle and the one provider, so they
/// all read and write the same log, engine state, and policy.
fn make_v1<T: AgentBus + Clone + 'static, P: TestPolicyProvider>(
    bus: T,
    storage: Rc<InMemoryStorage>,
    policy: P,
    environment: Rc<Simulator>,
) -> RoutedV1<T, P> {
    CommitServiceV1::new(bus, |bus| {
        BaseEngine::new(
            bus,
            storage.clone(),
            DelegatingVoterFactory::with_storage(storage.clone()),
            policy,
            DeciderFactoryImpl::new(storage),
            environment,
        )
    })
}

impl<BF, P> ConformanceFixture for RandomRoutingCommitServiceFixture<BF, P>
where
    BF: AgentBusTestFixture<Env = Simulator>,
    BF::Impl: Clone + 'static,
    P: TestPolicyProvider,
{
    type Env = Simulator;
    type Impl = RandomRoutingCommitService<RoutedV1<BF::Impl, P>, Simulator>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.bus_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let env = self.bus_fixture.get_env();
        // One shared storage, cloned into every engine.
        let storage = Rc::new(InMemoryStorage::new());
        // Each `bus_fixture.create_impl()` is a fresh front-end onto the same
        // shared log, so the N engines behave as one logical service.
        let services = (0..NUM_SERVICES)
            .map(|_| {
                make_v1(
                    self.bus_fixture.create_impl(),
                    storage.clone(),
                    self.policy.clone(),
                    env.clone(),
                )
            })
            .collect();
        RandomRoutingCommitService::new(services, env)
    }
}

impl<BF, P> SimulatorFixture for RandomRoutingCommitServiceFixture<BF, P>
where
    BF: AgentBusTestFixture<Env = Simulator> + BusSimulatorFixture,
    P: TestPolicyProvider,
{
    fn new(simulator: Simulator) -> Self {
        let (policy, policy_controller) = P::for_test();
        Self {
            bus_fixture: <BF as BusSimulatorFixture>::new(simulator),
            policy,
            policy_controller,
        }
    }
}

impl<BF, P> DeciderPolicyFixture for RandomRoutingCommitServiceFixture<BF, P>
where
    BF: AgentBusTestFixture<Env = Simulator>,
    BF::Impl: Clone,
    P: TestPolicyProvider,
    P::Controller: SettableDeciderPolicy,
{
    /// Sets the one register every routed engine reads, so all of them switch.
    async fn set_decider_policy(&self, policy: DeciderPolicy) -> anyhow::Result<()> {
        self.policy_controller.set_decider_policy(policy).await
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use agent_bus_proto_rust::agent_bus::intention;
    use agentbus_api::RealEnvironment;
    use agentbus_simple::InMemoryAgentBus;
    use futures::executor::block_on;

    use super::*;

    fn request() -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: "agent-1".to_string(),
            },
            intention: intention::Intention::StringIntention("test".to_string()),
        }
    }

    /// A `CommitSvc` that just counts the requests it receives.
    struct CountingService {
        hits: Rc<Cell<u32>>,
        bus: InMemoryAgentBus<RealEnvironment>,
    }

    impl CommitSvc for CountingService {
        type Bus = InMemoryAgentBus<RealEnvironment>;

        fn agent_bus(&self) -> &Self::Bus {
            &self.bus
        }

        async fn commit_intention(
            &self,
            _request: CommitIntentionCommand,
        ) -> CommitResult<CommitIntentionOutcome> {
            self.hits.set(self.hits.get() + 1);
            Ok(CommitIntentionOutcome {
                approved: true,
                reason: String::new(),
                log_position: 0,
            })
        }
    }

    #[test]
    fn single_service_receives_every_request() {
        let env = Rc::new(RealEnvironment::new());
        let hits = Rc::new(Cell::new(0));
        let service = CountingService {
            hits: hits.clone(),
            bus: InMemoryAgentBus::new(env.clone()),
        };
        let router = RandomRoutingCommitService::new(vec![service], env);

        for _ in 0..5 {
            block_on(router.commit_intention(request())).unwrap();
        }
        assert_eq!(hits.get(), 5);
    }

    #[test]
    fn requests_are_delegated_and_spread_across_services() {
        let env = Rc::new(RealEnvironment::new());
        let hits: Vec<Rc<Cell<u32>>> = (0..3).map(|_| Rc::new(Cell::new(0))).collect();
        let bus = InMemoryAgentBus::new(env.clone());
        let services = hits
            .iter()
            .map(|hits| CountingService {
                hits: hits.clone(),
                bus: bus.clone(),
            })
            .collect();
        let router = RandomRoutingCommitService::new(services, env);

        for _ in 0..300 {
            block_on(router.commit_intention(request())).unwrap();
        }

        let total: u32 = hits.iter().map(|h| h.get()).sum();
        assert_eq!(total, 300, "every request must be delegated exactly once");
        let used = hits.iter().filter(|h| h.get() > 0).count();
        assert!(
            used >= 2,
            "300 requests should reach more than one of 3 services, got {used}"
        );
    }
}
