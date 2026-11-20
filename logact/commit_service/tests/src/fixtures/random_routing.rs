/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A test-only `CommitSvc` that routes requests to each bus's home replica,
//! with optional random misrouting, plus a fixture for the generic suite.
//!
//! The fixture builds `NUM_SERVICES` replicas once, each one `create_impl` of a
//! commit-service fixture. Every `create_impl` is a router over those same
//! replicas, routing each request by its bus ID. For V1 replicas, which share
//! the log (one total order), the storage (one cursor / decider state, advanced
//! under CAS), and the policy register, they behave as one logical service with
//! several front-ends, shared by every client, so routing requests across them
//! stays linearizable.

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::AppendRequest;
use agentbus_api::AppendResponse;
use agentbus_api::BlockingPollRequest;
use agentbus_api::BlockingPollResponse;
use agentbus_api::BusResult;
use agentbus_api::CheckTailRequest;
use agentbus_api::CheckTailResponse;
use agentbus_api::Environment;
use agentbus_api::PollRequest;
use agentbus_api::PollResponse;
use agentbus_api::ReadNextRequest;
use agentbus_api::ReadNextResponse;
use agentbus_api::validation::validate_bus_id;
use anyhow::Result;
use conformance::TestConfig;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine_tests::fixtures::InMemoryStorageFixture;
use logact_commit_service_engine_tests::fixtures::ScopedStorageFixture;
use logact_commit_service_v1::SynchronousRegisterProvider;
use rand::RngExt as _;

use crate::fixtures::CheckpointFixture;
use crate::fixtures::CommitServiceV1Fixture;
use crate::fixtures::ConformanceFixture;
use crate::fixtures::DeciderPolicyFixture;
use crate::fixtures::ObservableTestPolicyProvider;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

/// Routes each commit request by its bus ID, as production does: to the bus's
/// home service, picked by a salted hash of the ID, so every clone agrees on it.
/// With probability `switch_probability`, a request strays to a service picked
/// uniformly at random instead. Randomness, including the salt, comes from the
/// `Environment` RNG so simulator tests stay deterministic.
pub struct RandomRoutingCommitService<S, E> {
    bus: RandomRoutingAgentBus<S, E>,
}

/// Routes each AgentBus request using the same bus-ID policy as commit requests.
pub struct RandomRoutingAgentBus<S, E> {
    services: Vec<S>,
    env: Rc<E>,
    switch_probability: f64,
    salt: u64,
}

impl<S: Clone, E> Clone for RandomRoutingAgentBus<S, E> {
    fn clone(&self) -> Self {
        Self {
            services: self.services.clone(),
            env: self.env.clone(),
            switch_probability: self.switch_probability,
            salt: self.salt,
        }
    }
}

impl<S: Clone, E> Clone for RandomRoutingCommitService<S, E> {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
        }
    }
}

impl<S: CommitSvc, E: Environment> RandomRoutingCommitService<S, E> {
    /// Build a router that picks a service uniformly at random for every request.
    /// Panics if `services` is empty, since there would be nowhere to route requests.
    pub fn new(services: Vec<S>, env: Rc<E>) -> Self {
        assert!(
            !services.is_empty(),
            "RandomRoutingCommitService needs at least one service"
        );
        Self {
            bus: RandomRoutingAgentBus {
                services,
                env,
                switch_probability: 1.0,
                salt: 0,
            },
        }
    }

    /// Send a request to a random service only with `switch_probability`, so each
    /// bus's requests mostly go to its home service.
    pub fn with_switch_probability(mut self, switch_probability: f64) -> Self {
        self.bus.switch_probability = switch_probability;
        self.bus.salt = self.bus.env.with_rng(|rng| rng.random());
        self
    }
}

impl<S, E: Environment> RandomRoutingAgentBus<S, E> {
    fn pick(&self, bus_id: &str) -> &S {
        let index = if self.switch_probability < 1.0
            && !self
                .env
                .with_rng(|rng| rng.random_bool(self.switch_probability))
        {
            let mut hasher = DefaultHasher::new();
            (self.salt, bus_id).hash(&mut hasher);
            (hasher.finish() % self.services.len() as u64) as usize
        } else {
            self.env
                .with_rng(|rng| rng.random_range(0..self.services.len()))
        };
        &self.services[index]
    }
}

impl<S: CommitSvc, E: Environment> AgentBus for RandomRoutingAgentBus<S, E> {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        self.pick(&bus_id.agent_bus_id)
            .agent_bus()
            .append(request)
            .await
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        self.pick(&bus_id.agent_bus_id)
            .agent_bus()
            .poll(request)
            .await
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        self.pick(&bus_id.agent_bus_id)
            .agent_bus()
            .read_next(request)
            .await
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        self.pick(&bus_id.agent_bus_id)
            .agent_bus()
            .check_tail(request)
            .await
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        self.pick(&bus_id.agent_bus_id)
            .agent_bus()
            .blocking_poll(request)
            .await
    }
}

impl<S: CommitSvc, E: Environment> CommitSvc for RandomRoutingCommitService<S, E> {
    type Bus = RandomRoutingAgentBus<S, E>;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.bus.pick(&request.bus_id).create_bus(request).await
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.bus
            .pick(&request.bus_id.agent_bus_id)
            .commit_intention(request)
            .await
    }
}

/// Number of replicas to route across.
const NUM_SERVICES: usize = 3;

/// Runs the generic suite against a router over `NUM_SERVICES` replicas, each
/// one `create_impl` of the commit-service fixture `CF`. `CF`'s replicas share
/// its durable state, so they behave as one logical service.
pub struct RandomRoutingFixture<CF: ConformanceFixture> {
    inner: CF,
    router: RandomRoutingCommitService<CF::Impl, Simulator>,
}

/// A router over V1 replicas with the bus from the injected
/// `AgentBusTestFixture`, one storage shared by every replica from the storage
/// fixture `SF`, and one shared policy provider. Generic over the provider `P`;
/// the variant list pins it to the default `SynchronousRegisterProvider`, while
/// `#[scenario_for]` tests can pin a different provider.
pub type RandomRoutingCommitServiceFixture<
    BF,
    P = SynchronousRegisterProvider<InMemoryStorage>,
    SF = InMemoryStorageFixture,
> = RandomRoutingFixture<CommitServiceV1Fixture<BF, P, SF>>;

/// The commit service as production deploys it, with in-memory stand-ins for
/// ZippyDB: requests routed across replicas, each with its own
/// [`agentbus_tests::BusProductionShape`] stack over the one shared space, and
/// shared scoped engine storage.
pub type CommitServiceV1ProductionShape = RandomRoutingCommitServiceFixture<
    agentbus_tests::BusProductionShape,
    ObservableTestPolicyProvider,
    ScopedStorageFixture,
>;

impl<CF> ConformanceFixture for RandomRoutingFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator, Impl: CommitSvc + Clone>,
{
    type Env = Simulator;
    type Impl = RandomRoutingCommitService<CF::Impl, Simulator>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    /// A router over the shared replicas. Every clone shares the salt, so it
    /// routes each bus to the same home replica.
    fn create_impl(&self) -> Self::Impl {
        self.router.clone()
    }
}

impl<CF> SimulatorFixture for RandomRoutingFixture<CF>
where
    CF: ConformanceFixture<Env = Simulator, Impl: CommitSvc + Clone> + SimulatorFixture,
{
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let inner = CF::try_new_with_config(simulator, config)?;
        let replicas = (0..NUM_SERVICES).map(|_| inner.create_impl()).collect();
        let router = RandomRoutingCommitService::new(replicas, inner.get_env())
            .with_switch_probability(1.0 - config.stickiness());
        Ok(Self { inner, router })
    }
}

impl<CF> CheckpointFixture for RandomRoutingFixture<CF>
where
    CF: CheckpointFixture<Env = Simulator, Impl: Clone>,
{
    async fn checkpoint_position(&self, _service: &Self::Impl, bus_id: &str) -> Result<i64> {
        self.inner
            .checkpoint_position(&self.inner.create_impl(), bus_id)
            .await
    }

    fn is_checkpoint_durable(&self) -> bool {
        self.inner.is_checkpoint_durable()
    }
}

impl<CF> DeciderPolicyFixture for RandomRoutingFixture<CF>
where
    CF: DeciderPolicyFixture<Env = Simulator, Impl: Clone>,
{
    type Controller = CF::Controller;

    fn policy_controller(&self) -> Self::Controller {
        self.inner.policy_controller()
    }

    fn supports_first_boolean_wins(&self) -> bool {
        self.inner.supports_first_boolean_wins()
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
    #[derive(Clone)]
    struct CountingService {
        hits: Rc<Cell<u32>>,
        bus: InMemoryAgentBus<RealEnvironment>,
    }

    impl CommitSvc for CountingService {
        type Bus = InMemoryAgentBus<RealEnvironment>;

        fn agent_bus(&self) -> &Self::Bus {
            &self.bus
        }

        async fn create_bus(&self, _request: CreateBusCommand) -> CommitResult<()> {
            self.hits.set(self.hits.get() + 1);
            Ok(())
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
    fn sticky_router_sends_every_request_to_one_service() {
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
        let router = RandomRoutingCommitService::new(services, env).with_switch_probability(0.0);

        for _ in 0..50 {
            block_on(router.commit_intention(request())).unwrap();
        }
        assert_eq!(
            hits.iter().map(|hits| hits.get()).max(),
            Some(50),
            "a router that never switches should keep every request on its first pick"
        );
    }

    #[test]
    fn clones_route_each_bus_to_the_same_service() {
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
        let router = RandomRoutingCommitService::new(services, env).with_switch_probability(0.0);
        let clone = router.clone();

        for _ in 0..25 {
            block_on(router.commit_intention(request())).unwrap();
            block_on(clone.commit_intention(request())).unwrap();
        }
        assert_eq!(
            hits.iter().map(|hits| hits.get()).max(),
            Some(50),
            "every clone should route a bus to the service it is on"
        );
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
