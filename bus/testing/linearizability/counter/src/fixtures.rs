/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::marker::PhantomData;
use std::rc::Rc;

use agentbus_api::Environment;
use agentbus_simulator::Simulator;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::simtest::ChainedAgentBusBuggyPollFixture;
use agentbus_tests::fixtures::simtest::FaultInjectingFixture;
use agentbus_tests::fixtures::simtest::SimpleMemoryFixture;
use conformance::ConformanceFixture;
use conformance::SimulatorFixture;
use logact_commit_service_tests::fixtures::CommitServiceTestFixture;
use logact_commit_service_tests::fixtures::CommitServiceV1CountingVoterFixture;
use rand::RngExt as _;

use crate::agentbus::AgentBusCounterImplementation;
use crate::agentbus::AgentBusCounterOptions;
use crate::agentbus::PollEntries;
use crate::commit_service::CommitServiceCounterImplementation;

pub trait AgentBusCounterConfiguration {
    fn options() -> AgentBusCounterOptions;
}

pub struct DefaultAgentBusCounterConfiguration;

impl AgentBusCounterConfiguration for DefaultAgentBusCounterConfiguration {
    fn options() -> AgentBusCounterOptions {
        AgentBusCounterOptions::default()
    }
}

pub struct SkipCommitsAgentBusCounterConfiguration;

impl AgentBusCounterConfiguration for SkipCommitsAgentBusCounterConfiguration {
    fn options() -> AgentBusCounterOptions {
        AgentBusCounterOptions {
            skip_commits: true,
            poll_entries: PollEntries::Random,
        }
    }
}

pub struct RandomPollAgentBusCounterConfiguration;

impl AgentBusCounterConfiguration for RandomPollAgentBusCounterConfiguration {
    fn options() -> AgentBusCounterOptions {
        AgentBusCounterOptions {
            skip_commits: false,
            poll_entries: PollEntries::Random,
        }
    }
}

pub struct SmallPollAgentBusCounterConfiguration;

impl AgentBusCounterConfiguration for SmallPollAgentBusCounterConfiguration {
    fn options() -> AgentBusCounterOptions {
        AgentBusCounterOptions {
            skip_commits: false,
            poll_entries: PollEntries::Fixed(1),
        }
    }
}

pub struct AgentBusCounterFixture<F, C = DefaultAgentBusCounterConfiguration> {
    inner: Rc<F>,
    configuration: PhantomData<C>,
}

impl<F: SimulatorFixture, C> SimulatorFixture for AgentBusCounterFixture<F, C> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: Rc::new(F::new(simulator)),
            configuration: PhantomData,
        }
    }
}

impl<F, C> ConformanceFixture for AgentBusCounterFixture<F, C>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    F::Impl: 'static,
    C: AgentBusCounterConfiguration,
{
    type Env = Simulator;
    type Impl = AgentBusCounterImplementation<F>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let environment = self.inner.get_env();
        let agent_bus_id = format!(
            "counter-{}",
            environment.with_rng(|rng| rng.random::<u64>())
        );
        AgentBusCounterImplementation::with_configuration(
            self.inner.clone(),
            agent_bus_id,
            C::options(),
        )
    }
}

pub type SkipCommitsCounterFixture<F> =
    AgentBusCounterFixture<F, SkipCommitsAgentBusCounterConfiguration>;
pub type RandomPollCounterFixture<F> =
    AgentBusCounterFixture<F, RandomPollAgentBusCounterConfiguration>;
pub type SmallPollCounterFixture<F> =
    AgentBusCounterFixture<F, SmallPollAgentBusCounterConfiguration>;

pub struct CommitServiceCounterFixture<F> {
    inner: Rc<F>,
}

impl<F: SimulatorFixture> SimulatorFixture for CommitServiceCounterFixture<F> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: Rc::new(F::new(simulator)),
        }
    }
}

pub trait CommitServiceCounterTestFixture: ConformanceFixture<Env = Simulator> {
    type Inner: CommitServiceTestFixture<Env = Simulator>;

    fn inner(&self) -> Rc<Self::Inner>;
}

impl<F> CommitServiceCounterTestFixture for CommitServiceCounterFixture<F>
where
    F: CommitServiceTestFixture<Env = Simulator>,
    F::Impl: 'static,
{
    type Inner = F;

    fn inner(&self) -> Rc<Self::Inner> {
        self.inner.clone()
    }
}

impl<F> ConformanceFixture for CommitServiceCounterFixture<F>
where
    F: CommitServiceTestFixture<Env = Simulator>,
    F::Impl: 'static,
{
    type Env = Simulator;
    type Impl = CommitServiceCounterImplementation<F>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        CommitServiceCounterImplementation::new(self.inner.clone())
    }
}

pub type BuggyPollCounterFixture = SkipCommitsCounterFixture<ChainedAgentBusBuggyPollFixture>;
pub type CountingVoterCounterFixture =
    CommitServiceCounterFixture<CommitServiceV1CountingVoterFixture<SimpleMemoryFixture>>;
pub type FaultyCountingVoterCounterFixture = CommitServiceCounterFixture<
    CommitServiceV1CountingVoterFixture<FaultInjectingFixture<SimpleMemoryFixture>>,
>;
