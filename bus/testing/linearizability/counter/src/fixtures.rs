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
use conformance::ConformanceFixture;
use conformance::SimulatorFixture;
use rand::RngExt as _;

use crate::agentbus::AgentBusCounterImplementation;
use crate::agentbus::AgentBusCounterOptions;
use crate::agentbus::PollEntries;

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

pub type BuggyPollCounterFixture = SkipCommitsCounterFixture<ChainedAgentBusBuggyPollFixture>;
