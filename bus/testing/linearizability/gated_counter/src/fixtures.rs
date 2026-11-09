/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_simulator::Simulator;
use agentbus_tests::fixtures::AgentBusTestFixture;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::SimulatorFixture;
use conformance::TestConfig;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_tests::fixtures::DeciderPolicyFixture;

use crate::agentbus::AgentBusGatedCounterImplementation;
use crate::commit_service::CommitServiceGatedCounterImplementation;
use crate::commit_service::gated_counter_implementation;

pub struct AgentBusGatedCounterFixture<F> {
    inner: Rc<F>,
}

impl<F: SimulatorFixture> SimulatorFixture for AgentBusGatedCounterFixture<F> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: Rc::new(F::new(simulator)),
        }
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            inner: Rc::new(F::try_new_with_config(simulator, config)?),
        })
    }
}

impl<F> ConformanceFixture for AgentBusGatedCounterFixture<F>
where
    F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    F::Impl: 'static,
{
    type Env = Simulator;
    type Impl = AgentBusGatedCounterImplementation<F>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        AgentBusGatedCounterImplementation::new(self.inner.clone())
    }
}

pub struct CommitServiceGatedCounterFixture<F> {
    inner: F,
}

impl<F: SimulatorFixture> SimulatorFixture for CommitServiceGatedCounterFixture<F> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
        }
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            inner: F::try_new_with_config(simulator, config)?,
        })
    }
}

impl<F> ConformanceFixture for CommitServiceGatedCounterFixture<F>
where
    F: DeciderPolicyFixture<Env = Simulator>,
    F::Impl: Clone + 'static,
    <F::Impl as CommitSvc>::Bus: Clone + 'static,
    F::Controller: 'static,
{
    type Env = Simulator;
    type Impl = CommitServiceGatedCounterImplementation<
        F::Impl,
        F::Controller,
        <F::Impl as CommitSvc>::Bus,
    >;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        gated_counter_implementation(&self.inner)
    }
}
