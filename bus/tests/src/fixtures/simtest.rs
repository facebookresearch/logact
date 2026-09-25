/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Test fixtures for different AgentBus implementations

use std::marker::PhantomData;
use std::rc::Rc;

use agentbus_api::NoopLogger;
use agentbus_api::NoopMetrics;
use agentbus_api::WriteOnceSpace;
use agentbus_api::metrics::AgentBusMetrics;
use agentbus_core::ChanneledAgentBus;
use agentbus_observable::ObservableAgentBus;
use agentbus_simple::InMemoryAgentBus;
use conformance::ConformanceFixture;
use rand::distr::Uniform;

use super::SimulatorFixture;
use super::WriteOnceAgentBusGenericFixture;
use crate::common::fault_config::FaultConfig;
use crate::impls::chained_agentbus::ChainedAgentBus;
use crate::impls::fault_injecting_agentbus::FaultInjectingAgentBus;
use crate::simulator::Simulator;
use crate::write_once_space::fixtures::simtest::ChanneledWriteOnceSpaceFixture;
use crate::write_once_space::fixtures::simtest::ConflictOnceWriteOnceSpaceFixture;
use crate::write_once_space::fixtures::simtest::FaultInjectingWriteOnceSpaceFixture;
use crate::write_once_space::fixtures::simtest::InMemoryWriteOnceSpaceFixture;

/// Fixture for testing InMemoryAgentBus
/// Owns an InMemoryAgentBus instance that can be cloned to share state
pub struct SimpleMemoryFixture {
    agentbus: InMemoryAgentBus<Simulator>,
}

impl ConformanceFixture for SimpleMemoryFixture {
    type Env = Simulator;
    type Impl = InMemoryAgentBus<Simulator>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.agentbus.environment()
    }

    fn create_impl(&self) -> Self::Impl {
        // self.agentbus owns an Rc to the backend state; the Rc is cloned here.
        self.agentbus.clone()
    }
}

impl SimulatorFixture for SimpleMemoryFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            agentbus: InMemoryAgentBus::new(Rc::new(simulator)),
        }
    }
}

/// Fixture for testing ChanneledAgentBus (wrapper around InMemoryAgentBus)
pub struct ChanneledAgentBusFixture {
    agentbus: ChanneledAgentBus,
    env: Rc<Simulator>,
}

impl ConformanceFixture for ChanneledAgentBusFixture {
    type Env = Simulator;
    type Impl = ChanneledAgentBus;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        // self.agentbus is a wrapper around a channel sender; the sender is cloned here. The
        // receiver and backend state are owned by the `run()` task in the simulator.
        self.agentbus.clone()
    }
}

impl SimulatorFixture for ChanneledAgentBusFixture {
    fn new(simulator: Simulator) -> Self {
        let env = Rc::new(simulator);
        let worker_env = env.clone();
        let agentbus = ChanneledAgentBus::new_on_environment(env.clone(), move || {
            InMemoryAgentBus::new(worker_env.clone())
        });
        Self { agentbus, env }
    }
}

/// Generic fixture for testing any AgentBus implementation with fault and latency injection
pub struct FaultInjectingFixture<
    F: SimulatorFixture + ConformanceFixture<Env = Simulator, Impl: agentbus_api::AgentBus + 'static>,
> {
    inner: F,
    config: FaultConfig,
    append_latency_ms: Uniform<u64>,
    poll_latency_ms: Uniform<u64>,
    metrics: Rc<dyn AgentBusMetrics>,
}

impl<F> FaultInjectingFixture<F>
where
    F: SimulatorFixture
        + ConformanceFixture<Env = Simulator, Impl: agentbus_api::AgentBus + 'static>,
{
    pub fn new_with_config(simulator: Simulator, config: FaultConfig) -> Self {
        Self::new_with_config_and_latency(
            simulator,
            config,
            Uniform::new(0, 1).unwrap(),
            Uniform::new(0, 1).unwrap(),
        )
    }

    pub fn new_with_config_and_latency(
        simulator: Simulator,
        config: FaultConfig,
        append_latency_ms: Uniform<u64>,
        poll_latency_ms: Uniform<u64>,
    ) -> Self {
        Self {
            inner: F::new(simulator),
            config,
            append_latency_ms,
            poll_latency_ms,
            metrics: Rc::new(NoopMetrics),
        }
    }
}

impl<F> ConformanceFixture for FaultInjectingFixture<F>
where
    F: SimulatorFixture
        + ConformanceFixture<Env = Simulator, Impl: agentbus_api::AgentBus + 'static>,
{
    type Env = Simulator;
    type Impl = ObservableAgentBus<FaultInjectingAgentBus<F::Impl, Uniform<u64>>, Simulator>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let backing_bus = self.inner.create_impl();
        let fault_bus = FaultInjectingAgentBus::new(
            backing_bus,
            self.config.clone(),
            self.append_latency_ms,
            self.poll_latency_ms,
            self.inner.get_env(),
        )
        .expect("Failed to create FaultInjectingAgentBus");
        ObservableAgentBus::new(
            fault_bus,
            self.metrics.clone(),
            Rc::new(NoopLogger),
            self.inner.get_env(),
        )
    }
}

impl<F> SimulatorFixture for FaultInjectingFixture<F>
where
    F: SimulatorFixture
        + ConformanceFixture<Env = Simulator, Impl: agentbus_api::AgentBus + 'static>,
{
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
            config: FaultConfig {
                prob_lost: 0.0,
                prob_commit_then_error: 0.0,
                prob_error_then_commit: 0.0,
            },
            append_latency_ms: Uniform::new(0, 10).unwrap(),
            poll_latency_ms: Uniform::new(0, 10).unwrap(),
            metrics: Rc::new(NoopMetrics),
        }
    }
}

/// Generic fixture for testing ChainedAgentBus with configurable mode
pub struct ChainedAgentBusGenericFixture<const MODE: u8> {
    agentbus: ChainedAgentBus,
}

impl<const MODE: u8> ConformanceFixture for ChainedAgentBusGenericFixture<MODE> {
    type Env = Simulator;
    type Impl = ChainedAgentBus;

    fn get_env(&self) -> Rc<Self::Env> {
        self.agentbus.environment()
    }

    fn create_impl(&self) -> Self::Impl {
        self.agentbus.clone()
    }
}

impl<const MODE: u8> SimulatorFixture for ChainedAgentBusGenericFixture<MODE> {
    fn new(simulator: Simulator) -> Self {
        let env = Rc::new(simulator);
        let agentbus = match MODE {
            0 => ChainedAgentBus::new_correct(env),
            1 => ChainedAgentBus::new_buggy_poll(env),
            _ => panic!("Invalid mode"),
        };
        Self { agentbus }
    }
}

/// Correct chain replication mode
pub type ChainedAgentBusFixture = ChainedAgentBusGenericFixture<0>;

/// Buggy poll mode for linearizability checker testing
pub type ChainedAgentBusBuggyPollFixture = ChainedAgentBusGenericFixture<1>;

// ===========================================================================
// Fault fixtures for the annotation-driven failure-injection scenarios.
//
// Faults live in the *fixture* (not the scenario body) because space-level faults
// are injected below the bus, where a scenario can't reach. A generic
// `FaultFixture<Base, Mix>` bakes a fault `Mix` onto either fault family, so a
// failure-injection scenario just pins `FaultFixture<Base, Mix>` via
// `#[scenario_for]` instead of a hand-written per-fixture driver.
// ===========================================================================

/// Fault fixtures constructible with an explicit [`FaultConfig`], so [`FaultFixture`]
/// can bake a config generically over both the bus-level ([`FaultInjectingFixture`])
/// and space-level ([`WriteOnceAgentBusGenericFixture`] over
/// [`FaultInjectingWriteOnceSpaceFixture`]) families.
pub trait WithFaultConfig: Sized {
    fn with_fault_config(simulator: Simulator, config: FaultConfig) -> Self;
}

impl<F> WithFaultConfig for FaultInjectingFixture<F>
where
    F: SimulatorFixture
        + ConformanceFixture<Env = Simulator, Impl: agentbus_api::AgentBus + 'static>,
{
    fn with_fault_config(simulator: Simulator, config: FaultConfig) -> Self {
        Self::new_with_config(simulator, config)
    }
}

impl<F> WithFaultConfig for WriteOnceAgentBusGenericFixture<FaultInjectingWriteOnceSpaceFixture<F>>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture + 'static,
    F::Impl: Clone + 'static,
{
    fn with_fault_config(simulator: Simulator, config: FaultConfig) -> Self {
        Self::new_with_config(simulator, config)
    }
}

/// A fault mix encoded as a type, so it can be a `#[scenario_for]` pin parameter.
pub trait FaultMix {
    const CONFIG: FaultConfig;
}

/// Every append is silently lost.
pub struct Lost;
impl FaultMix for Lost {
    const CONFIG: FaultConfig = FaultConfig {
        prob_lost: 1.0,
        prob_commit_then_error: 0.0,
        prob_error_then_commit: 0.0,
    };
}

/// Every append commits but returns an error to the caller.
pub struct CommitThenError;
impl FaultMix for CommitThenError {
    const CONFIG: FaultConfig = FaultConfig {
        prob_lost: 0.0,
        prob_commit_then_error: 1.0,
        prob_error_then_commit: 0.0,
    };
}

/// Every append returns an error but later commits.
pub struct ErrorThenCommit;
impl FaultMix for ErrorThenCommit {
    const CONFIG: FaultConfig = FaultConfig {
        prob_lost: 0.0,
        prob_commit_then_error: 0.0,
        prob_error_then_commit: 1.0,
    };
}

/// 25% of each fault type — exercises every legal commit ordering over enough runs.
pub struct Mixed;
impl FaultMix for Mixed {
    const CONFIG: FaultConfig = FaultConfig {
        prob_lost: 0.25,
        prob_commit_then_error: 0.25,
        prob_error_then_commit: 0.25,
    };
}

/// Config-baking wrapper: builds the wrapped fault fixture `F` at `M::CONFIG`, so a
/// `#[scenario_for(FaultFixture<Base, Mix>, ..)]` pin selects both the injection
/// layer (via `Base`) and the fault mix (via `Mix`).
pub struct FaultFixture<F, M>(F, PhantomData<M>);

impl<F: WithFaultConfig, M: FaultMix> SimulatorFixture for FaultFixture<F, M> {
    fn new(simulator: Simulator) -> Self {
        Self(F::with_fault_config(simulator, M::CONFIG), PhantomData)
    }
}

impl<F: ConformanceFixture, M> ConformanceFixture for FaultFixture<F, M> {
    type Env = F::Env;
    type Impl = F::Impl;

    fn get_env(&self) -> Rc<Self::Env> {
        self.0.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.0.create_impl()
    }
}

// Base fault fixtures, aliased so scenario pins stay readable. Bus-level faults are
// injected at the AgentBus layer (which can reorder same-client proposals);
// space-level faults are injected below the bus at the WriteOnceSpace layer (which
// preserves per-client FIFO).
pub type BusSimpleMemory = FaultInjectingFixture<SimpleMemoryFixture>;
pub type BusChanneled = FaultInjectingFixture<ChanneledAgentBusFixture>;
pub type BusChained = FaultInjectingFixture<ChainedAgentBusFixture>;
pub type SpaceInMemory = WriteOnceAgentBusGenericFixture<
    FaultInjectingWriteOnceSpaceFixture<InMemoryWriteOnceSpaceFixture>,
>;
pub type SpaceChanneled = WriteOnceAgentBusGenericFixture<
    FaultInjectingWriteOnceSpaceFixture<ChanneledWriteOnceSpaceFixture>,
>;
pub type SpaceTransactionConflictEmpty = WriteOnceAgentBusGenericFixture<
    ConflictOnceWriteOnceSpaceFixture<InMemoryWriteOnceSpaceFixture, false>,
>;
pub type SpaceTransactionConflictOccupied = WriteOnceAgentBusGenericFixture<
    ConflictOnceWriteOnceSpaceFixture<InMemoryWriteOnceSpaceFixture, true>,
>;
