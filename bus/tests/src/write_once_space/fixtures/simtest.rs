/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Test fixtures for WriteOnceSpace implementations using the simulator

use std::rc::Rc;

use agentbus_api::WriteOnceSpace;
use agentbus_writeonce::ChanneledWriteOnceSpace;
use agentbus_writeonce::InMemoryWriteOnceSpace;
use conformance::ConformanceFixture;
use rand::distr::Uniform;

use crate::common::fault_config::FaultConfig;
use crate::fixtures::SimulatorFixture;
use crate::impls::fault_injecting_write_once_space::ConflictOnceWriteOnceSpace;
use crate::impls::fault_injecting_write_once_space::FaultInjectingWriteOnceSpace;
use crate::simulator::Simulator;

/// Fixture for testing InMemoryWriteOnceSpace directly.
pub struct InMemoryWriteOnceSpaceFixture {
    space: InMemoryWriteOnceSpace,
    env: Rc<Simulator>,
}

impl ConformanceFixture for InMemoryWriteOnceSpaceFixture {
    type Env = Simulator;
    type Impl = InMemoryWriteOnceSpace;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.space.clone()
    }
}

impl SimulatorFixture for InMemoryWriteOnceSpaceFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            space: InMemoryWriteOnceSpace::new(),
            env: Rc::new(simulator),
        }
    }
}

/// Fixture for testing ChanneledWriteOnceSpace explicitly.
pub struct ChanneledWriteOnceSpaceFixture {
    space: ChanneledWriteOnceSpace,
    env: Rc<Simulator>,
}

impl ConformanceFixture for ChanneledWriteOnceSpaceFixture {
    type Env = Simulator;
    type Impl = ChanneledWriteOnceSpace;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.space.clone()
    }
}

impl SimulatorFixture for ChanneledWriteOnceSpaceFixture {
    fn new(simulator: Simulator) -> Self {
        let env = Rc::new(simulator);
        let (channeled_space, space_backend) =
            ChanneledWriteOnceSpace::new(InMemoryWriteOnceSpace::new());
        let _space_handle = env.spawn(space_backend.run());
        Self {
            space: channeled_space,
            env,
        }
    }
}

/// Fixture that injects one transaction conflict into an in-memory write-once space.
pub struct ConflictOnceWriteOnceSpaceFixture<F, const OCCUPY_CONFLICTING_SLOT: bool> {
    inner: F,
}

impl<F, const OCCUPY_CONFLICTING_SLOT: bool> ConformanceFixture
    for ConflictOnceWriteOnceSpaceFixture<F, OCCUPY_CONFLICTING_SLOT>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture,
    F::Impl: Clone + 'static,
{
    type Env = Simulator;
    type Impl = ConflictOnceWriteOnceSpace<F::Impl>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        ConflictOnceWriteOnceSpace::new(self.inner.create_impl(), OCCUPY_CONFLICTING_SLOT)
    }
}

impl<F, const OCCUPY_CONFLICTING_SLOT: bool> SimulatorFixture
    for ConflictOnceWriteOnceSpaceFixture<F, OCCUPY_CONFLICTING_SLOT>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture,
    F::Impl: Clone + 'static,
{
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
        }
    }
}

/// Generic fixture that wraps any WriteOnceSpace fixture with fault injection
pub struct FaultInjectingWriteOnceSpaceFixture<
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture,
> {
    inner: F,
    config: FaultConfig,
    write_latency_ms: Uniform<u64>,
    read_latency_ms: Uniform<u64>,
}

impl<F> FaultInjectingWriteOnceSpaceFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture,
    F::Impl: Clone + 'static,
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
        write_latency_ms: Uniform<u64>,
        read_latency_ms: Uniform<u64>,
    ) -> Self {
        Self {
            inner: F::new(simulator),
            config,
            write_latency_ms,
            read_latency_ms,
        }
    }
}

impl<F> ConformanceFixture for FaultInjectingWriteOnceSpaceFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture,
    F::Impl: Clone + 'static,
{
    type Env = Simulator;
    type Impl = FaultInjectingWriteOnceSpace<F::Impl, Uniform<u64>>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let backing_space = self.inner.create_impl();
        FaultInjectingWriteOnceSpace::new(
            backing_space,
            self.config.clone(),
            self.write_latency_ms,
            self.read_latency_ms,
            self.inner.get_env(),
        )
        .expect("Failed to create FaultInjectingWriteOnceSpace")
    }
}

impl<F> SimulatorFixture for FaultInjectingWriteOnceSpaceFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture,
    F::Impl: Clone + 'static,
{
    fn new(simulator: Simulator) -> Self {
        Self::new_with_config_and_latency(
            simulator,
            FaultConfig {
                prob_lost: 0.0,
                prob_commit_then_error: 0.0,
                prob_error_then_commit: 0.0,
            },
            Uniform::new(0, 10).unwrap(),
            Uniform::new(0, 10).unwrap(),
        )
    }
}
