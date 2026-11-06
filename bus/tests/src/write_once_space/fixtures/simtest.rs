/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Test fixtures for WriteOnceSpace implementations using the simulator

use std::rc::Rc;
use std::time::Duration;

use agentbus_api::WriteOnceSpace;
use agentbus_writeonce::CachingWriteOnceSpace;
use agentbus_writeonce::ChanneledWriteOnceSpace;
use agentbus_writeonce::InMemoryWriteOnceSpace;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::TestConfig;
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

/// In-memory storage whose writes expire after `ttl`, if set.
fn in_memory_space(env: &Rc<Simulator>, ttl: Option<Duration>) -> InMemoryWriteOnceSpace {
    match ttl {
        Some(ttl) => InMemoryWriteOnceSpace::with_ttl(env.clone(), ttl),
        None => InMemoryWriteOnceSpace::new(),
    }
}

impl SimulatorFixture for InMemoryWriteOnceSpaceFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let env = Rc::new(simulator);
        Ok(Self {
            space: in_memory_space(&env, config.ttl),
            env,
        })
    }
}

/// Fixture for a bounded cache over another write-once-space fixture.
pub struct CachingWriteOnceSpaceFixture<F = InMemoryWriteOnceSpaceFixture>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace>,
{
    space: CachingWriteOnceSpace<F::Impl>,
    env: Rc<Simulator>,
}

impl<F> CachingWriteOnceSpaceFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace>,
{
    pub fn from_fixture(inner: F) -> Self {
        Self::from_fixture_with_ttl(inner, None)
    }

    /// Expire cached writes after `ttl`, if set, matching the backing storage.
    fn from_fixture_with_ttl(inner: F, ttl: Option<Duration>) -> Self {
        let space = CachingWriteOnceSpace::new(inner.create_impl(), 2, 64 * 1024);
        Self {
            space: match ttl {
                Some(ttl) => space.with_write_ttl(inner.get_env(), ttl),
                None => space,
            },
            env: inner.get_env(),
        }
    }
}

impl<F> ConformanceFixture for CachingWriteOnceSpaceFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace>,
    F::Impl: Clone,
{
    type Env = Simulator;
    type Impl = CachingWriteOnceSpace<F::Impl>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.space.clone()
    }
}

impl<F> SimulatorFixture for CachingWriteOnceSpaceFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture,
    F::Impl: Clone,
{
    fn new(simulator: Simulator) -> Self {
        Self::from_fixture(F::new(simulator))
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self::from_fixture_with_ttl(
            F::try_new_with_config(simulator, config)?,
            config.ttl,
        ))
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
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let env = Rc::new(simulator);
        let (channeled_space, space_backend) =
            ChanneledWriteOnceSpace::new(in_memory_space(&env, config.ttl));
        let _space_handle = env.spawn(space_backend.run());
        Ok(Self {
            space: channeled_space,
            env,
        })
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
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    /// The scenario's config goes to the wrapped space fixture.
    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            inner: F::try_new_with_config(simulator, config)?,
            config: FaultConfig {
                prob_lost: 0.0,
                prob_commit_then_error: 0.0,
                prob_error_then_commit: 0.0,
            },
            write_latency_ms: Uniform::new(0, 10).unwrap(),
            read_latency_ms: Uniform::new(0, 10).unwrap(),
        })
    }
}
