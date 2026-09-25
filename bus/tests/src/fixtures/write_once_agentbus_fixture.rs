/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic fixture for WriteOnceAgentBus composed from WriteOnceSpace fixtures.
//!
//! This enables recursive fixture composition - AgentBus fixtures can be built
//! from WriteOnceSpace fixtures, inheriting their construction traits.

use std::rc::Rc;

use agentbus_api::WriteOnceSpace;
use agentbus_writeonce::WriteOnceAgentBus;
use anyhow::Result;
use conformance::ConformanceFixture;
use fbinit::FacebookInit;
use rand::distr::Uniform;

use super::IntegrationFixture;
use super::SimulatorFixture;
use crate::common::fault_config::FaultConfig;
use crate::simulator::Simulator;
use crate::write_once_space::fixtures::WriteOnceSpaceTestFixture;
use crate::write_once_space::fixtures::simtest::FaultInjectingWriteOnceSpaceFixture;

/// Generic fixture for WriteOnceAgentBus composed from WriteOnceSpace fixtures.
///
/// This allows AgentBus fixtures to be composed from WriteOnceSpace fixtures,
/// enabling recursive fixture composition. The fixture inherits the construction
/// trait from the underlying space fixture.
pub struct WriteOnceAgentBusGenericFixture<WOF: WriteOnceSpaceTestFixture>
where
    WOF::Impl: Clone,
{
    space_fixture: Rc<WOF>,
    agentbus: WriteOnceAgentBus<WOF::Impl, WOF::Env>,
}

impl<WOF: WriteOnceSpaceTestFixture + 'static> WriteOnceAgentBusGenericFixture<WOF>
where
    WOF::Impl: Clone + 'static,
{
    pub fn new(space_fixture: Rc<WOF>) -> Self {
        let space = space_fixture.create_impl();
        let env = space_fixture.get_env();
        Self {
            agentbus: WriteOnceAgentBus::new(space, env, None),
            space_fixture,
        }
    }
}

impl<WOF: WriteOnceSpaceTestFixture + 'static> ConformanceFixture
    for WriteOnceAgentBusGenericFixture<WOF>
where
    WOF::Impl: Clone + 'static,
{
    type Env = WOF::Env;
    type Impl = WriteOnceAgentBus<WOF::Impl, WOF::Env>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.space_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.agentbus.clone()
    }
}

impl<WOF> SimulatorFixture for WriteOnceAgentBusGenericFixture<WOF>
where
    WOF: WriteOnceSpaceTestFixture + SimulatorFixture + 'static,
    WOF::Impl: Clone + 'static,
{
    fn new(simulator: Simulator) -> Self {
        let space_fixture = Rc::new(WOF::new(simulator));
        Self::new(space_fixture)
    }
}

impl<WOF> IntegrationFixture for WriteOnceAgentBusGenericFixture<WOF>
where
    WOF: WriteOnceSpaceTestFixture + IntegrationFixture + 'static,
    WOF::Impl: Clone + 'static,
{
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        let space_fixture = Rc::new(WOF::new_async(fb).await?);
        Ok(Self::new(space_fixture))
    }
}

impl<F> WriteOnceAgentBusGenericFixture<FaultInjectingWriteOnceSpaceFixture<F>>
where
    F: ConformanceFixture<Env = Simulator, Impl: WriteOnceSpace> + SimulatorFixture + 'static,
    F::Impl: Clone + 'static,
{
    pub fn new_with_config(simulator: Simulator, config: FaultConfig) -> Self {
        let space_fixture = Rc::new(FaultInjectingWriteOnceSpaceFixture::new_with_config(
            simulator, config,
        ));
        Self::new(space_fixture)
    }

    pub fn new_with_config_and_latency(
        simulator: Simulator,
        config: FaultConfig,
        write_latency_ms: Uniform<u64>,
        read_latency_ms: Uniform<u64>,
    ) -> Self {
        let space_fixture = Rc::new(
            FaultInjectingWriteOnceSpaceFixture::new_with_config_and_latency(
                simulator,
                config,
                write_latency_ms,
                read_latency_ms,
            ),
        );
        Self::new(space_fixture)
    }
}
