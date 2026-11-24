/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic fixture for WriteOnceAgentBusV2 composed from WriteOnceSpace fixtures.
//!
//! This enables recursive fixture composition - AgentBus fixtures can be built
//! from WriteOnceSpace fixtures, inheriting their construction traits.

use std::marker::PhantomData;
use std::rc::Rc;

use agentbus_api::WriteOnceSpace;
use agentbus_writeonce::Sequencer;
use agentbus_writeonce::SimpleSequencer;
use agentbus_writeonce::WriteOnceAgentBusV2;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::TestConfig;
use fbinit::FacebookInit;
use rand::distr::Uniform;

use super::IntegrationFixture;
use super::SimulatorFixture;
use crate::common::fault_config::FaultConfig;
use crate::simulator::Simulator;
use crate::write_once_space::fixtures::WriteOnceSpaceTestFixture;
use crate::write_once_space::fixtures::simtest::FaultInjectingWriteOnceSpaceFixture;

/// Generic fixture for WriteOnceAgentBusV2 composed from WriteOnceSpace fixtures.
///
/// This allows AgentBus fixtures to be composed from WriteOnceSpace fixtures,
/// enabling recursive fixture composition. The fixture inherits the construction
/// trait from the underlying space fixture. `Q` is the bus's sequencer.
pub struct WriteOnceAgentBusV2GenericFixture<WOF: WriteOnceSpaceTestFixture, Q = SimpleSequencer>
where
    WOF::Impl: Clone,
{
    space_fixture: Rc<WOF>,
    sequencer: PhantomData<Q>,
}

impl<WOF: WriteOnceSpaceTestFixture + 'static, Q> WriteOnceAgentBusV2GenericFixture<WOF, Q>
where
    WOF::Impl: Clone + 'static,
{
    pub fn new(space_fixture: Rc<WOF>) -> Self {
        Self {
            space_fixture,
            sequencer: PhantomData,
        }
    }
}

impl<WOF: WriteOnceSpaceTestFixture + 'static, Q: Sequencer<WOF::Env> + 'static> ConformanceFixture
    for WriteOnceAgentBusV2GenericFixture<WOF, Q>
where
    WOF::Impl: Clone + 'static,
{
    type Env = WOF::Env;
    type Impl = WriteOnceAgentBusV2<WOF::Impl, WOF::Env, Q>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.space_fixture.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let space = self.space_fixture.create_impl();
        let environment = self.space_fixture.get_env();
        WriteOnceAgentBusV2::new(space, environment, None)
    }
}

impl<WOF, Q> SimulatorFixture for WriteOnceAgentBusV2GenericFixture<WOF, Q>
where
    Q: Sequencer<WOF::Env> + 'static,
    WOF: WriteOnceSpaceTestFixture + SimulatorFixture + 'static,
    WOF::Impl: Clone + 'static,
{
    fn new(simulator: Simulator) -> Self {
        let space_fixture = Rc::new(WOF::new(simulator));
        Self::new(space_fixture)
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let space_fixture = Rc::new(WOF::try_new_with_config(simulator, config)?);
        Ok(Self::new(space_fixture))
    }
}

impl<WOF, Q> IntegrationFixture for WriteOnceAgentBusV2GenericFixture<WOF, Q>
where
    Q: Sequencer<WOF::Env> + 'static,
    WOF: WriteOnceSpaceTestFixture + IntegrationFixture + 'static,
    WOF::Impl: Clone + 'static,
{
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        let space_fixture = Rc::new(WOF::new_async(fb).await?);
        Ok(Self::new(space_fixture))
    }
}

impl<F, Q> WriteOnceAgentBusV2GenericFixture<FaultInjectingWriteOnceSpaceFixture<F>, Q>
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
