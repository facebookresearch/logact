/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Write-once AgentBus instances of two adjacent versions over one space, as on
//! a bus whose servers are moving from one version to the next.

use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::environment::Environment;
use agentbus_core::ChanneledAgentBus;
use agentbus_writeonce::Sequencer;
use agentbus_writeonce::WindowedSequencer;
use agentbus_writeonce::WriteOnceBusConsumer;
use agentbus_writeonce::WriteOnceBusVersion;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::TestConfig;

use super::SimulatorFixture;
use super::TailWindowFixture;
use crate::simulator::Simulator;
use crate::write_once_space::fixtures::WriteOnceSpaceTestFixture;

/// The versions a [`WriteOnceMixedVersionFixture`] alternates between.
pub trait VersionPair {
    const VERSIONS: [WriteOnceBusVersion; 2];
}

pub struct V1AndSimpleBigTailWindow;
impl VersionPair for V1AndSimpleBigTailWindow {
    const VERSIONS: [WriteOnceBusVersion; 2] = [
        WriteOnceBusVersion::V1,
        WriteOnceBusVersion::V2SimpleBigTailWindow,
    ];
}

pub struct SimpleBigTailWindowAndWindowed;
impl VersionPair for SimpleBigTailWindowAndWindowed {
    const VERSIONS: [WriteOnceBusVersion; 2] = [
        WriteOnceBusVersion::V2SimpleBigTailWindow,
        WriteOnceBusVersion::V2Windowed,
    ];
}

/// Each `create_impl` builds the next of `P`'s versions, in turn, over the
/// shared space, behind a `ChanneledAgentBus` as servers serve it.
pub struct WriteOnceMixedVersionFixture<WOF, P> {
    space_fixture: Rc<WOF>,
    created: Cell<usize>,
    versions: PhantomData<P>,
}

impl<WOF, P> ConformanceFixture for WriteOnceMixedVersionFixture<WOF, P>
where
    WOF: WriteOnceSpaceTestFixture<Env: Environment + 'static, Impl: Clone>,
    P: VersionPair,
{
    type Env = WOF::Env;
    type Impl = ChanneledAgentBus;

    fn get_env(&self) -> Rc<Self::Env> {
        self.space_fixture.get_env()
    }

    fn create_impl(&self) -> ChanneledAgentBus {
        let created = self.created.replace(self.created.get() + 1);
        let env = self.get_env();
        P::VERSIONS[created % P::VERSIONS.len()].build(
            self.space_fixture.create_impl(),
            env.clone(),
            None,
            Channeled(env),
        )
    }
}

impl<WOF, P> TailWindowFixture for WriteOnceMixedVersionFixture<WOF, P>
where
    WOF: WriteOnceSpaceTestFixture<Env: Environment + 'static, Impl: Clone>,
    P: VersionPair,
{
    /// The larger of the two versions' windows.
    fn tail_window_size(&self) -> u64 {
        let [first, second] = P::VERSIONS.map(|version| match version {
            WriteOnceBusVersion::V1 => 1,
            WriteOnceBusVersion::V2SimpleBigTailWindow | WriteOnceBusVersion::V2Windowed => {
                <WindowedSequencer as Sequencer<WOF::Env>>::TAIL_WINDOW_SIZE
            }
        });
        first.max(second)
    }
}

impl<WOF, P> SimulatorFixture for WriteOnceMixedVersionFixture<WOF, P>
where
    WOF: WriteOnceSpaceTestFixture<Env: Environment + 'static, Impl: Clone> + SimulatorFixture,
    P: VersionPair,
{
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            space_fixture: Rc::new(WOF::try_new_with_config(simulator, config)?),
            created: Cell::new(0),
            versions: PhantomData,
        })
    }
}

struct Channeled<E>(Rc<E>);

impl<E: Environment + 'static> WriteOnceBusConsumer for Channeled<E> {
    type Output = ChanneledAgentBus;

    fn consume<B: AgentBus + 'static>(self, bus: B) -> ChanneledAgentBus {
        ChanneledAgentBus::new_on_environment(self.0, move || bus)
    }
}
