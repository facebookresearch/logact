/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Which write-once AgentBus a server builds, while V2 rolls out.

use std::rc::Rc;
use std::time::Duration;

use agentbus_api::AgentBus;
use agentbus_api::FeatureFlags;
use agentbus_api::WriteOnceSpace;
use agentbus_api::environment::Environment;

use crate::sequencer::Sequencer;
use crate::simple_sequencer::SimpleSequencer;
use crate::windowed_sequencer::WindowedSequencer;
use crate::write_once_agentbus::WriteOnceAgentBus;
use crate::write_once_agentbus_v2::WriteOnceAgentBusV2;

/// The write-once AgentBus stages, in rollout order.
///
/// Bus instances on one space interoperate when they are at most one stage
/// apart, so move every instance serving a bus to a stage before moving any to
/// the next, and roll back the same way. `V1` and `V2SimpleBigTailWindow` write
/// one entry at a time and leave no unwritten positions below the tail.
/// `V2Windowed` may leave up to its window of positions unwritten below the
/// tail while writes are in flight: an instance that looks for the tail only 1
/// position back reads past them, and writes past them, so they fall out of
/// every instance's window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteOnceBusVersion {
    /// `WriteOnceAgentBus`.
    #[default]
    V1,
    /// `WriteOnceAgentBusV2` with `SimpleSequencer`, looking for the tail as
    /// far back as `V2Windowed`. Under `enable_wobusv2_simple_bigtailwindow`.
    V2SimpleBigTailWindow,
    /// `WriteOnceAgentBusV2` with `WindowedSequencer`. Under
    /// `enable_wobusv2_windowed`.
    V2Windowed,
}

/// Takes the bus [`WriteOnceBusVersion::build`] builds, whose type depends on
/// the version.
pub trait WriteOnceBusConsumer {
    type Output;

    fn consume<B: AgentBus + 'static>(self, bus: B) -> Self::Output;
}

impl WriteOnceBusVersion {
    /// The version `flags` select.
    pub fn from_feature_flags(flags: &FeatureFlags) -> Self {
        if flags.enable_wobusv2_windowed {
            Self::V2Windowed
        } else if flags.enable_wobusv2_simple_bigtailwindow {
            Self::V2SimpleBigTailWindow
        } else {
            Self::V1
        }
    }

    /// Builds this version's bus over `space` and hands it to `consumer`.
    pub fn build<W, E, C>(
        self,
        space: W,
        environment: Rc<E>,
        blocking_poll_interval: Option<Duration>,
        consumer: C,
    ) -> C::Output
    where
        W: WriteOnceSpace + Clone + 'static,
        E: Environment + 'static,
        C: WriteOnceBusConsumer,
    {
        match self {
            Self::V1 => consumer.consume(WriteOnceAgentBus::new(
                space,
                environment,
                blocking_poll_interval,
            )),
            Self::V2SimpleBigTailWindow => consumer.consume(
                WriteOnceAgentBusV2::<_, _, SimpleSequencer>::new(
                    space,
                    environment,
                    blocking_poll_interval,
                )
                .with_tail_window_size(<WindowedSequencer as Sequencer<E>>::TAIL_WINDOW_SIZE),
            ),
            Self::V2Windowed => {
                consumer.consume(WriteOnceAgentBusV2::<_, _, WindowedSequencer>::new(
                    space,
                    environment,
                    blocking_poll_interval,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_follow_feature_flags() {
        let version = |enable_wobusv2_simple_bigtailwindow, enable_wobusv2_windowed| {
            WriteOnceBusVersion::from_feature_flags(&FeatureFlags {
                enable_wobusv2_simple_bigtailwindow,
                enable_wobusv2_windowed,
                ..Default::default()
            })
        };
        assert_eq!(version(false, false), WriteOnceBusVersion::V1);
        assert_eq!(
            version(true, false),
            WriteOnceBusVersion::V2SimpleBigTailWindow
        );
        assert_eq!(version(false, true), WriteOnceBusVersion::V2Windowed);
        assert_eq!(version(true, true), WriteOnceBusVersion::V2Windowed);
    }
}
