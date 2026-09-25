/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixtures for the generic applicator conformance suite.

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Header;
use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::environment::Environment;
use logact_commit_service_engine::Applicator;

pub mod decider;
pub mod stateless_voter_adapter;

pub use decider::DeciderApplicatorFixture;
pub use stateless_voter_adapter::StatelessVoterAdapterFixture;

/// Builds a fresh applicator and the entries it acts on.
pub trait ApplicatorTestFixture: Sized {
    type Env: Environment + 'static;
    type Impl: Applicator;

    fn get_env(&self) -> Rc<Self::Env>;
    fn create_impl(&self) -> Self::Impl;
    /// Build an entry the applicator acts on (i.e. one that advances its applied
    /// position), placed at `position`.
    fn make_entry(&self, position: i64) -> BusEntry;
}

/// Helper for fixtures whose applicator acts on string intentions.
pub fn intention_entry(position: i64) -> BusEntry {
    BusEntry {
        header: Some(Header {
            log_position: position,
            ..Default::default()
        }),
        payload: Some(Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention("test".to_string())),
                ..Default::default()
            })),
        }),
    }
}
