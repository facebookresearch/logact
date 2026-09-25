/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Persisted runtime state for the LogAct decider.

use agent_bus_proto_rust::agent_bus::Payload;
use prost_types::Any;

/// Persisted per-bus runtime state. Tag 1, formerly the policy, is retired.
#[derive(Clone, PartialEq, prost::Message)]
pub struct DeciderState {
    /// Last result, retained for idempotent replay. The storage slot carries its
    /// position.
    #[prost(message, optional, tag = "2")]
    pub last_payload: Option<Payload>,
    /// Policy-specific state; empty for stateless policies.
    #[prost(message, optional, tag = "3")]
    pub policy_state: Option<Any>,
}
