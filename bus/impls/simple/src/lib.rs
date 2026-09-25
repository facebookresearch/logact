/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Simple in-memory implementation of the AgentBus.
//!
//! This is meant for testing and demo purposes.
//! Notably, it's a reference implementation for the AgentBus spec.
//! It uses a shared in-memory object to store backing state.
//! It is not meant for distributed / production use.

mod in_memory_agentbus;
mod in_memory_agentbus_state;

pub use in_memory_agentbus::InMemoryAgentBus;
pub use in_memory_agentbus_state::InMemoryAgentBusState;
