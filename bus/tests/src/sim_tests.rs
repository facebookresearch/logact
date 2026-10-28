/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Consolidated conformance test invocations for the AgentBus suite family.

use agentbus_tests::agentbus_integration_suite;
use agentbus_tests::agentbus_sim_suite;
use agentbus_tests::conditional_write_space_integration_suite;
use agentbus_tests::conditional_write_space_sim_suite;
use agentbus_tests::tailable_space_integration_suite;
use agentbus_tests::tailable_space_sim_suite;
use agentbus_tests::write_once_space_integration_suite;
use agentbus_tests::write_once_space_sim_suite;

#[rustfmt::skip]
mod tests {
use super::*;

// =============================================================================
// AgentBus tests
// =============================================================================

// Every AgentBus test in this crate is emitted by `agentbus_sim_suite!`. Counter
// linearizability coverage lives in the shared linearizability Counter suite.
agentbus_sim_suite!();

// End-to-end tests over the in-process transport fixture, driven by the generated
// integration suite (the `integration`-tagged registry entry).
agentbus_integration_suite!();

// =============================================================================
// WriteOnceSpace tests
// =============================================================================

write_once_space_sim_suite!();
write_once_space_integration_suite!();

// =============================================================================
// ConditionalWriteSpace tests
// =============================================================================

conditional_write_space_sim_suite!();
conditional_write_space_integration_suite!();

// =============================================================================
// TailableSpace tests
// =============================================================================

tailable_space_sim_suite!();
tailable_space_integration_suite!();
}
