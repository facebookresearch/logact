/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AgentBus test utilities.
//!
//! Every AgentBus test runs through `define_driver!` (below) over the `variants.rs`
//! fixture registry — there are no hand-written exceptions. Beyond the standard
//! `#[scenario]` fan-out:
//! - failure injection — `#[scenario_for]` scenarios pinning the `FaultFixture<Base,
//!   Mix>` fixtures (`scenarios/fault_injection.rs`);
//! - buggy-linearizability — a seeded `#[scenario_for]` in `scenarios/lin_test`;
//! - multi-node determinism — a `#[scenario(sim_only, cardinality = 2)]` in
//!   `scenarios/multi_node.rs`, which `sim_determinism_test!` runs twice at one seed
//!   and compares (so it returns the committed-log string, not `Result<()>`).
//!
//! ([`fault_injection_helpers`] holds the assertion logic the fault scenarios call.)

pub mod common;
pub mod conditional_write_space;
pub mod fault_injection_helpers;
pub mod fixtures;
// Fault-injection fixture types, re-exported at the crate root so `#[scenario_for]`
// pins in `scenarios/fault_injection.rs` can name them as `agentbus_tests::FaultFixture`
// etc. rather than the full `agentbus_tests::fixtures::simtest::` path.
pub use fixtures::simtest::BusChained;
pub use fixtures::simtest::BusChanneled;
pub use fixtures::simtest::BusSimpleMemory;
pub use fixtures::simtest::CommitThenError;
pub use fixtures::simtest::ErrorThenCommit;
pub use fixtures::simtest::FaultFixture;
pub use fixtures::simtest::Lost;
pub use fixtures::simtest::Mixed;
pub use fixtures::simtest::SpaceChanneled;
pub use fixtures::simtest::SpaceInMemory;
pub use fixtures::simtest::SpaceTransactionConflictEmpty;
pub use fixtures::simtest::SpaceTransactionConflictOccupied;
pub mod impls;
pub mod variants;
pub use agentbus_simulator as simulator;
pub mod tailable_space;
pub mod write_once_space;

#[cfg(test)]
mod observable;

pub use futures;

// Generate the AgentBus driver from the shared template. AgentBus is the crate's
// top-level suite, so its scenarios live at `agentbus_tests::scenarios` (the
// `scenarios` module tree is generated here from `scenario_mods`). Faults are
// injected via fixture *types* (`FaultFixture<Base, Mix>`); the multi-node
// determinism guard is a `#[scenario(sim_only, cardinality = 2)]` (run twice at one
// seed and compared). Nothing is hand-written outside the framework.
conformance::define_driver! {
    $,
    root = agentbus_tests,
    scenario_mods = [test_scenarios, appserver, mailbox, voter_e2e, sim_only_scenarios, lin_test, multi_node, fault_injection],
    fixtures = agentbus_fixtures,
    suite = agentbus,
}
