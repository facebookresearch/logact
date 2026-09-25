/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Storage conformance test crate.
//!
//! The places you edit to add coverage are the scenario files under `scenarios/`
//! (what to test), `fixtures.rs` (the implementations), and `variants.rs` (which
//! concrete implementations to run). Everything else is one-time scaffolding: this
//! crate root, the `define_driver!` invocation below (which generates the whole
//! driver — including the `scenarios` module tree — from those inputs), and one
//! entry file per Buck test target — `sim_tests.rs` (simulator) and
//! `integration_tests.rs` (real backends), kept separate because integration needs
//! `fbinit` and live backends.

pub mod applicator;
pub mod fixtures;
pub mod variants;
pub mod voters;

pub use agentbus_simulator::Simulator;

// Generates Storage's whole driver from the shared template — the `scenarios`
// module tree, the scenario combiner, the per-test codegen, the
// environment-inclusion table, the per-fixture fan-out, and the suite entry points
// (`storage_sim_suite!` / `storage_integration_suite!`, called by the test
// targets). Its inputs are the scenario files (`scenario_mods`) and the fixture
// registry (`storage_fixtures!` in `variants.rs`).
conformance::define_driver! {
    // Plumbing: a literal `$`, so the generated macros can carry their own
    // metavariables (`$name`, `$fix`, …).
    $,
    // Inputs — where this suite's scenarios and fixtures live.
    root = logact_commit_service_engine_tests, // this crate, by name (not `$crate`)
    scenario_mods = [test_scenarios, fault_injection_scenarios], // files under `scenarios/`
    fixtures = storage_fixtures,               // fixture registry (variants.rs)
    // Suite name prefix. The driver derives every generated macro from it —
    // `storage_sim_suite!` / `storage_integration_suite!` (the entry points the
    // test targets call) plus the internal fan-out steps.
    suite = storage,
}
