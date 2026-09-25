/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic test suite for LogAct `CommitSvc` implementations.
//!
//! The places you edit to add coverage are the scenario files under `scenarios/`
//! (what to test), `fixtures.rs` (the implementations), and `variants.rs` (which
//! concrete implementations to run). Everything else is one-time scaffolding: this
//! crate root, the `define_driver!` invocation below (which generates the whole
//! driver — including the `scenarios` module tree — from those inputs), plus
//! `sim_tests.rs` and `integration_tests.rs`, the Buck test entry points.

pub mod fixtures;
pub mod variants;

pub use agentbus_simulator as simulator;

// Generates CommitSvc's whole driver from the shared template — the `scenarios`
// module tree, the scenario combiner, the per-test codegen, the
// environment-inclusion table, the per-fixture fan-out, and the suite entry point
// (`commit_service_sim_suite!` and `commit_service_integration_suite!`, called by
// the test targets). Its inputs are the
// scenario files (`scenario_mods`) and the fixture registry
// (`commit_service_fixtures!` in `variants.rs`).
conformance::define_driver! {
    // Plumbing: a literal `$`, so the generated macros can carry their own
    // metavariables (`$name`, `$fix`, …).
    $,
    // Inputs — where this suite's scenarios and fixtures live.
    root = logact_commit_service_tests,
    scenario_mods = [test_scenarios, lin_test, policy_register, static_policy, replay_backlog], // files under `scenarios/`
    fixtures = commit_service_fixtures,         // fixture registry (variants.rs)
    // Suite name prefix — the driver derives every generated macro from it
    // (`commit_service_sim_suite!` and `commit_service_integration_suite!`, plus
    // the internal fan-out steps).
    suite = commit_service,
}
