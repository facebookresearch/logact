/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `PolicyRegister` conformance test crate.
//!
//! Scenarios live under `scenarios/`, implementations in `fixtures.rs`, and the
//! registry of concrete implementations in `variants.rs`. `sim_tests.rs` and
//! `integration_tests.rs` are the test-target entry points.

pub mod fixtures;
pub mod variants;

conformance::define_driver! {
    $,
    root = logact_commit_service_policy_register_tests,
    scenario_mods = [test_scenarios],
    fixtures = policy_register_fixtures,
    suite = policy_register,
}
