/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Conditional write space test utilities

pub mod fixtures;
pub mod variants;

// Generate the ConditionalWriteSpace driver from the shared template. CWS is a
// module of this crate (not its own crate), so `module` points the generated codegen
// at `agentbus_tests::conditional_write_space::scenarios` (the `scenarios` module
// tree is itself generated from `scenario_mods`). This generates both suite entry
// points plus both emit tables; the `fb`/`oss` integration crates can additionally
// drive the integration emitter with their own backend fixtures. Concurrency
// scenarios are split into sim-only (deterministic `Simulator::spawn`) and
// integration (`LocalSet`) variants.
conformance::define_driver! {
    $,
    root = agentbus_tests,
    module = conditional_write_space,
    scenario_mods = [test_scenarios],
    fixtures = conditional_write_space_fixtures,
    suite = conditional_write_space,
}
