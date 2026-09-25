/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TailableSpace test utilities

pub mod fixtures;
pub mod variants;

// Generate the TailableSpace driver from the shared template. Tailable is a module
// of this crate (not its own crate), so `module` points the generated codegen at
// `agentbus_tests::tailable_space::scenarios` (the `scenarios` module tree is itself
// generated from `scenario_mods`). This generates both suite entry points plus both
// emit tables; the `fb`/`oss` integration crates can additionally drive the
// integration emitter with their own backend fixtures.
conformance::define_driver! {
    $,
    root = agentbus_tests,
    module = tailable_space,
    scenario_mods = [test_scenarios],
    fixtures = tailable_space_fixtures,
    suite = tailable_space,
}
