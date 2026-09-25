/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! WriteOnceSpace test utilities

pub mod fixtures;
pub mod variants;

// Generate the WriteOnceSpace driver from the shared template. WOS is a module of
// this crate (not its own crate), so `module` points the generated codegen at
// `agentbus_tests::write_once_space::scenarios` (the `scenarios` module tree is
// itself generated from `scenario_mods`). This generates both suite entry points
// plus both emit tables; the `fb`/`oss` integration crates can additionally drive
// the integration emitter with their own backend fixtures.
conformance::define_driver! {
    $,
    root = agentbus_tests,
    module = write_once_space,
    scenario_mods = [test_scenarios],
    fixtures = write_once_space_fixtures,
    suite = write_once_space,
}
