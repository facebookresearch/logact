/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Eligibility gate cache scenarios with controlled lookup completion.

pub mod fixtures;
pub mod variants;

conformance::define_driver! {
    $,
    root = agentbus_tests,
    module = eligibility_cache,
    scenario_mods = [test_scenarios],
    fixtures = eligibility_cache_fixtures,
    suite = eligibility_cache,
}
