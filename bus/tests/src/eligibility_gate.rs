/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Eligibility `RequestGate` conformance for admission and lookup behavior.

pub mod fixtures;
pub mod variants;

conformance::define_driver! {
    $,
    root = agentbus_tests,
    module = eligibility_gate,
    scenario_mods = [admission, lookups],
    fixtures = eligibility_gate_fixtures,
    suite = eligibility_gate,
}
