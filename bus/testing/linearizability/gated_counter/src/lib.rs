/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub mod agentbus;
pub mod commit_service;
pub mod fixtures;
pub mod model;
pub mod variants;

pub use futures;

conformance::define_driver! {
    $,
    root = gated_counter_suite,
    scenario_mods = [gated_counter],
    fixtures = gated_counter_fixtures,
    suite = gated_counter,
}
