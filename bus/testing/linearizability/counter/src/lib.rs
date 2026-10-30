/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub mod agentbus;
pub mod commit_service;
pub mod counter;
pub mod fixtures;
pub mod variants;

pub use futures;

conformance::define_driver! {
    $,
    root = counter_suite,
    scenario_mods = [common],
    fixtures = counter_fixtures,
    suite = counter,
}
