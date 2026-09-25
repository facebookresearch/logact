/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Linearizability testing framework for counter operations

pub mod linearizability_tracker;
pub mod tracking_counter;

pub mod counter_impl;
pub mod counter_trait;
pub mod counter_worker;
pub mod random_voter;
pub mod test;

// Surface the scenario functions (and the buggy-test helpers) at the `lin_test`
// module root so the parent `scenarios` module can flatten them with a single glob.
pub use test::*;
