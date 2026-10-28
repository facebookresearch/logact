/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Linearizability testing framework for counter operations

#[path = "lin_test/linearizability_tracker.rs"]
pub mod linearizability_tracker;
#[path = "lin_test/tracking_counter.rs"]
pub mod tracking_counter;

#[path = "lin_test/counter_impl.rs"]
pub mod counter_impl;
#[path = "lin_test/counter_trait.rs"]
pub mod counter_trait;
#[path = "lin_test/counter_worker.rs"]
pub mod counter_worker;
#[path = "lin_test/random_voter.rs"]
pub mod random_voter;
#[path = "lin_test/test.rs"]
pub mod test;
