/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Assigns append positions for a bus over a write-once space.

use std::rc::Rc;

use agentbus_api::environment::Environment;

/// Assigns append positions for a bus.
#[expect(
    async_fn_in_trait,
    reason = "sequencers run on a single-threaded runtime and do not require Send futures"
)]
pub trait Sequencer<E: Environment> {
    type Claim<'a>: Claim
    where
        Self: 'a;

    /// Creates a sequencer for a bus whose positions before `next` are written.
    fn new(next: u64, environment: Rc<E>) -> Self;

    /// How many positions past the written prefix may be written, so how far
    /// back the tail must be looked for.
    const TAIL_WINDOW_SIZE: u64;

    /// Waits for a position to append at and claims it.
    async fn claim(&self) -> Self::Claim<'_>;

    /// Moves past `tail`, below which every position is written.
    fn advance(&self, tail: u64);
}

/// A claimed position. Dropping it before `set`,
/// such as when its append is cancelled, records the position as unwritten.
pub trait Claim {
    fn position(&self) -> u64;

    /// Records whether the position is written.
    fn set(self, written: bool);
}
