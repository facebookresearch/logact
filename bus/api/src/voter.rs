/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Voter trait — the policy interface for voting on intentions.

#[derive(Clone, Copy, Debug)]
pub struct VoterContext<'a> {
    pub bus_id: &'a str,
    pub intention: &'a str,
}

impl<'a> VoterContext<'a> {
    pub fn new(bus_id: &'a str, intention: &'a str) -> Self {
        Self { bus_id, intention }
    }
}

#[async_trait::async_trait(?Send)]
pub trait Voter {
    /// Evaluate an intention in the context of its bus.
    /// Returns `(is_safe, reason)` where `reason` is a human-readable
    /// explanation surfaced generically on the vote.
    async fn evaluate(&self, context: VoterContext<'_>) -> (bool, String);

    /// Apply a runtime policy update from the bus.
    /// Concrete implementations decide whether the `type_url` matches them.
    fn apply_policy(&mut self, config: &prost_types::Any);

    /// Human-readable description for logging.
    fn describe(&self) -> String;
}
