/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Counter API trait for linearizability testing

use super::linearizability_tracker::ExecutedCommand;

/// Result of a counter operation.
#[derive(Clone, Debug)]
pub struct CommandResult {
    /// The log position where the command is executed.
    pub log_position: i64,
    pub value: i64,
}

/// A simple counter API for testing linearizability.
/// Both operations are atomic and return the intention ID and new value.
#[allow(async_fn_in_trait)]
pub trait Counter {
    /// Increment the counter by 1 and return the intention ID and new value.
    async fn increment(&self) -> Result<CommandResult, String>;

    /// Decrement the counter by 1 and return the intention ID and new value.
    async fn decrement(&self) -> Result<CommandResult, String>;

    /// Read the current counter value.
    async fn read(&self) -> Result<CommandResult, String>;

    /// Get the history of executed commands in linearization order.
    async fn get_command_history(&self) -> Vec<ExecutedCommand>;
}
