/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Worker that performs counter operations

use super::counter_trait::Counter;
use super::linearizability_tracker::ExecutedCommand;
use super::test::Op;

pub struct CounterWorker<C: Counter> {
    counter: C,
    operations: Vec<Op>,
}

impl<C: Counter> CounterWorker<C> {
    pub fn new(counter: C, operations: Vec<Op>) -> Self {
        Self {
            counter,
            operations,
        }
    }

    pub async fn run_workload(&self) {
        for &op in &self.operations {
            let _ = match op {
                Op::Increment => self.counter.increment().await,
                Op::Decrement => self.counter.decrement().await,
                Op::Read => self.counter.read().await,
            };
        }
    }

    pub async fn get_command_history(&self) -> Vec<ExecutedCommand> {
        self.counter.get_command_history().await
    }
}
