/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::rc::Rc;

use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_simulator::Simulator;
use rand::RngExt as _;

use super::history::CompletedOperation;
use super::history::History;
use super::history::Observation;
use super::history::OperationId;
use super::operation::OperationResult;

/// Executes one client's commands and records their observed results.
pub struct Worker<Operation, Output, Tag> {
    history: Rc<History<Operation, Output, Tag>>,
    client: usize,
    operations: Vec<Operation>,
}

impl<Operation, Output, Tag> Worker<Operation, Output, Tag>
where
    Operation: Clone,
{
    /// Create a worker for one ordered sequence of client commands.
    pub fn new(
        history: Rc<History<Operation, Output, Tag>>,
        client: usize,
        operations: Vec<Operation>,
    ) -> Self {
        Self {
            history,
            client,
            operations,
        }
    }

    /// Execute every command, adding randomized simulator delay between operations.
    pub async fn run<Action, ActionFuture>(
        self,
        environment: Rc<Simulator>,
        max_delay_ms: u64,
        action: Action,
    ) where
        Action: Fn(OperationId, Operation) -> ActionFuture,
        ActionFuture: Future<Output = OperationResult<Output, Tag>>,
    {
        for (sequence, operation) in self.operations.into_iter().enumerate() {
            if max_delay_ms > 0 {
                let delay = environment.with_rng(|rng| rng.random_range(0..=max_delay_ms));
                environment
                    .sleep(std::time::Duration::from_millis(delay))
                    .await;
            }
            let id = OperationId {
                client: self.client,
                sequence,
            };
            let start_time = environment.with_clock(|clock| clock.monotonic_time());
            let result = action(id, operation.clone()).await;
            let end_time = environment.with_clock(|clock| clock.monotonic_time());
            self.history.record(Observation {
                id,
                start_time,
                end_time,
                completed: CompletedOperation {
                    operation,
                    output: result.output,
                    tag: result.tag,
                },
            });
        }
    }
}
