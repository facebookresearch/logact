/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;
use std::time::Duration;

use agentbus_api::Environment;
use agentbus_simulator::Simulator;

use crate::history::History;
use crate::history::OperationId;
use crate::operation::OperationResult;
use crate::worker::Worker;

#[test]
fn records_outputs_timing_and_tags() {
    let environment = Rc::new(Simulator::new(0));
    let history: Rc<History<i64, i64, usize>> = Rc::new(History::new());
    let worker = Worker::new(history.clone(), 0, vec![2, 3]);
    let worker_environment = environment.clone();
    let action_environment = environment.clone();
    let handle = environment.spawn(async move {
        worker
            .run(worker_environment, 1, move |id, operation| {
                let action_environment = action_environment.clone();
                async move {
                    action_environment.sleep(Duration::from_millis(2)).await;
                    OperationResult {
                        output: operation * 10,
                        tag: id.sequence,
                    }
                }
            })
            .await
    });

    environment.run();
    futures::executor::block_on(handle).expect("worker task should complete");

    let observations = history.observations();
    assert_eq!(observations.len(), 2);
    assert_eq!(
        observations[0].id,
        OperationId {
            client: 0,
            sequence: 0
        }
    );
    assert_eq!(observations[0].completed.operation, 2);
    assert_eq!(observations[0].completed.output, 20);
    assert_eq!(observations[0].completed.tag, 0);
    assert!(
        observations[0].end_time >= observations[0].start_time,
        "operation should not end before it starts"
    );
    assert_eq!(
        observations[1].id,
        OperationId {
            client: 0,
            sequence: 1
        }
    );
    assert_eq!(observations[1].completed.operation, 3);
    assert!(
        observations[1].start_time >= observations[0].end_time,
        "the second operation should start after the first one ends"
    );
}
