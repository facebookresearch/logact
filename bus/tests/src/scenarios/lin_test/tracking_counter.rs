/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A counter wrapper that automatically records operations for linearizability checking.

use std::cell::RefCell;
use std::rc::Rc;

use agentbus_api::Clock;
use agentbus_api::MonotonicInstant;
use agentbus_api::environment::Environment;

use super::counter_trait::CommandResult;
use super::counter_trait::Counter;
use super::linearizability_tracker::ExecutedCommand;
use super::linearizability_tracker::LinearizabilityTracker;
use super::linearizability_tracker::Operation;

pub struct TrackingCounter<C: Counter, E: Environment> {
    inner: C,
    env: Rc<E>,
    tracker: Rc<LinearizabilityTracker<i64>>,
    client_id: String,
    next_seq_no: RefCell<i64>,
}

impl<C: Counter, E: Environment> TrackingCounter<C, E> {
    pub fn new(
        inner: C,
        env: Rc<E>,
        tracker: Rc<LinearizabilityTracker<i64>>,
        client_id: String,
    ) -> Self {
        Self {
            inner,
            env,
            tracker,
            client_id,
            next_seq_no: RefCell::new(0),
        }
    }

    fn record(
        &self,
        operation: &str,
        read_only: bool,
        start_time: MonotonicInstant,
        result: &Result<CommandResult, String>,
    ) {
        let end_time = self.env.with_clock(|clock| clock.monotonic_time());
        if let Ok(cmd_result) = result {
            let mut seq = self.next_seq_no.borrow_mut();
            self.tracker.record(Operation {
                client_id: self.client_id.clone(),
                client_seq_no: *seq,
                log_position: cmd_result.log_position,
                start_time,
                end_time,
                operation: operation.to_string(),
                value: cmd_result.value,
                read_only,
            });
            *seq += 1;
        }
        //TODO: record and track errors as well
    }
}

impl<C: Counter, E: Environment> Counter for TrackingCounter<C, E> {
    async fn increment(&self) -> Result<CommandResult, String> {
        let start_time = self.env.with_clock(|clock| clock.monotonic_time());
        let result = self.inner.increment().await;
        self.record("inc", false, start_time, &result);
        result
    }

    async fn decrement(&self) -> Result<CommandResult, String> {
        let start_time = self.env.with_clock(|clock| clock.monotonic_time());
        let result = self.inner.decrement().await;
        self.record("dec", false, start_time, &result);
        result
    }

    async fn read(&self) -> Result<CommandResult, String> {
        let start_time = self.env.with_clock(|clock| clock.monotonic_time());
        let result = self.inner.read().await;
        self.record("read", true, start_time, &result);
        result
    }

    async fn get_command_history(&self) -> Vec<ExecutedCommand> {
        self.inner.get_command_history().await
    }
}
