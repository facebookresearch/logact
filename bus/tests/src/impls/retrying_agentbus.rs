/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RetryingAgentBus: retries the reads of an AgentBus.

use std::rc::Rc;

use agentbus_api::Environment;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::retry;
use agentbus_api::traits::*;

/// Retries `poll`, `read_next`, `check_tail` and `blocking_poll` on `Timeout`
/// and `Unavailable`.
///
/// `append` is not idempotent, so it is never retried: a failed append may or
/// may not have landed (at most once), and its caller sees the error.
pub struct RetryingAgentBus<B, E> {
    inner: B,
    environment: Rc<E>,
    config: RetryConfig,
}

impl<B: Clone, E> Clone for RetryingAgentBus<B, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            environment: self.environment.clone(),
            config: self.config,
        }
    }
}

impl<B, E> RetryingAgentBus<B, E> {
    pub fn new(inner: B, environment: Rc<E>, config: RetryConfig) -> Self {
        Self {
            inner,
            environment,
            config,
        }
    }
}

fn decide(error: &AgentBusError) -> RetryDecision {
    match error {
        AgentBusError::Timeout(_) | AgentBusError::Unavailable(_) => {
            RetryDecision::RetryWithBackoff
        }
        AgentBusError::PositionNotFound(_)
        | AgentBusError::InvalidArgument(_)
        | AgentBusError::Internal(_) => RetryDecision::Stop,
    }
}

impl<B: AgentBus, E: Environment> AgentBus for RetryingAgentBus<B, E> {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        self.inner.append(request).await
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.poll(request.clone()),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.read_next(request.clone()),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.check_tail(request.clone()),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.blocking_poll(request.clone()),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }
}
