/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RetryingRequestGate: retries a RequestGate's unavailable admissions.

use std::rc::Rc;

use agentbus_api::AuthorizationError;
use agentbus_api::Environment;
use agentbus_api::RequestContext;
use agentbus_api::RequestGate;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::retry;
use async_trait::async_trait;

/// Retries `admit` on `Unavailable`; a denial is final.
pub struct RetryingRequestGate<G, E> {
    inner: G,
    environment: Rc<E>,
    config: RetryConfig,
}

impl<G, E> RetryingRequestGate<G, E> {
    pub fn new(inner: G, environment: Rc<E>, config: RetryConfig) -> Self {
        Self {
            inner,
            environment,
            config,
        }
    }
}

fn decide(error: &AuthorizationError) -> RetryDecision {
    match error {
        AuthorizationError::Unavailable(_) => RetryDecision::RetryWithBackoff,
        AuthorizationError::Denied(_) => RetryDecision::Stop,
    }
}

#[async_trait(?Send)]
impl<G: RequestGate, E: Environment> RequestGate for RetryingRequestGate<G, E> {
    async fn admit(
        &self,
        ctx: &RequestContext,
        method: &'static str,
        agent_bus_id: &str,
    ) -> Result<(), AuthorizationError> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.admit(ctx, method, agent_bus_id),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }
}
