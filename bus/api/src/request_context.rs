/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use async_trait::async_trait;

/// Caller identities authenticated by the transport that received a request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestContext {
    /// Authenticated `USER` identities, e.g. unixnames or numeric user IDs.
    pub users: Vec<String>,
    /// Authenticated `SERVICE_IDENTITY` identities.
    pub services: Vec<String>,
}

impl RequestContext {
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.users.push(user.into());
        self
    }

    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.services.push(service.into());
        self
    }
}

/// Why a [`RequestGate`] rejected a request.
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    /// The caller is not authorized to make the request.
    #[error(transparent)]
    Denied(anyhow::Error),
    /// The caller's authorization could not be checked.
    #[error(transparent)]
    Unavailable(anyhow::Error),
}

/// Admits or rejects a request that entered a channel with a caller's
/// context, before the request runs.
#[async_trait(?Send)]
pub trait RequestGate {
    async fn admit(
        &self,
        ctx: &RequestContext,
        method: &'static str,
        agent_bus_id: &str,
    ) -> Result<(), AuthorizationError>;
}

/// Admits every request without checking the caller.
pub struct ExtremelyUnsafeNoOpGate;

#[async_trait(?Send)]
impl RequestGate for ExtremelyUnsafeNoOpGate {
    async fn admit(
        &self,
        _: &RequestContext,
        _: &'static str,
        _: &str,
    ) -> Result<(), AuthorizationError> {
        Ok(())
    }
}
