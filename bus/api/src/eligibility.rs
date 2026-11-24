/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Caller eligibility checks for services behind a channel boundary.
//!
//! # Eligibility gate contract
//!
//! An eligibility gate is a [`RequestGate`] that admits a caller by the
//! eligibility of its unixname. [`EligibilityGate`] implements this contract;
//! other gates, such as [`ExtremelyUnsafeNoOpGate`], apply other policies. An
//! eligibility gate:
//!
//! - Admits a caller with any exempt service identity, without an eligibility
//!   lookup.
//! - Otherwise needs exactly one user identity that is nonempty and not all
//!   digits; empty users and numeric user IDs are ignored. A caller with no
//!   such identity, or with more than one (including the same unixname twice),
//!   is denied without a lookup.
//! - Looks up that unixname: an eligible caller is admitted, an ineligible one
//!   is denied, and a failed lookup is unavailable, with the lookup's error as
//!   its source.
//! - Decides the same way for every method and bus.
//! - May retain allow and deny decisions for a fixed TTL measured from the
//!   start of the lookup that produced them, by a request or a background
//!   refresh. Serving a retained decision does not extend it; a refresh that
//!   decides replaces it and restarts its TTL. Unavailable results are never
//!   retained and never replace a retained decision. Without retention, a
//!   changed eligibility applies to the next request.
//!
//! [`ExtremelyUnsafeNoOpGate`]: crate::ExtremelyUnsafeNoOpGate

use std::cell::Cell;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use async_trait::async_trait;

use crate::AuthorizationError;
use crate::Clock;
use crate::Environment;
use crate::MonotonicInstant;
use crate::RequestContext;
use crate::RequestGate;

mod cache;

pub use self::cache::CachingRequestGate;
pub use self::cache::RefreshPolicy;

const WARNING_INTERVAL: Duration = Duration::from_secs(5);

type LookupFuture = Pin<Box<dyn Future<Output = anyhow::Result<bool>>>>;
pub(crate) type Lookup = Rc<dyn Fn(String) -> LookupFuture>;

/// Why a caller failed the eligibility check.
#[derive(Debug, thiserror::Error)]
pub enum EligibilityError {
    #[error("caller has no authenticated user unixname for eligibility checking")]
    MissingUnixname,
    #[error("caller has multiple authenticated user unixnames for eligibility checking")]
    AmbiguousUnixnames,
    #[error("caller `{unixname}` fails eligibility check for LogAct")]
    Ineligible { unixname: String },
    #[error("eligibility check unavailable for caller `{unixname}`")]
    Unavailable {
        unixname: String,
        #[source]
        source: anyhow::Error,
    },
}

/// Checks every request's caller and rejects failed eligibility checks.
///
/// A caller with an exempt service identity skips the check. Otherwise the
/// caller must have exactly one authenticated unixname, which the
/// lookup callback must accept.
pub struct EligibilityGate<E> {
    lookup: Lookup,
    env: Rc<E>,
    exempt_services: HashSet<String>,
    last_warning: Cell<Option<MonotonicInstant>>,
}

impl<E> EligibilityGate<E> {
    pub fn new<F, Fut>(lookup: F, env: Rc<E>) -> Self
    where
        F: Fn(String) -> Fut + 'static,
        Fut: Future<Output = anyhow::Result<bool>> + 'static,
    {
        Self {
            lookup: Rc::new(move |unixname| Box::pin(lookup(unixname))),
            env,
            exempt_services: HashSet::new(),
            last_warning: Cell::new(None),
        }
    }

    /// Skip the check for callers with any of these service identities.
    pub fn with_exempt_services(mut self, services: impl IntoIterator<Item = String>) -> Self {
        self.exempt_services = services.into_iter().collect();
        self
    }
}

#[async_trait(?Send)]
impl<E: Environment> RequestGate for EligibilityGate<E> {
    async fn admit(
        &self,
        ctx: &RequestContext,
        method: &'static str,
        agent_bus_id: &str,
    ) -> Result<(), AuthorizationError> {
        let Err(error) = self.check(ctx).await else {
            return Ok(());
        };
        Err(self.reject(ctx, method, agent_bus_id, error))
    }
}

impl<E: Environment> EligibilityGate<E> {
    fn is_exempt(&self, ctx: &RequestContext) -> bool {
        ctx.services
            .iter()
            .any(|service| self.exempt_services.contains(service))
    }

    async fn check(&self, ctx: &RequestContext) -> Result<(), EligibilityError> {
        if self.is_exempt(ctx) {
            return Ok(());
        }
        let unixname = caller_unixname(ctx)?;
        match (self.lookup)(unixname.to_owned()).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(EligibilityError::Ineligible {
                unixname: unixname.to_owned(),
            }),
            Err(source) => Err(EligibilityError::Unavailable {
                unixname: unixname.to_owned(),
                source,
            }),
        }
    }

    fn reject(
        &self,
        ctx: &RequestContext,
        method: &str,
        agent_bus_id: &str,
        error: EligibilityError,
    ) -> AuthorizationError {
        self.warn(ctx, method, agent_bus_id, &error);
        match error {
            EligibilityError::Unavailable { .. } => AuthorizationError::Unavailable(error.into()),
            EligibilityError::MissingUnixname
            | EligibilityError::AmbiguousUnixnames
            | EligibilityError::Ineligible { .. } => AuthorizationError::Denied(error.into()),
        }
    }

    fn warn(
        &self,
        ctx: &RequestContext,
        method: &str,
        agent_bus_id: &str,
        error: &EligibilityError,
    ) {
        let now = self.env.with_clock(|clock| clock.monotonic_time());
        warn_caller(
            &self.last_warning,
            now,
            ctx,
            method,
            agent_bus_id,
            error,
            false,
        );
    }
}

/// Logs a failed eligibility check, at most once per `WARNING_INTERVAL` per
/// `last_warning`.
fn warn_caller(
    last_warning: &Cell<Option<MonotonicInstant>>,
    now: MonotonicInstant,
    ctx: &RequestContext,
    method: &str,
    agent_bus_id: &str,
    error: &dyn std::fmt::Debug,
    cached: bool,
) {
    if last_warning
        .get()
        .is_some_and(|last| now.duration_since(last) < WARNING_INTERVAL)
    {
        return;
    }
    last_warning.set(Some(now));
    tracing::warn!(
        method,
        agent_bus_id,
        users = ?ctx.users,
        services = ?ctx.services,
        ?error,
        cached,
        "caller eligibility warning"
    );
}

/// The caller's only unixname, ignoring numeric user IDs.
fn caller_unixname(ctx: &RequestContext) -> Result<&str, EligibilityError> {
    let mut unixnames = ctx
        .users
        .iter()
        .filter(|user| !user.is_empty() && !user.bytes().all(|byte| byte.is_ascii_digit()));
    let unixname = unixnames.next().ok_or(EligibilityError::MissingUnixname)?;
    if unixnames.next().is_some() {
        return Err(EligibilityError::AmbiguousUnixnames);
    }
    Ok(unixname)
}
