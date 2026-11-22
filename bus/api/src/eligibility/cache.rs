/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A [`RequestGate`] that remembers an eligibility gate's decisions.

use std::cell::Cell;
use std::cell::RefCell;
use std::fmt;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lru::LruCache;

use super::caller_unixname;
use super::warn_caller;
use crate::AuthorizationError;
use crate::Clock;
use crate::Environment;
use crate::MonotonicInstant;
use crate::RequestContext;
use crate::RequestGate;

/// The caller identity an eligibility gate decides on: its only unixname and
/// its service identities.
#[derive(Clone, Eq, Hash, PartialEq)]
struct Caller {
    unixname: String,
    /// Sorted and deduplicated, so the key does not depend on their order.
    services: Vec<String>,
}

impl Caller {
    fn of(ctx: &RequestContext) -> Option<Self> {
        let unixname = caller_unixname(ctx).ok()?.to_owned();
        let mut services = ctx.services.clone();
        services.sort_unstable();
        services.dedup();
        Some(Self { unixname, services })
    }
}

#[derive(Clone)]
struct CachedDecision {
    denial: Option<Arc<anyhow::Error>>,
    expires_at: MonotonicInstant,
}

/// A cached denial, displayed with the same message and sources as the
/// gate's original error.
#[derive(Debug)]
struct CachedDenial(Arc<anyhow::Error>);

impl fmt::Display for CachedDenial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for CachedDenial {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Remembers the allow and deny decisions of a gate that follows the
/// [eligibility gate contract](super), by caller unixname and service
/// identities, in a bounded LRU; a caller presenting several sets of service
/// identities takes one entry per set. A decision expires a fixed `ttl` after
/// the request that made it started, and hits do not extend it; a cached denial
/// repeats the gate's original error and is logged, rate-limited, as the
/// eligibility gate logs its denials. Unavailable decisions are not remembered,
/// and callers without exactly one unixname always reach the gate. Concurrent
/// misses each reach the gate, and a decision from an earlier request never
/// replaces one from a later request. This is the retention the contract
/// permits.
pub struct CachingRequestGate<G, E> {
    inner: G,
    env: Rc<E>,
    decisions: RefCell<LruCache<Caller, CachedDecision>>,
    ttl: Duration,
    last_warning: Cell<Option<MonotonicInstant>>,
}

impl<G, E> CachingRequestGate<G, E> {
    pub fn new(inner: G, env: Rc<E>, capacity: NonZeroUsize, ttl: Duration) -> Self {
        Self {
            inner,
            env,
            decisions: RefCell::new(LruCache::new(capacity)),
            ttl,
            last_warning: Cell::new(None),
        }
    }
}

#[async_trait(?Send)]
impl<G: RequestGate, E: Environment> RequestGate for CachingRequestGate<G, E> {
    async fn admit(
        &self,
        ctx: &RequestContext,
        method: &'static str,
        agent_bus_id: &str,
    ) -> Result<(), AuthorizationError> {
        let Some(caller) = Caller::of(ctx) else {
            return self.inner.admit(ctx, method, agent_bus_id).await;
        };
        let started = self.env.with_clock(|clock| clock.monotonic_time());
        if let Some(decision) = self.cached(&caller, started) {
            let Some(denial) = decision.denial else {
                return Ok(());
            };
            let denial = CachedDenial(denial);
            warn_caller(
                &self.last_warning,
                started,
                ctx,
                method,
                agent_bus_id,
                &denial,
                true,
            );
            return Err(AuthorizationError::Denied(denial.into()));
        }

        let result = self.inner.admit(ctx, method, agent_bus_id).await;
        let (result, denial) = match result {
            Ok(()) => (Ok(()), None),
            Err(AuthorizationError::Denied(error)) => {
                let denial = Arc::new(error);
                let error = CachedDenial(denial.clone()).into();
                (Err(AuthorizationError::Denied(error)), Some(denial))
            }
            Err(AuthorizationError::Unavailable(error)) => {
                return Err(AuthorizationError::Unavailable(error));
            }
        };
        self.remember(caller, denial, started + self.ttl);
        result
    }
}

impl<G, E> CachingRequestGate<G, E> {
    fn cached(&self, caller: &Caller, now: MonotonicInstant) -> Option<CachedDecision> {
        self.decisions
            .borrow_mut()
            .get(caller)
            .filter(|decision| now < decision.expires_at)
            .cloned()
    }

    fn remember(
        &self,
        caller: Caller,
        denial: Option<Arc<anyhow::Error>>,
        expires_at: MonotonicInstant,
    ) {
        let mut decisions = self.decisions.borrow_mut();
        if decisions
            .peek(&caller)
            .is_none_or(|newest| newest.expires_at <= expires_at)
        {
            decisions.put(caller, CachedDecision { denial, expires_at });
        }
    }
}
