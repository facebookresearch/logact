/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An [`EligibilityGate`] that remembers its decisions.

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::time::Duration;

use async_trait::async_trait;
use lru::LruCache;

use super::EligibilityError;
use super::EligibilityGate;
use super::caller_unixname;
use crate::AuthorizationError;
use crate::Clock;
use crate::Environment;
use crate::MonotonicInstant;
use crate::RequestContext;
use crate::RequestGate;

#[derive(Clone, Copy)]
struct CachedDecision {
    eligible: bool,
    expires_at: MonotonicInstant,
}

/// Remembers an [`EligibilityGate`]'s allow and deny decisions by caller
/// unixname in a bounded LRU. A decision expires a fixed `ttl` after the request
/// that made it started, and hits do not extend it. Unavailable decisions are
/// not remembered, and exempt callers and callers without exactly one unixname
/// always reach the gate. Concurrent misses each reach the gate, and a decision
/// from an earlier request never replaces one from a later request. This is the
/// retention the [eligibility gate contract](super) permits.
pub struct CachingEligibilityGate<E> {
    inner: EligibilityGate<E>,
    decisions: RefCell<LruCache<String, CachedDecision>>,
    ttl: Duration,
}

impl<E> CachingEligibilityGate<E> {
    pub fn new(inner: EligibilityGate<E>, capacity: NonZeroUsize, ttl: Duration) -> Self {
        Self {
            inner,
            decisions: RefCell::new(LruCache::new(capacity)),
            ttl,
        }
    }
}

#[async_trait(?Send)]
impl<E: Environment> RequestGate for CachingEligibilityGate<E> {
    async fn admit(
        &self,
        ctx: &RequestContext,
        method: &'static str,
        agent_bus_id: &str,
    ) -> Result<(), AuthorizationError> {
        let unixname = match caller_unixname(ctx) {
            Ok(unixname) if !self.inner.is_exempt(ctx) => unixname,
            _ => return self.inner.admit(ctx, method, agent_bus_id).await,
        };
        let started = self.inner.env.with_clock(|clock| clock.monotonic_time());
        match self.cached(unixname, started) {
            Some(true) => return Ok(()),
            Some(false) => {
                let error = EligibilityError::Ineligible {
                    unixname: unixname.to_owned(),
                };
                return Err(self.inner.reject(ctx, method, agent_bus_id, error));
            }
            None => {}
        }

        let result = self.inner.admit(ctx, method, agent_bus_id).await;
        let eligible = match &result {
            Ok(()) => true,
            Err(AuthorizationError::Denied(_)) => false,
            Err(AuthorizationError::Unavailable(_)) => return result,
        };
        self.remember(unixname, eligible, started + self.ttl);
        result
    }
}

impl<E> CachingEligibilityGate<E> {
    fn cached(&self, unixname: &str, now: MonotonicInstant) -> Option<bool> {
        self.decisions
            .borrow_mut()
            .get(unixname)
            .filter(|decision| now < decision.expires_at)
            .map(|decision| decision.eligible)
    }

    fn remember(&self, unixname: &str, eligible: bool, expires_at: MonotonicInstant) {
        let mut decisions = self.decisions.borrow_mut();
        if decisions
            .peek(unixname)
            .is_none_or(|newest| newest.expires_at <= expires_at)
        {
            decisions.put(
                unixname.to_owned(),
                CachedDecision {
                    eligible,
                    expires_at,
                },
            );
        }
    }
}
