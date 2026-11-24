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

/// The method refresh lookups pass to the inner gate, which decides the same
/// way for every method. An eligibility gate logs a refresh's denials and
/// failures under it, within the warning rate limit it shares with requests.
const REFRESH_METHOD: &str = "eligibility_cache_refresh";

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

    fn context(&self) -> RequestContext {
        RequestContext {
            users: vec![self.unixname.clone()],
            services: self.services.clone(),
        }
    }
}

struct CachedDecision {
    denial: Option<Arc<anyhow::Error>>,
    expires_at: MonotonicInstant,
}

/// Chooses which decisions a refresh sweep looks up, and in what order, from
/// the cache's decisions in scan order (most recently used first) when the
/// sweep starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshPolicy {
    /// Every candidate, in scan order.
    ScanOrder,
}

impl RefreshPolicy {
    fn order<'a, I>(self, candidates: I) -> impl Iterator<Item = I::Item>
    where
        I: Iterator<Item = (&'a Caller, &'a Rc<CachedDecision>)>,
    {
        match self {
            Self::ScanOrder => candidates,
        }
    }
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
///
/// With [`Self::with_refresh`], each request starts a refresh sweep by a
/// [`RefreshPolicy`] unless one is running. A refresh that decides replaces the
/// caller's decision and restarts its `ttl` from the refresh's start; an
/// unavailable one changes nothing, so the decision still expires on time.
/// Refreshes never add or promote decisions, skip an entry that expired before
/// its lookup, and leave an entry that was evicted or replaced before or during
/// its lookup unchanged.
pub struct CachingRequestGate<G, E> {
    cache: Rc<Cache<G, E>>,
    refresh: Option<RefreshPolicy>,
    sweeping: Rc<Cell<bool>>,
}

struct Cache<G, E> {
    inner: G,
    env: Rc<E>,
    decisions: RefCell<LruCache<Caller, Rc<CachedDecision>>>,
    ttl: Duration,
    last_warning: Cell<Option<MonotonicInstant>>,
}

impl<G, E> CachingRequestGate<G, E> {
    pub fn new(inner: G, env: Rc<E>, capacity: NonZeroUsize, ttl: Duration) -> Self {
        Self {
            cache: Rc::new(Cache::new(inner, env, capacity, ttl)),
            refresh: None,
            sweeping: Rc::new(Cell::new(false)),
        }
    }
}

#[async_trait(?Send)]
impl<G: RequestGate + 'static, E: Environment + 'static> RequestGate for CachingRequestGate<G, E> {
    async fn admit(
        &self,
        ctx: &RequestContext,
        method: &'static str,
        agent_bus_id: &str,
    ) -> Result<(), AuthorizationError> {
        self.start_sweep();
        self.cache.admit(ctx, method, agent_bus_id).await
    }
}

impl<G, E> CachingRequestGate<G, E> {
    /// Lets each request start a refresh sweep by `policy` unless one is
    /// running.
    pub fn with_refresh(mut self, policy: RefreshPolicy) -> Self {
        self.refresh = Some(policy);
        self
    }
}

impl<G: RequestGate + 'static, E: Environment + 'static> CachingRequestGate<G, E> {
    fn start_sweep(&self) {
        let Some(policy) = self.refresh else {
            return;
        };
        if self.sweeping.replace(true) {
            return;
        }
        let cache = self.cache.clone();
        let sweeping = self.sweeping.clone();
        self.cache.env.spawn_local(async move {
            cache.sweep(policy).await;
            sweeping.set(false);
        });
    }
}

impl<G, E> Cache<G, E> {
    fn new(inner: G, env: Rc<E>, capacity: NonZeroUsize, ttl: Duration) -> Self {
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
impl<G: RequestGate, E: Environment> RequestGate for Cache<G, E> {
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
            let Some(denial) = decision.denial.clone() else {
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

impl<G: RequestGate + 'static, E: Environment + 'static> Cache<G, E> {
    /// Snapshots the decisions `policy` chooses and refreshes them one at a
    /// time.
    async fn sweep(&self, policy: RefreshPolicy) {
        let snapshot: Vec<_> = policy
            .order(self.decisions.borrow().iter())
            .map(|(caller, decision)| (caller.clone(), decision.clone()))
            .collect();
        for (caller, expected) in snapshot {
            self.refresh(&caller, &expected).await;
        }
    }

    /// Looks `caller` up again if `expected` has not expired and, if `expected`
    /// is still its entry after the lookup, replaces it with the lookup's
    /// decision and a `ttl` from the lookup's start.
    async fn refresh(&self, caller: &Caller, expected: &Rc<CachedDecision>) {
        let now = self.env.with_clock(|clock| clock.monotonic_time());
        if expected.expires_at <= now {
            return;
        }
        let denial = match self
            .inner
            .admit(&caller.context(), REFRESH_METHOD, "")
            .await
        {
            Ok(()) => None,
            Err(AuthorizationError::Denied(error)) => Some(Arc::new(error)),
            Err(AuthorizationError::Unavailable(_)) => return,
        };
        if let Some(decision) = self.decisions.borrow_mut().peek_mut(caller)
            && Rc::ptr_eq(decision, expected)
        {
            *decision = Rc::new(CachedDecision {
                denial,
                expires_at: now + self.ttl,
            });
        }
    }
}

impl<G, E> Cache<G, E> {
    fn cached(&self, caller: &Caller, now: MonotonicInstant) -> Option<Rc<CachedDecision>> {
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
            decisions.put(caller, Rc::new(CachedDecision { denial, expires_at }));
        }
    }
}
