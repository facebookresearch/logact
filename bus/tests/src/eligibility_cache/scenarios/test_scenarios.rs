/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(eligibility_cache_test_scenarios_list)]
mod defs {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use agentbus_api::AuthorizationError;
    use agentbus_api::Environment;
    use agentbus_api::RefreshPolicy;
    use agentbus_api::RequestContext;
    use agentbus_api::RequestGate;
    use anyhow::Result;
    use futures::FutureExt as _;

    use crate::eligibility::EXEMPT_SERVICE;
    use crate::eligibility::gate_decision;
    use crate::eligibility_cache::fixtures::ControlledEligibilityLookup;
    use crate::eligibility_cache::fixtures::EligibilityCacheTestFixture;

    const CAPACITY: NonZeroUsize = NonZeroUsize::new(2).unwrap();
    const TTL: Duration = Duration::from_secs(300);
    const POLICY: RefreshPolicy = RefreshPolicy::ScanOrder;
    const SETTLE: Duration = Duration::from_secs(1);

    #[derive(Debug, thiserror::Error)]
    #[error("GK unavailable")]
    struct TypedLookupError;

    fn cached(service: &impl RequestGate, unixname: &str) -> Result<bool> {
        gate_decision(service, unixname)
            .now_or_never()
            .unwrap_or_else(|| Err(anyhow::anyhow!("{unixname}: a cache hit must not look up")))
    }

    /// A request without a caller, which neither looks up nor changes the
    /// cache but starts a sweep unless one is running.
    fn request(service: &impl RequestGate) -> Result<()> {
        service
            .admit(&RequestContext::default(), "probe", "test-bus")
            .now_or_never()
            .ok_or_else(|| anyhow::anyhow!("a request without a caller looked up"))?
            .err()
            .ok_or_else(|| anyhow::anyhow!("a request without a caller was admitted"))?;
        Ok(())
    }

    /// Answers the refresh lookup a sweep makes for `unixname`.
    async fn refreshed(inner: &ControlledEligibilityLookup, unixname: &str, result: Result<bool>) {
        inner.wait_for_lookup(unixname).await;
        inner.answer(unixname, result);
    }

    async fn lookup(
        service: &impl RequestGate,
        inner: &ControlledEligibilityLookup,
        unixname: &str,
        result: Result<bool>,
    ) -> Result<bool> {
        let mut check = Box::pin(gate_decision(service, unixname));
        anyhow::ensure!(
            (&mut check).now_or_never().is_none(),
            "{unixname}: expected a lookup"
        );
        inner.wait_for_lookup(unixname).await;
        inner.answer(unixname, result);
        check.await
    }

    /// Allow and deny decisions are retained until a fixed deadline that hits
    /// do not extend; an expired decision is looked up again.
    #[scenario(sim_only)]
    pub async fn run_decisions_expire_at_fixed_deadline<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        anyhow::ensure!(!lookup(&service, &inner, "bob", Ok(false)).await?);

        env.sleep(TTL - Duration::from_secs(1)).await;
        anyhow::ensure!(cached(&service, "alice")?);
        anyhow::ensure!(!cached(&service, "bob")?);

        env.sleep(Duration::from_secs(1)).await;
        anyhow::ensure!(!lookup(&service, &inner, "alice", Ok(false)).await?);
        anyhow::ensure!(!cached(&service, "alice")?);
        Ok(())
    }

    /// A decision expires `TTL` after its lookup started, not after it completed.
    #[scenario(sim_only)]
    pub async fn run_expiry_starts_at_lookup_start<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        let mut check = Box::pin(gate_decision(&service, "alice"));
        anyhow::ensure!((&mut check).now_or_never().is_none());
        inner.wait_for_lookup("alice").await;
        env.sleep(Duration::from_secs(10)).await;
        inner.answer("alice", Ok(true));
        anyhow::ensure!(check.await?);

        env.sleep(TTL - Duration::from_secs(11)).await;
        anyhow::ensure!(cached(&service, "alice")?);
        env.sleep(Duration::from_secs(1)).await;
        anyhow::ensure!(!lookup(&service, &inner, "alice", Ok(false)).await?);
        Ok(())
    }

    /// Concurrent misses for one unixname each look up and get their own answer.
    #[scenario(sim_only)]
    pub async fn run_concurrent_misses_each_look_up<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        let mut first = Box::pin(gate_decision(&service, "alice"));
        let mut second = Box::pin(gate_decision(&service, "alice"));
        anyhow::ensure!((&mut first).now_or_never().is_none());
        anyhow::ensure!((&mut second).now_or_never().is_none());
        anyhow::ensure!(inner.waiting() == 2, "each miss looks up");
        inner.answer("alice", Ok(true));
        inner.answer("alice", Ok(false));
        anyhow::ensure!(first.await?);
        anyhow::ensure!(!second.await?);
        Ok(())
    }

    /// A lookup that started earlier but completes later does not replace the
    /// decision of a later lookup.
    #[scenario(sim_only)]
    pub async fn run_older_lookups_do_not_replace_newer_decisions<
        F: EligibilityCacheTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        let mut older = Box::pin(gate_decision(&service, "alice"));
        anyhow::ensure!((&mut older).now_or_never().is_none());
        env.sleep(Duration::from_secs(1)).await;
        let mut newer = Box::pin(gate_decision(&service, "alice"));
        anyhow::ensure!((&mut newer).now_or_never().is_none());

        inner.answer_last("alice", Ok(true));
        anyhow::ensure!(newer.await?);
        inner.answer("alice", Ok(false));
        anyhow::ensure!(!older.await?);
        anyhow::ensure!(
            cached(&service, "alice")?,
            "an older lookup replaced a newer decision"
        );
        Ok(())
    }

    /// Cancelling a caller cancels its lookup and caches nothing.
    #[scenario(sim_only)]
    pub async fn run_cancelled_callers_leave_nothing_cached<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        let mut cancelled = Box::pin(gate_decision(&service, "alice"));
        anyhow::ensure!((&mut cancelled).now_or_never().is_none());
        anyhow::ensure!(inner.waiting() == 1);
        drop(cancelled);
        anyhow::ensure!(inner.waiting() == 0, "the lookup outlived its caller");
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        Ok(())
    }

    /// A failed lookup keeps its typed source and is not cached, and a failed
    /// refresh of an expired allow never serves the expired decision.
    #[scenario(sim_only)]
    pub async fn run_failures_are_not_cached<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        let Err(error) = lookup(&service, &inner, "alice", Err(TypedLookupError.into())).await
        else {
            anyhow::bail!("a failed lookup returned a decision");
        };
        anyhow::ensure!(error.chain().any(|cause| cause.is::<TypedLookupError>()));
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);

        env.sleep(TTL).await;
        anyhow::ensure!(
            lookup(&service, &inner, "alice", Err(TypedLookupError.into()))
                .await
                .is_err(),
            "an expired allow was served after a failed refresh"
        );
        anyhow::ensure!(!lookup(&service, &inner, "alice", Ok(false)).await?);
        Ok(())
    }

    /// Hits update recency and insertion evicts the least recently used decision.
    #[scenario(sim_only)]
    pub async fn run_hits_update_lru_recency_and_bound_capacity<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        anyhow::ensure!(!lookup(&service, &inner, "bob", Ok(false)).await?);
        anyhow::ensure!(cached(&service, "alice")?);
        anyhow::ensure!(lookup(&service, &inner, "carol", Ok(true)).await?);
        anyhow::ensure!(cached(&service, "alice")?);
        anyhow::ensure!(cached(&service, "carol")?);
        anyhow::ensure!(
            lookup(&service, &inner, "bob", Ok(true)).await?,
            "bob was the least recently used decision"
        );
        Ok(())
    }

    /// An exempt caller is admitted without a lookup and leaves no decision for
    /// its unixname, so the same unixname without the exemption is looked up.
    #[scenario(sim_only)]
    pub async fn run_exempt_callers_are_not_cached<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        let exempt = RequestContext::default()
            .with_service(EXEMPT_SERVICE)
            .with_user("alice");
        let admitted = service
            .admit(&exempt, "probe", "test-bus")
            .now_or_never()
            .ok_or_else(|| anyhow::anyhow!("an exempt caller was looked up"))?;
        anyhow::ensure!(admitted.is_ok());
        anyhow::ensure!(
            !lookup(&service, &inner, "alice", Ok(false)).await?,
            "an exempt admission was cached for its unixname"
        );
        Ok(())
    }

    /// A cached denial repeats the gate's error, message and sources.
    #[scenario(sim_only)]
    pub async fn run_cached_denials_repeat_the_gate_error<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_with_config(CAPACITY, TTL);
        let inner = fixture.lookup_controller();
        let ctx = RequestContext::default().with_user("bob");
        let mut first = Box::pin(service.admit(&ctx, "probe", "test-bus"));
        anyhow::ensure!((&mut first).now_or_never().is_none());
        inner.wait_for_lookup("bob").await;
        inner.answer("bob", Ok(false));
        let denial = |result| match result {
            Err(AuthorizationError::Denied(error)) => Ok(format!("{error:#}")),
            other => Err(anyhow::anyhow!("expected a denial: {other:?}")),
        };
        let looked_up = denial(first.await)?;
        let cached = service
            .admit(&ctx, "probe", "test-bus")
            .now_or_never()
            .ok_or_else(|| anyhow::anyhow!("a cache hit must not look up"))?;
        anyhow::ensure!(denial(cached)? == looked_up);
        Ok(())
    }

    /// Each request starts a sweep unless one is running; a sweep takes its
    /// snapshot when it first runs, here when the scenario next waits. A sweep
    /// refreshes, one at a time in the cache's scan order, the decisions cached
    /// when it started, and a refresh that decides replaces the decision.
    #[scenario(sim_only)]
    pub async fn run_requests_start_sweeps_one_at_a_time<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_refresh(CAPACITY, TTL, POLICY);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        anyhow::ensure!(!lookup(&service, &inner, "bob", Ok(false)).await?);

        inner.wait_for_lookup("bob").await;
        request(&service)?;
        env.sleep(SETTLE).await;
        anyhow::ensure!(
            inner.waiting() == 1,
            "refreshes and sweeps must be sequential"
        );
        inner.answer("bob", Ok(true));
        refreshed(&inner, "alice", Ok(false)).await;
        env.sleep(SETTLE).await;
        anyhow::ensure!(inner.waiting() == 0, "a sweep refreshed a decision twice");
        anyhow::ensure!(
            cached(&service, "bob")?,
            "a refresh did not replace bob's denial"
        );
        anyhow::ensure!(
            !cached(&service, "alice")?,
            "a refresh did not replace alice's allow"
        );

        refreshed(&inner, "alice", Ok(false)).await;
        refreshed(&inner, "bob", Ok(true)).await;
        Ok(())
    }

    /// A failed refresh keeps its allow or deny until it expires, and the sweep
    /// moves on to the next caller without retrying it.
    #[scenario(sim_only)]
    pub async fn run_failed_refresh_keeps_decision_and_advances<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_refresh(CAPACITY, TTL, POLICY);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(!lookup(&service, &inner, "bob", Ok(false)).await?);
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);

        for _ in 0..2 {
            refreshed(&inner, "alice", Err(TypedLookupError.into())).await;
            refreshed(&inner, "bob", Err(TypedLookupError.into())).await;
            env.sleep(SETTLE).await;
            anyhow::ensure!(inner.waiting() == 0, "a failed refresh was retried");
            anyhow::ensure!(!cached(&service, "bob")?, "a failed refresh admitted bob");
            anyhow::ensure!(cached(&service, "alice")?, "a failed refresh dropped alice");
        }
        refreshed(&inner, "alice", Err(TypedLookupError.into())).await;
        refreshed(&inner, "bob", Err(TypedLookupError.into())).await;
        env.sleep(TTL).await;
        anyhow::ensure!(
            !lookup(&service, &inner, "alice", Ok(false)).await?,
            "a failed refresh extended alice's TTL"
        );
        refreshed(&inner, "alice", Ok(false)).await;
        Ok(())
    }

    /// A refresh, deciding or failed, does not change a decision a request
    /// replaced while the refresh was in flight.
    #[scenario(sim_only)]
    pub async fn run_refresh_cannot_change_reinserted_decisions<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let inner = fixture.lookup_controller();
        for result in [Ok(true), Err(TypedLookupError.into())] {
            let service = fixture.create_with_refresh(CAPACITY, TTL, POLICY);
            anyhow::ensure!(lookup(&service, &inner, "bob", Ok(true)).await?);
            anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
            inner.wait_for_lookup("alice").await;
            anyhow::ensure!(cached(&service, "bob")?);
            anyhow::ensure!(lookup(&service, &inner, "carol", Ok(true)).await?);
            let mut replacement = Box::pin(gate_decision(&service, "alice"));
            anyhow::ensure!((&mut replacement).now_or_never().is_none());
            inner.answer_last("alice", Ok(false));
            anyhow::ensure!(!replacement.await?);
            inner.answer("alice", result);
            refreshed(&inner, "bob", Ok(true)).await;
            env.sleep(SETTLE).await;
            anyhow::ensure!(
                !cached(&service, "alice")?,
                "a refresh changed a replacement"
            );
            anyhow::ensure!(cached(&service, "carol")?);
            refreshed(&inner, "carol", Ok(true)).await;
            refreshed(&inner, "alice", Ok(false)).await;
        }
        Ok(())
    }

    /// A refresh neither promotes a decision nor restores one evicted while its
    /// refresh was in flight.
    #[scenario(sim_only)]
    pub async fn run_refresh_neither_promotes_nor_restores<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_refresh(CAPACITY, TTL, POLICY);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        anyhow::ensure!(lookup(&service, &inner, "bob", Ok(true)).await?);
        refreshed(&inner, "bob", Ok(true)).await;
        refreshed(&inner, "alice", Ok(true)).await;
        env.sleep(SETTLE).await;
        anyhow::ensure!(lookup(&service, &inner, "carol", Ok(true)).await?);
        anyhow::ensure!(
            cached(&service, "bob")?,
            "a refresh promoted alice past bob"
        );

        inner.wait_for_lookup("bob").await;
        anyhow::ensure!(lookup(&service, &inner, "dave", Ok(true)).await?);
        anyhow::ensure!(lookup(&service, &inner, "erin", Ok(true)).await?);
        inner.answer("bob", Ok(true));
        refreshed(&inner, "carol", Ok(true)).await;
        env.sleep(SETTLE).await;
        anyhow::ensure!(
            !lookup(&service, &inner, "bob", Ok(false)).await?,
            "a refresh restored bob"
        );
        refreshed(&inner, "bob", Ok(false)).await;
        refreshed(&inner, "erin", Ok(true)).await;
        Ok(())
    }

    /// A refresh that started before a request's lookup does not replace the
    /// request's decision, even for an expired decision.
    #[scenario(sim_only)]
    pub async fn run_refresh_does_not_replace_newer_decisions<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let inner = fixture.lookup_controller();
        for result in [Ok(true), Err(TypedLookupError.into())] {
            let service = fixture.create_with_refresh(CAPACITY, SETTLE * 10, POLICY);
            anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
            inner.wait_for_lookup("alice").await;
            env.sleep(SETTLE * 10).await;
            let mut newer = Box::pin(gate_decision(&service, "alice"));
            anyhow::ensure!((&mut newer).now_or_never().is_none(), "alice had expired");
            inner.answer_last("alice", Ok(false));
            anyhow::ensure!(!newer.await?);
            inner.answer("alice", result);
            env.sleep(SETTLE).await;
            anyhow::ensure!(
                !cached(&service, "alice")?,
                "an older refresh changed a newer decision"
            );
            refreshed(&inner, "alice", Ok(false)).await;
        }
        Ok(())
    }

    /// A sweep running when its gate is dropped finishes its snapshot and
    /// releases the cache, and no other sweep starts.
    #[scenario(sim_only)]
    pub async fn run_dropped_gates_stop_refreshing<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_refresh(CAPACITY, TTL, POLICY);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        anyhow::ensure!(lookup(&service, &inner, "bob", Ok(true)).await?);
        inner.wait_for_lookup("bob").await;
        drop(service);
        inner.answer("bob", Ok(true));
        refreshed(&inner, "alice", Ok(true)).await;

        env.sleep(SETTLE).await;
        anyhow::ensure!(inner.waiting() == 0, "a dropped gate refreshed");
        Ok(())
    }

    /// A refresh restarts a decision's TTL from the refresh's start, not its
    /// end.
    #[scenario(sim_only)]
    pub async fn run_refresh_restarts_ttl_from_its_start<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let service = fixture.create_with_refresh(CAPACITY, TTL, POLICY);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        refreshed(&inner, "alice", Err(TypedLookupError.into())).await;
        env.sleep(SETTLE * 100).await;
        request(&service)?;
        inner.wait_for_lookup("alice").await;
        env.sleep(SETTLE * 10).await;
        inner.answer("alice", Ok(true));

        env.sleep(TTL - SETTLE * 11).await;
        anyhow::ensure!(
            cached(&service, "alice")?,
            "a refresh did not restart alice's TTL"
        );
        refreshed(&inner, "alice", Err(TypedLookupError.into())).await;
        env.sleep(SETTLE * 2).await;
        anyhow::ensure!(
            !lookup(&service, &inner, "alice", Ok(false)).await?,
            "a refresh restarted alice's TTL from its end"
        );
        refreshed(&inner, "alice", Ok(false)).await;
        Ok(())
    }

    /// A cache without refresh never refreshes, and sweeps skip expired
    /// decisions.
    #[scenario(sim_only)]
    pub async fn run_no_refresh_without_refresh_or_for_expired_decisions<
        F: EligibilityCacheTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let inner = fixture.lookup_controller();
        let service = fixture.create_with_config(CAPACITY, TTL);
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        request(&service)?;
        env.sleep(SETTLE).await;
        anyhow::ensure!(inner.waiting() == 0, "a cache without refresh refreshed");

        let service = fixture.create_with_refresh(CAPACITY, SETTLE, POLICY);
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        refreshed(&inner, "alice", Err(TypedLookupError.into())).await;
        env.sleep(SETTLE * 2).await;
        request(&service)?;
        env.sleep(SETTLE).await;
        anyhow::ensure!(
            inner.waiting() == 0,
            "a sweep refreshed an expired decision"
        );
        Ok(())
    }

    /// Zero TTL retains nothing.
    #[scenario(sim_only)]
    pub async fn run_zero_ttl_bypasses_cache<F: EligibilityCacheTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_with_config(CAPACITY, Duration::ZERO);
        let inner = fixture.lookup_controller();
        anyhow::ensure!(lookup(&service, &inner, "alice", Ok(true)).await?);
        anyhow::ensure!(!lookup(&service, &inner, "alice", Ok(false)).await?);
        Ok(())
    }
}
pub use defs::*;
