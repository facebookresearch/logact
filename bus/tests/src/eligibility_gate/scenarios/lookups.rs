/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(eligibility_gate_lookups_list)]
mod defs {
    //! Test scenarios for eligibility lookups through `RequestGate` admission
    //! over time, under concurrency, and across a lossy network

    use agentbus_api::Environment;
    use anyhow::Result;
    use futures::StreamExt as _;
    use futures::stream;

    use crate::eligibility::Eligibility;
    use crate::eligibility::InMemoryEligibilityLookup;
    use crate::eligibility::gate_decision;
    use crate::eligibility_gate::fixtures::EligibilityGateTestFixture;

    fn alice_or_bob(index: usize) -> &'static str {
        if index.is_multiple_of(2) {
            "alice"
        } else {
            "bob"
        }
    }

    /// Gives each of `count` new callers alternating decisions and returns them
    /// with whether each is eligible.
    fn set_callers(eligibility: &InMemoryEligibilityLookup, count: usize) -> Vec<(String, bool)> {
        (0..count)
            .map(|index| {
                let eligible = index.is_multiple_of(2);
                let unixname = format!("{}-{index}", alice_or_bob(index));
                let decision = if eligible {
                    Eligibility::Eligible
                } else {
                    Eligibility::Ineligible
                };
                eligibility.set(&unixname, decision);
                (unixname, eligible)
            })
            .collect()
    }

    /// Test that a changed decision applies once any retained decision expires.
    /// Failures are never retained, so recovery applies to the next lookup.
    #[scenario]
    pub async fn run_test_decision_changes_apply_to_next_lookup<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let eligibility = fixture.eligibility();
        let gate = fixture.create_impl();

        let mut previous = None;
        for (decision, expected) in [
            (Eligibility::Eligible, Some(true)),
            (Eligibility::Ineligible, Some(false)),
            (Eligibility::Unavailable, None),
            (Eligibility::Eligible, Some(true)),
        ] {
            eligibility.set("alice", decision);
            if previous.is_some() && !fixture.cache_ttl().is_zero() {
                assert_eq!(
                    gate_decision(&gate, "alice").await.ok(),
                    previous,
                    "{decision:?}"
                );
                fixture.get_env().sleep(fixture.cache_ttl()).await;
            }
            assert_eq!(
                gate_decision(&gate, "alice").await.ok(),
                expected,
                "{decision:?}"
            );
            previous = expected;
        }
        Ok(())
    }

    /// Test that gates share the table but not retained decisions.
    #[scenario(sim_only)]
    pub async fn run_test_gates_keep_separate_decisions<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let eligibility = fixture.eligibility();
        eligibility.set("alice", Eligibility::Eligible);
        let first = fixture.create_impl();
        assert!(gate_decision(&first, "alice").await?);

        eligibility.set("alice", Eligibility::Ineligible);
        let second = fixture.create_impl();
        assert!(
            !gate_decision(&second, "alice").await?,
            "a new gate reused another gate's decision"
        );
        assert_eq!(
            gate_decision(&first, "alice").await?,
            !fixture.cache_ttl().is_zero(),
            "the first gate should keep its own decision until it expires"
        );
        fixture.get_env().sleep(fixture.cache_ttl()).await;
        assert!(
            !gate_decision(&first, "alice").await?,
            "an expired decision was kept"
        );
        Ok(())
    }

    /// Test that concurrent admissions for different callers each get their own
    /// caller's answer.
    #[scenario(sim_only, latency_ms = 20)]
    pub async fn run_test_concurrent_lookups_answer_their_own_caller<
        F: EligibilityGateTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        const LOOKUPS: usize = 16;
        let eligibility = fixture.eligibility();
        eligibility.set("alice", Eligibility::Eligible);
        eligibility.set("bob", Eligibility::Ineligible);
        let gate = &fixture.create_impl();

        let answers: Vec<_> = stream::iter(0..LOOKUPS)
            .map(|index| async move {
                let unixname = alice_or_bob(index);
                (unixname, gate_decision(gate, unixname).await)
            })
            .buffer_unordered(LOOKUPS)
            .collect()
            .await;
        for (unixname, answer) in answers {
            assert_eq!(answer?, unixname == "alice", "{unixname}");
        }
        Ok(())
    }

    /// Test that a request lost to the network is unavailable; it never admits
    /// an ineligible caller or denies an eligible one.
    #[scenario(sim_only, latency_ms = 20, loss = 0.2)]
    pub async fn run_test_lost_lookups_are_unavailable<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        const LOOKUPS: usize = 50;
        let eligibility = fixture.eligibility();
        let gate = fixture.create_impl();

        let mut failures = 0;
        for (unixname, eligible) in set_callers(&eligibility, LOOKUPS) {
            match gate_decision(&gate, &unixname).await {
                Ok(answer) => assert_eq!(answer, eligible, "{unixname}"),
                Err(_) => failures += 1,
            }
        }
        assert!(
            failures > 0 && failures < LOOKUPS,
            "{failures} of {LOOKUPS} lookups failed"
        );
        Ok(())
    }

    /// Test that retried lookups survive network loss, while a failing service
    /// still fails.
    #[scenario(sim_only, latency_ms = 20, loss = 0.05, retry)]
    pub async fn run_test_retried_lookups_survive_loss<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let eligibility = fixture.eligibility();
        eligibility.set("carol", Eligibility::Unavailable);
        let gate = fixture.create_impl();

        for (unixname, eligible) in set_callers(&eligibility, 20) {
            assert_eq!(
                gate_decision(&gate, &unixname).await?,
                eligible,
                "{unixname}"
            );
        }
        assert!(
            gate_decision(&gate, "carol").await.is_err(),
            "carol's lookup should fail"
        );
        Ok(())
    }
}
pub use defs::*;
