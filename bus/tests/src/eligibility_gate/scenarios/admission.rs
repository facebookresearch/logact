/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(eligibility_gate_admission_list)]
mod defs {
    //! Test scenarios for how an eligibility `RequestGate` admits a caller from
    //! its identities and eligibility

    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use agentbus_api::AuthorizationError;
    use agentbus_api::RequestContext;
    use agentbus_api::RequestGate;
    use anyhow::Result;

    use crate::eligibility::EXEMPT_SERVICE;
    use crate::eligibility::Eligibility;
    use crate::eligibility::InMemoryEligibilityLookup;
    use crate::eligibility::LookupUnavailable;
    use crate::eligibility_gate::fixtures::EligibilityGateTestFixture;

    #[derive(Debug, PartialEq)]
    enum Decision {
        Admitted,
        Denied,
        Unavailable,
    }

    async fn decide_on(
        gate: &impl RequestGate,
        ctx: &RequestContext,
        method: &'static str,
        agent_bus_id: &str,
    ) -> Decision {
        match gate.admit(ctx, method, agent_bus_id).await {
            Ok(()) => Decision::Admitted,
            Err(AuthorizationError::Denied(_)) => Decision::Denied,
            Err(AuthorizationError::Unavailable(_)) => Decision::Unavailable,
        }
    }

    async fn assert_decisions(
        gate: &impl RequestGate,
        cases: impl IntoIterator<Item = (RequestContext, Decision)>,
    ) {
        for (ctx, expected) in cases {
            let decision = decide_on(gate, &ctx, "commit_intention", "bus").await;
            assert_eq!(decision, expected, "{ctx:?}");
        }
    }

    fn user(unixname: &str) -> RequestContext {
        RequestContext::default().with_user(unixname)
    }

    fn service(name: &str) -> RequestContext {
        RequestContext::default().with_service(name)
    }

    fn looked_up(eligibility: &InMemoryEligibilityLookup) -> BTreeSet<String> {
        eligibility.lookups().into_keys().collect()
    }

    /// Test that a caller with an exempt service identity is admitted without a
    /// lookup, whatever its users and other services.
    #[scenario]
    pub async fn run_test_exempt_services_are_admitted_without_lookup<
        F: EligibilityGateTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        let eligibility = fixture.eligibility();
        eligibility.set("bob", Eligibility::Ineligible);
        eligibility.set("carol", Eligibility::Unavailable);
        let gate = fixture.create_impl();

        assert_decisions(
            &gate,
            [
                service(EXEMPT_SERVICE),
                service(EXEMPT_SERVICE).with_user("bob"),
                service(EXEMPT_SERVICE).with_user("carol"),
                service(EXEMPT_SERVICE).with_user("bob").with_user("carol"),
                service("test.other.service").with_service(EXEMPT_SERVICE),
            ]
            .map(|ctx| (ctx, Decision::Admitted)),
        )
        .await;
        assert_eq!(looked_up(&eligibility), BTreeSet::new());
        Ok(())
    }

    /// Test that any other caller needs exactly one unixname, ignoring empty and
    /// numeric users, or is denied without a lookup. Duplicate unixnames are
    /// ambiguous.
    #[scenario]
    pub async fn run_test_callers_need_exactly_one_unixname<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let eligibility = fixture.eligibility();
        for unixname in ["alice", "dave", "erin"] {
            eligibility.set(unixname, Eligibility::Eligible);
        }
        let gate = fixture.create_impl();

        assert_decisions(
            &gate,
            [
                RequestContext::default(),
                user(""),
                user("123456789"),
                user("").with_user("123456789"),
                user("dave").with_user("erin"),
                user("dave").with_user("dave"),
                service("test.other.service"),
            ]
            .map(|ctx| (ctx, Decision::Denied)),
        )
        .await;
        assert_eq!(looked_up(&eligibility), BTreeSet::new());

        assert_decisions(
            &gate,
            [
                user("123456789").with_user("alice"),
                user("").with_user("alice"),
                service("test.other.service").with_user("alice"),
            ]
            .map(|ctx| (ctx, Decision::Admitted)),
        )
        .await;
        assert_eq!(
            looked_up(&eligibility),
            BTreeSet::from(["alice".to_owned()])
        );
        Ok(())
    }

    /// Test that the caller's unixname is looked up once: an eligible caller is
    /// admitted, an ineligible or unknown one denied, and a failed lookup is
    /// unavailable.
    #[scenario]
    pub async fn run_test_lookup_decides_admission<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let eligibility = fixture.eligibility();
        eligibility.set("alice", Eligibility::Eligible);
        eligibility.set("bob", Eligibility::Ineligible);
        eligibility.set("carol", Eligibility::Unavailable);
        let gate = fixture.create_impl();

        assert_decisions(
            &gate,
            [
                (user("alice"), Decision::Admitted),
                (user("bob"), Decision::Denied),
                (user("dave"), Decision::Denied),
                (user("carol"), Decision::Unavailable),
            ],
        )
        .await;
        let expected = ["alice", "bob", "carol", "dave"].map(|name| (name.to_owned(), 1));
        assert_eq!(eligibility.lookups(), BTreeMap::from(expected));
        Ok(())
    }

    /// Test that an unavailable decision carries the lookup's error.
    #[scenario]
    pub async fn run_test_unavailable_keeps_lookup_error<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        fixture.eligibility().set("carol", Eligibility::Unavailable);
        let gate = fixture.create_impl();

        let result = gate.admit(&user("carol"), "commit_intention", "bus").await;
        let Err(AuthorizationError::Unavailable(error)) = result else {
            panic!("a failed lookup should be unavailable, got {result:?}");
        };
        assert!(
            error.chain().any(|cause| cause.is::<LookupUnavailable>()),
            "lost the lookup error: {error:#}"
        );
        Ok(())
    }

    /// Test that the method and bus a request targets do not change its
    /// caller's decision.
    #[scenario]
    pub async fn run_test_method_and_bus_do_not_change_decision<F: EligibilityGateTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let eligibility = fixture.eligibility();
        eligibility.set("alice", Eligibility::Eligible);
        eligibility.set("bob", Eligibility::Ineligible);
        let gate = fixture.create_impl();

        for (method, agent_bus_id) in [
            ("append", "bus-a"),
            ("poll", "bus-b"),
            ("commit_intention", "bus-a"),
        ] {
            for (unixname, expected) in [("alice", Decision::Admitted), ("bob", Decision::Denied)] {
                let decision = decide_on(&gate, &user(unixname), method, agent_bus_id).await;
                assert_eq!(decision, expected, "{unixname} on {method} {agent_bus_id}");
            }
        }
        Ok(())
    }
}
pub use defs::*;
