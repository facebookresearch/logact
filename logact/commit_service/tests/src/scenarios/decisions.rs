/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_decisions_list)]
mod defs {

    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::Environment;
    use agentbus_tests::common::helpers::read_linearizable_snapshot;
    use agentbus_tests::decisions::LoggedDecision;
    use agentbus_tests::decisions::first_decisions;
    use anyhow::Context as _;
    use anyhow::Result;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::CommitServiceTestFixture;
    use crate::fixtures::DeciderPolicyFixture;

    #[scenario]
    pub async fn run_test_concurrent_calls_return_persisted_outcomes<F: DeciderPolicyFixture>(
        fixture: &F,
    ) -> Result<()> {
        fixture
            .set_decider_policy(DeciderPolicy::OnByDefault)
            .await?;
        let service = fixture.create_impl();
        let other_service = fixture.create_impl();
        let bus_id = format!(
            "concurrent-decisions-{}",
            fixture.get_env().with_rng(|rng| rng.random::<u64>())
        );
        let command = CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: bus_id.clone(),
            },
            intention: intention::Intention::StringIntention("concurrent decision".to_string()),
        };
        let (first, second) = futures::try_join!(
            service.commit_intention(command.clone()),
            other_service.commit_intention(command),
        )?;
        anyhow::ensure!(first.approved && second.approved);
        anyhow::ensure!(first.log_position != second.log_position);
        let (entries, _) = read_linearizable_snapshot(
            service.agent_bus(),
            bus_id,
            0,
            None,
            fixture.get_env().as_ref(),
            Some(1),
        )
        .await;
        let decisions = first_decisions(&entries);
        for outcome in [first, second] {
            let decision = decisions
                .get(&outcome.log_position)
                .map(|decision| (decision.approved, &decision.reason));
            anyhow::ensure!(
                decision == Some((outcome.approved, &outcome.reason)),
                "each concurrent request must return its first durable decision"
            );
        }
        Ok(())
    }

    #[scenario]
    pub async fn run_test_returned_outcome_is_persisted<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        check_returned_outcome_is_persisted(fixture).await?;
        Ok(())
    }

    #[scenario]
    pub async fn run_test_approved_and_denied_outcomes_are_persisted<F: DeciderPolicyFixture>(
        fixture: &F,
    ) -> Result<()> {
        for (policy, approved) in [
            (DeciderPolicy::OnByDefault, true),
            (DeciderPolicy::OffByDefault, false),
        ] {
            fixture.set_decider_policy(policy).await?;
            anyhow::ensure!(
                check_returned_outcome_is_persisted(fixture).await? == approved,
                "{policy:?} must return approved={approved}"
            );
        }
        Ok(())
    }

    async fn check_returned_outcome_is_persisted<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<bool> {
        let service = fixture.create_impl();
        let bus_id = format!(
            "decisions-{}",
            fixture.get_env().with_rng(|rng| rng.random::<u64>())
        );
        let outcome = service
            .commit_intention(CommitIntentionCommand {
                bus_id: agentbus_api::BusId {
                    agent_bus_id: bus_id.clone(),
                },
                intention: intention::Intention::StringIntention("decision probe".to_string()),
            })
            .await?;
        assert_returned_outcome_is_persisted(
            &service,
            fixture.get_env().as_ref(),
            &bus_id,
            &outcome,
        )
        .await?;
        Ok(outcome.approved)
    }

    pub async fn assert_returned_outcome_is_persisted<S: CommitSvc, E: Environment>(
        service: &S,
        environment: &E,
        bus_id: &str,
        outcome: &logact_commit_service_api::CommitIntentionOutcome,
    ) -> Result<()> {
        let (entries, _) = read_linearizable_snapshot(
            service.agent_bus(),
            bus_id.to_owned(),
            0,
            None,
            environment,
            Some(1),
        )
        .await;
        anyhow::ensure!(
            matches!(
                entries
                    .iter()
                    .find(|entry| entry
                        .header
                        .as_ref()
                        .is_some_and(|header| header.log_position == outcome.log_position))
                    .and_then(|entry| entry.payload.as_ref()?.payload.as_ref()),
                Some(payload::Payload::Intention(_))
            ),
            "returned position {} must identify the committed intention",
            outcome.log_position,
        );
        let decisions = first_decisions(&entries);
        let LoggedDecision {
            position,
            approved,
            reason,
            ..
        } = decisions.get(&outcome.log_position).with_context(|| {
            format!(
                "returned outcome for intention {} (approved={}) has no persisted decision",
                outcome.log_position, outcome.approved,
            )
        })?;
        anyhow::ensure!(
            *position > outcome.log_position,
            "decision must follow its intention"
        );
        anyhow::ensure!(
            *approved == outcome.approved && reason == &outcome.reason,
            "returned outcome disagrees with the first persisted decision at {position}"
        );
        Ok(())
    }
}

pub use defs::*;
