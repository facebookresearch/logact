/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_abort_on_drain_list)]
mod defs {
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::Environment;
    use agentbus_tests::common::helpers::append_vote;
    use agentbus_tests::common::helpers::read_linearizable_snapshot;
    use anyhow::Context as _;
    use anyhow::Result;
    use futures::future::Either;
    use futures::future::select;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitIntentionOutcome;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::DeciderPolicyFixture;

    fn intention(bus_id: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: bus_id.to_string(),
            },
            intention: intention::Intention::StringIntention("drain probe".to_string()),
        }
    }

    async fn assert_durable_abort<F: DeciderPolicyFixture>(
        fixture: &F,
        service: &F::Impl,
        bus_id: &str,
        outcome: &CommitIntentionOutcome,
    ) -> Result<()> {
        let (entries, _) = read_linearizable_snapshot(
            service.agent_bus(),
            bus_id.to_string(),
            outcome.log_position,
            None,
            fixture.get_env().as_ref(),
            Some(1),
        )
        .await;
        anyhow::ensure!(matches!(
            entries
                .first()
                .and_then(|entry| entry.payload.as_ref()?.payload.as_ref()),
            Some(payload::Payload::Intention(_))
        ));
        let (decision_index, decision) = entries
            .iter()
            .enumerate()
            .find(
                |(_, entry)| match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
                    Some(payload::Payload::Commit(commit)) => {
                        commit.intention_id == outcome.log_position
                    }
                    Some(payload::Payload::Abort(abort)) => {
                        abort.intention_id == outcome.log_position
                    }
                    _ => false,
                },
            )
            .context("an undecided intention must have a durable abort after the bus drains")?;
        anyhow::ensure!(
            entries[..decision_index].iter().all(|entry| !matches!(
                entry.payload.as_ref().and_then(|p| p.payload.as_ref()),
                Some(payload::Payload::Vote(_))
            )),
            "the drain-time abort must happen without any votes"
        );
        anyhow::ensure!(
            decision
                .header
                .as_ref()
                .context("decision must have a header")?
                .log_position
                > outcome.log_position
        );
        let Some(payload::Payload::Abort(abort)) =
            decision.payload.as_ref().and_then(|p| p.payload.as_ref())
        else {
            anyhow::bail!(
                "the first durable decision must be an abort, even after a late approving vote"
            );
        };
        anyhow::ensure!(abort.reason == outcome.reason);
        Ok(())
    }

    #[scenario]
    pub async fn run_test_undecided_intention_aborts_on_drain<F: DeciderPolicyFixture>(
        fixture: &F,
    ) -> Result<()> {
        fixture
            .set_decider_policy(DeciderPolicy::FirstBooleanWins)
            .await?;
        let service = fixture.create_impl();
        let environment = fixture.get_env();
        let bus_id = format!("drain-{}", environment.with_rng(|rng| rng.random::<u64>()));
        let outcome = match select(
            Box::pin(service.commit_intention(intention(&bus_id))),
            Box::pin(environment.sleep(Duration::from_secs(10))),
        )
        .await
        {
            Either::Left((result, _)) => result?,
            Either::Right(_) => {
                anyhow::bail!("an intention with no votes must abort within ten seconds")
            }
        };
        anyhow::ensure!(!outcome.approved, "an intention with no votes must abort");
        assert_durable_abort(fixture, &service, &bus_id, &outcome).await?;

        append_vote(
            service.agent_bus(),
            bus_id.clone(),
            outcome.log_position,
            true,
        )
        .await;
        fixture
            .set_decider_policy(DeciderPolicy::OnByDefault)
            .await?;
        let next = service.commit_intention(intention(&bus_id)).await?;
        anyhow::ensure!(next.approved && next.log_position > outcome.log_position);
        assert_durable_abort(fixture, &service, &bus_id, &outcome).await
    }
}

pub use defs::*;
