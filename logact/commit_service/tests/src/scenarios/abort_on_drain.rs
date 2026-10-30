/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_abort_on_drain_list)]
mod defs {
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::Abort;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::AgentBus;
    use agentbus_api::AppendRequest;
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

    async fn append_undecided_intention(bus: &impl AgentBus, bus_id: &str) -> Result<i64> {
        Ok(bus
            .append(AppendRequest {
                bus_id: Some(agentbus_api::BusId {
                    agent_bus_id: bus_id.to_string(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::Intention(Intention {
                        intention: Some(intention::Intention::StringIntention(
                            "earlier".to_string(),
                        )),
                        ..Default::default()
                    })),
                }),
            })
            .await?
            .log_position)
    }

    #[scenario]
    pub async fn run_test_superseded_intention_is_not_aborted_on_drain<F: DeciderPolicyFixture>(
        fixture: &F,
    ) -> Result<()> {
        fixture
            .set_decider_policy(DeciderPolicy::FirstBooleanWins)
            .await?;
        let service = fixture.create_impl();
        let environment = fixture.get_env();
        let bus_id = format!(
            "superseded-{}",
            environment.with_rng(|rng| rng.random::<u64>())
        );
        let initialized = service.commit_intention(intention(&bus_id)).await?;
        assert_durable_abort(fixture, &service, &bus_id, &initialized).await?;

        let earlier = append_undecided_intention(service.agent_bus(), &bus_id).await?;
        let outcome = service.commit_intention(intention(&bus_id)).await?;
        assert_durable_abort(fixture, &service, &bus_id, &outcome).await?;

        append_vote(service.agent_bus(), bus_id.clone(), earlier, true).await;
        let next = service.commit_intention(intention(&bus_id)).await?;
        assert_durable_abort(fixture, &service, &bus_id, &next).await?;
        let (entries, _) = read_linearizable_snapshot(
            service.agent_bus(),
            bus_id,
            earlier,
            None,
            environment.as_ref(),
            Some(1),
        )
        .await;
        // If supersession adopts explicit aborts, require a durable Abort here instead.
        anyhow::ensure!(
            entries.iter().all(|entry| {
                match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
                    Some(payload::Payload::Commit(commit)) => commit.intention_id != earlier,
                    Some(payload::Payload::Abort(abort)) => abort.intention_id != earlier,
                    _ => true,
                }
            }),
            "a superseded unvoted intention must stay undecided, even after a late approving vote"
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_replay_decision_before_aborting_unvoted_request<
        F: DeciderPolicyFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        fixture
            .set_decider_policy(DeciderPolicy::FirstBooleanWins)
            .await?;
        let service = fixture.create_impl();
        let environment = fixture.get_env();
        let bus_id = format!(
            "replay-drain-{}",
            environment.with_rng(|rng| rng.random::<u64>())
        );
        let initialized = service.commit_intention(intention(&bus_id)).await?;
        assert_durable_abort(fixture, &service, &bus_id, &initialized).await?;

        let earlier = append_undecided_intention(service.agent_bus(), &bus_id).await?;
        append_vote(service.agent_bus(), bus_id.clone(), earlier, true).await;

        let outcome = service.commit_intention(intention(&bus_id)).await?;
        anyhow::ensure!(!outcome.approved, "the new request has no approving vote");
        assert_durable_abort(fixture, &service, &bus_id, &outcome).await?;
        let (entries, _) = read_linearizable_snapshot(
            service.agent_bus(),
            bus_id,
            earlier,
            None,
            environment.as_ref(),
            Some(1),
        )
        .await;
        let decision = entries
            .iter()
            .filter_map(|entry| entry.payload.as_ref()?.payload.as_ref())
            .find(|payload| match payload {
                payload::Payload::Commit(commit) => commit.intention_id == earlier,
                payload::Payload::Abort(abort) => abort.intention_id == earlier,
                _ => false,
            })
            .context("replay must persist the earlier intention's decision")?;
        anyhow::ensure!(
            matches!(decision, payload::Payload::Commit(_)),
            "the earlier intention's approving vote must produce a commit"
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_logged_abort_remains_first_decision_after_late_vote<
        F: DeciderPolicyFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        fixture
            .set_decider_policy(DeciderPolicy::FirstBooleanWins)
            .await?;
        let service = fixture.create_impl();
        let environment = fixture.get_env();
        let bus_id = format!(
            "logged-abort-{}",
            environment.with_rng(|rng| rng.random::<u64>())
        );
        let initialized = service.commit_intention(intention(&bus_id)).await?;
        assert_durable_abort(fixture, &service, &bus_id, &initialized).await?;

        let earlier = append_undecided_intention(service.agent_bus(), &bus_id).await?;
        let reason = "externally logged abort".to_string();
        service
            .agent_bus()
            .append(AppendRequest {
                bus_id: Some(agentbus_api::BusId {
                    agent_bus_id: bus_id.clone(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::Abort(Abort {
                        intention_id: earlier,
                        reason: reason.clone(),
                    })),
                }),
            })
            .await?;
        append_vote(service.agent_bus(), bus_id.clone(), earlier, true).await;
        let next = service.commit_intention(intention(&bus_id)).await?;
        anyhow::ensure!(!next.approved, "the new request has no approving vote");
        assert_durable_abort(
            fixture,
            &service,
            &bus_id,
            &CommitIntentionOutcome {
                approved: false,
                reason,
                log_position: earlier,
            },
        )
        .await?;
        assert_durable_abort(fixture, &service, &bus_id, &next).await
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
        let next = service.commit_intention(intention(&bus_id)).await?;
        anyhow::ensure!(!next.approved && next.log_position > outcome.log_position);
        assert_durable_abort(fixture, &service, &bus_id, &next).await?;
        assert_durable_abort(fixture, &service, &bus_id, &outcome).await
    }
}

pub use defs::*;
