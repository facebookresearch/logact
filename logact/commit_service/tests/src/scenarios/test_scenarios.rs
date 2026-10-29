/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Trait-surface scenarios for `CommitSvc`.
//!
//! Each scenario is a generic async function over a `CommitServiceTestFixture`
//! plus a `#[scenario(..)]` annotation. The
//! `#[scenarios(commit_service_test_scenarios_list)]` proc-macro scans this module
//! and generates the `commit_service_test_scenarios_list!` callback, which the
//! `define_driver!`-generated `commit_service_scenarios!` combiner folds with the
//! other scenario files' lists. Adding a test is just adding an annotated function
//! here.

#[conformance_macros::scenarios(commit_service_test_scenarios_list)]
mod defs {
    use std::collections::HashSet;

    use agent_bus_proto_rust::agent_bus::AgentInput;
    use agent_bus_proto_rust::agent_bus::AppendRequest;
    use agent_bus_proto_rust::agent_bus::BusId;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::PollRequest;
    use agent_bus_proto_rust::agent_bus::agent_input;
    use agent_bus_proto_rust::agent_bus::base_engine_control;
    use agent_bus_proto_rust::agent_bus::control;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::AgentBus;
    use agentbus_api::environment::Environment;
    use agentbus_tests::common::helpers::read_linearizable_snapshot;
    use anyhow::Result;
    use futures::StreamExt as _;
    use futures::TryStreamExt as _;
    use futures::future::try_join;
    use logact_commit_service_api::CommitError;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitIntentionOutcome;
    use logact_commit_service_api::CommitSvc;
    use logact_commit_service_api::CreateBusCommand;
    use rand::RngExt as _;

    use crate::fixtures::CommitServiceTestFixture;
    use crate::scenarios::decisions::assert_returned_outcome_is_persisted;

    async fn assert_initial_policy_is_appended<F: CommitServiceTestFixture>(
        fixture: &F,
        service: &F::Impl,
        bus_id: &str,
    ) -> Result<()> {
        let (entries, _) = read_linearizable_snapshot(
            service.agent_bus(),
            bus_id.to_owned(),
            0,
            None,
            fixture.get_env().as_ref(),
            Some(64),
        )
        .await;
        anyhow::ensure!(
            !entries.iter().any(|entry| matches!(
                entry.payload.as_ref().and_then(|p| p.payload.as_ref()),
                Some(payload::Payload::Intention(_)),
            )),
            "creation must not submit an intention"
        );
        let policy = entries
            .iter()
            .find_map(|entry| match entry.payload.as_ref()?.payload.as_ref()? {
                payload::Payload::Control(control) => match control.control.as_ref()? {
                    control::Control::BaseEngineControl(control) => {
                        match control.control.as_ref()? {
                            base_engine_control::Control::PolicyBatch(batch) => Some(batch),
                            _ => None,
                        }
                    }
                },
                _ => None,
            })
            .ok_or_else(|| {
                anyhow::anyhow!("CreateBus must append its initial policy before any commit")
            })?;
        anyhow::ensure!(
            policy.expected_current_version.is_none(),
            "initial policy must be a bootstrap batch"
        );
        anyhow::ensure!(
            policy.decider_policy == Some(DeciderPolicy::OnByDefault as i32),
            "default initial policy must approve"
        );
        Ok(())
    }

    fn fresh_agent_id<F: CommitServiceTestFixture>(fixture: &F) -> String {
        let env = fixture.get_env();
        format!("agent-{}", env.with_rng(|rng| rng.random::<u64>()))
    }

    fn intention(agent_id: String, body: impl Into<String>) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: agent_id,
            },
            intention: intention::Intention::StringIntention(body.into()),
        }
    }

    async fn commit_two_intentions_concurrently<F>(
        fixture: &F,
    ) -> Result<(CommitIntentionOutcome, CommitIntentionOutcome)>
    where
        F: CommitServiceTestFixture,
        F::Impl: Clone,
    {
        let first_service = fixture.create_impl();
        let second_service = first_service.clone();
        let agent_id = fresh_agent_id(fixture);

        let (first, second) = try_join(
            first_service.commit_intention(intention(agent_id.clone(), "first")),
            second_service.commit_intention(intention(agent_id, "second")),
        )
        .await?;

        anyhow::ensure!(
            first.log_position != second.log_position,
            "concurrent intentions must occupy distinct log positions"
        );
        Ok((first, second))
    }

    #[scenario]
    pub async fn run_create_bus_then_commit<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_impl();
        let bus_id = fresh_agent_id(fixture);
        service
            .create_bus(CreateBusCommand {
                bus_id: bus_id.clone(),
            })
            .await?;
        assert_initial_policy_is_appended(fixture, &service, &bus_id).await?;
        let committing_service = fixture.create_impl();
        let committed = committing_service
            .commit_intention(intention(bus_id.clone(), "probe"))
            .await?;
        anyhow::ensure!(committed.approved, "default policy should approve");
        assert_returned_outcome_is_persisted(
            &committing_service,
            fixture.get_env().as_ref(),
            &bus_id,
            &committed,
        )
        .await?;
        Ok(())
    }

    #[scenario]
    pub async fn run_concurrent_create_bus<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture,
        F::Impl: Clone,
    {
        let first = fixture.create_impl();
        let second = first.clone();
        let bus_id = fresh_agent_id(fixture);
        let command = CreateBusCommand {
            bus_id: bus_id.clone(),
        };
        try_join(
            first.create_bus(command.clone()),
            second.create_bus(command),
        )
        .await?;
        assert_initial_policy_is_appended(fixture, &first, &bus_id).await?;
        let committed = first.commit_intention(intention(bus_id, "probe")).await?;
        anyhow::ensure!(committed.approved, "created bus should be usable");
        Ok(())
    }

    #[scenario]
    pub async fn run_create_bus_on_existing_uninitialized_bus<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_impl();
        let bus_id = fresh_agent_id(fixture);
        service
            .agent_bus()
            .append(AppendRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::AgentInput(AgentInput {
                        agent_input: Some(agent_input::AgentInput::StringAgentInput(
                            "existing input".to_owned(),
                        )),
                    })),
                }),
            })
            .await?;
        service
            .create_bus(CreateBusCommand {
                bus_id: bus_id.clone(),
            })
            .await?;
        assert_initial_policy_is_appended(fixture, &service, &bus_id).await?;
        let outcome = service
            .commit_intention(intention(bus_id.clone(), "probe"))
            .await?;
        anyhow::ensure!(
            outcome.approved,
            "commit must replay the policy after a nonzero creation append"
        );
        assert_returned_outcome_is_persisted(
            &service,
            fixture.get_env().as_ref(),
            &bus_id,
            &outcome,
        )
        .await
    }

    #[scenario]
    pub async fn run_create_bus_rejects_invalid_id<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let service = fixture.create_impl();
        for bus_id in ["".to_owned(), "invalid bus".to_owned(), "x".repeat(257)] {
            let error = service
                .create_bus(CreateBusCommand { bus_id })
                .await
                .expect_err("invalid bus IDs must be rejected");
            anyhow::ensure!(matches!(error, CommitError::InvalidArgument(_)));
        }
        Ok(())
    }

    #[scenario]
    pub async fn run_test_intention_default_policy_approves<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let svc = fixture.create_impl();
        let response = svc
            .commit_intention(intention(fresh_agent_id(fixture), "run tool"))
            .await?;
        // No voters configured, default policy is ON_BY_DEFAULT → allow.
        assert!(
            response.approved,
            "intention with default policy should be approved"
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_multi_agent_isolation<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let svc = fixture.create_impl();
        let id_a = fresh_agent_id(fixture);
        let id_b = fresh_agent_id(fixture);
        assert_ne!(id_a, id_b, "RNG should produce distinct agent ids");

        let response_a = svc.commit_intention(intention(id_a, "from A")).await?;
        let response_b = svc.commit_intention(intention(id_b, "from B")).await?;
        anyhow::ensure!(
            response_a.log_position == response_b.log_position,
            "fresh agents should have independent logs with matching initial positions; got {} and {}",
            response_a.log_position,
            response_b.log_position,
        );
        Ok(())
    }

    /// The `log_position` returned by commits is a logical sequence number on
    /// the agent's log: successive intentions on the same agent must report
    /// strictly increasing, non-negative positions. Positions are per-agent, so
    /// we only assert monotonicity within a single agent.
    #[scenario]
    pub async fn run_test_commit_positions_increase<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let svc = fixture.create_impl();
        let agent_id = fresh_agent_id(fixture);

        let mut last = -1i64;
        for i in 0..5 {
            let response = svc
                .commit_intention(intention(agent_id.clone(), format!("intention {i}")))
                .await?;
            anyhow::ensure!(
                response.log_position > last,
                "commit_intention #{i} position {} must exceed previous {}",
                response.log_position,
                last,
            );
            last = response.log_position;
        }

        Ok(())
    }

    /// A commit must be visible through the AgentBus exposed by the same
    /// service. This is the cross-surface contract that replaces treating a
    /// `CommitSvc` implementation itself as an `AgentBus`.
    #[scenario]
    pub async fn run_test_committed_intention_is_visible_on_exposed_bus<
        F: CommitServiceTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        let svc = fixture.create_impl();
        let agent_id = fresh_agent_id(fixture);
        let body = "visible through agent_bus";
        let committed = svc
            .commit_intention(intention(agent_id.clone(), body))
            .await?;

        let polled = svc
            .agent_bus()
            .poll(PollRequest {
                bus_id: Some(BusId {
                    agent_bus_id: agent_id,
                }),
                start_log_position: committed.log_position,
                max_entries: 1,
                ..Default::default()
            })
            .await?;
        let entry = polled.entries.first().ok_or_else(|| {
            anyhow::anyhow!("exposed bus did not contain the committed intention")
        })?;
        anyhow::ensure!(
            entry.header.as_ref().map(|header| header.log_position) == Some(committed.log_position),
            "exposed bus returned a different log position",
        );
        let Some(payload::Payload::Intention(intention)) = entry
            .payload
            .as_ref()
            .and_then(|payload| payload.payload.as_ref())
        else {
            anyhow::bail!("entry at committed position was not an intention");
        };
        anyhow::ensure!(
            intention.intention.as_ref()
                == Some(&intention::Intention::StringIntention(body.to_string())),
            "exposed bus returned a different intention body",
        );
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_concurrent_intentions_succeed<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture,
        F::Impl: Clone,
    {
        commit_two_intentions_concurrently(fixture).await?;
        Ok(())
    }

    #[scenario_for(
        CommitServiceV1CountingVoterFixture<SimpleMemoryFixture>,
        suffix = v1_counting_voter_simple_memory,
        sim_only
    )]
    pub async fn run_test_concurrent_counting_voter_intentions<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture,
        F::Impl: Clone,
    {
        let (first, second) = commit_two_intentions_concurrently(fixture).await?;
        // This scenario is instantiated only with the modulus-2 counting-voter
        // fixture. Consecutive persisted counts produce opposite verdicts; matching
        // responses would indicate that both evaluations used the same stale count.
        anyhow::ensure!(
            first.approved != second.approved,
            "the modulus-2 voter must approve one concurrent intention and reject the other"
        );
        Ok(())
    }

    #[scenario_for(
        CommitServiceV1StorageConflictFixture<SimpleMemoryFixture>,
        suffix = v1_storage_conflict_simple_memory,
        sim_only
    )]
    pub async fn run_test_concurrent_intentions_survive_storage_conflicts<F>(
        fixture: &F,
    ) -> Result<()>
    where
        F: CommitServiceTestFixture,
        F::Impl: Clone,
    {
        const NUM_INTENTIONS: usize = 10;

        let service = fixture.create_impl();
        let agent_id = fresh_agent_id(fixture);
        service
            .commit_intention(intention(agent_id.clone(), "warmup"))
            .await?;

        let responses: Vec<CommitIntentionOutcome> = futures::stream::iter(0..NUM_INTENTIONS)
            .map(|index| {
                let service = service.clone();
                let agent_id = agent_id.clone();
                async move {
                    service
                        .commit_intention(intention(
                            agent_id,
                            format!("concurrent intention {index}"),
                        ))
                        .await
                }
            })
            .buffer_unordered(NUM_INTENTIONS)
            .try_collect()
            .await?;

        assert_eq!(responses.len(), NUM_INTENTIONS);
        let positions: HashSet<_> = responses
            .iter()
            .map(|response| response.log_position)
            .collect();
        assert_eq!(
            positions.len(),
            NUM_INTENTIONS,
            "concurrent intentions must occupy distinct log positions"
        );
        Ok(())
    }
}

pub use defs::*;
