/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_checkpoints_list)]
mod defs {
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::AgentBus;
    use agentbus_api::AppendRequest;
    use agentbus_api::Environment;
    use agentbus_tests::common::helpers::read_linearizable_snapshot;
    use agentbus_tests::decisions::first_decisions;
    use anyhow::Result;
    use futures::future::Either;
    use futures::future::select;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::CheckpointFixture;
    use crate::fixtures::DeciderPolicyFixture;

    fn intention(bus_id: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: bus_id.to_string(),
            },
            intention: intention::Intention::StringIntention("checkpoint probe".to_string()),
        }
    }

    /// An intention appended without a commit, as if its commit crashed right
    /// after appending it.
    async fn append_raw_intention(bus: &impl AgentBus, bus_id: &str) -> Result<()> {
        bus.append(AppendRequest {
            bus_id: Some(agentbus_api::BusId {
                agent_bus_id: bus_id.to_string(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention("crashed".to_string())),
                    ..Default::default()
                })),
            }),
        })
        .await?;
        Ok(())
    }

    /// Every intention at or before `position` has a decision, anywhere in the
    /// log: a `Commit`, an `Abort`, or a correct speculative decision.
    async fn assert_prefix_decided(
        bus: &impl AgentBus,
        bus_id: &str,
        position: i64,
        allow_pending: bool,
        environment: &impl Environment,
    ) -> Result<()> {
        let (entries, _) =
            read_linearizable_snapshot(bus, bus_id.to_string(), 0, None, environment, Some(64))
                .await;
        let payloads = entries.iter().filter_map(|entry| {
            Some((
                entry.header.as_ref()?.log_position,
                entry.payload.as_ref()?.payload.as_ref()?,
            ))
        });
        let decided = first_decisions(&entries);
        let latest = payloads
            .clone()
            .filter(|(intention_position, payload)| {
                *intention_position <= position && matches!(payload, payload::Payload::Intention(_))
            })
            .map(|(intention_position, _)| intention_position)
            .max();
        for (intention_position, payload) in payloads {
            anyhow::ensure!(
                intention_position > position
                    || !matches!(payload, payload::Payload::Intention(_))
                    || decided.contains_key(&intention_position)
                    || (allow_pending && Some(intention_position) == latest),
                "checkpoint at {position} covers undecided intention at {intention_position}"
            );
        }
        Ok(())
    }

    /// Under deciders that need no votes, a checkpoint only ever covers
    /// intentions with a logged decision, however commits interleave or crash.
    /// Under `FIRST_BOOLEAN_WINS`, the latest intention it covers may still be
    /// pending, waiting for a vote; every earlier one is decided or superseded.
    #[scenario(sim_only)]
    pub async fn run_test_checkpoints_cover_only_decided_intentions<F>(fixture: &F) -> Result<()>
    where
        F: CheckpointFixture + DeciderPolicyFixture,
    {
        let environment = fixture.get_env();
        for policy in [
            DeciderPolicy::OnByDefault,
            DeciderPolicy::OffByDefault,
            DeciderPolicy::FirstBooleanWins,
        ]
        .into_iter()
        .filter(|policy| {
            *policy != DeciderPolicy::FirstBooleanWins || fixture.supports_first_boolean_wins()
        }) {
            fixture.set_decider_policy(policy).await?;
            let servers: Vec<_> = (0..3).map(|_| fixture.create_impl()).collect();
            let bus_id = format!(
                "checkpoints-{}",
                environment.with_rng(|rng| rng.random::<u64>())
            );
            for server in &servers {
                server.commit_intention(intention(&bus_id)).await?;
            }
            for _ in 0..20 {
                let (server, step, cutoff) = environment.with_rng(|rng| {
                    (
                        rng.random_range(0..servers.len()),
                        rng.random_range(0..3),
                        Duration::from_millis(rng.random_range(0..50)),
                    )
                });
                let server = &servers[server];
                match step {
                    0 => {
                        server.commit_intention(intention(&bus_id)).await?;
                    }
                    1 => append_raw_intention(server.agent_bus(), &bus_id).await?,
                    _ => {
                        if let Either::Left((result, _)) = select(
                            Box::pin(server.commit_intention(intention(&bus_id))),
                            Box::pin(environment.sleep(cutoff)),
                        )
                        .await
                        {
                            result?;
                        }
                    }
                }
                for server in &servers {
                    let position = fixture.checkpoint_position(server, &bus_id).await?;
                    assert_prefix_decided(
                        server.agent_bus(),
                        &bus_id,
                        position,
                        policy == DeciderPolicy::FirstBooleanWins,
                        environment.as_ref(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
}

pub use defs::*;
