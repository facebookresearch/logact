/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Commit-service replay scenarios over a pre-existing AgentBus backlog.

#[conformance_macros::scenarios(commit_service_replay_backlog_list)]
mod defs {
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::ActionOutput;
    use agent_bus_proto_rust::agent_bus::AppendRequest;
    use agent_bus_proto_rust::agent_bus::BusId;
    use agent_bus_proto_rust::agent_bus::Commit;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::AgentBus;
    use agentbus_api::Clock;
    use agentbus_api::Environment;
    use agentbus_tests::common::helpers::read_linearizable_snapshot;
    use anyhow::Result;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;

    use crate::fixtures::CommitServiceTestFixture;
    use crate::simulator::Simulator;

    const BUS_ID: &str = "replay-backlog-scenario";
    const WORKLOAD_LEN: usize = 95;
    const REPLAY_LATENCY_CEILING: Duration = Duration::from_secs(20);

    // An anonymized workload with 15 intentions, 73 commits, and 7 action outputs.
    const INTENTION_INDICES: [usize; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16];
    const ACTION_OUTPUT_INDICES: [usize; 7] = [0, 53, 54, 68, 78, 79, 87];

    fn commit_payload() -> Payload {
        Payload {
            payload: Some(payload::Payload::Commit(Commit {
                intention_id: -1,
                reason: "synthetic duplicate commit".to_string(),
            })),
        }
    }

    fn action_output_payload() -> Payload {
        Payload {
            payload: Some(payload::Payload::ActionOutput(ActionOutput {
                intention_id: -1,
                ..Default::default()
            })),
        }
    }

    fn intention_payload(workload_index: usize) -> Payload {
        Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention(format!(
                    "synthetic replay intention {workload_index}"
                ))),
                ..Default::default()
            })),
        }
    }

    async fn seed_workload(bus: &impl AgentBus) -> Result<(Vec<i64>, i64)> {
        let mut intention_log_positions = Vec::new();
        let mut tail = 0;
        for workload_index in 0..WORKLOAD_LEN {
            let is_intention = INTENTION_INDICES.contains(&workload_index);
            let payload = if ACTION_OUTPUT_INDICES.contains(&workload_index) {
                action_output_payload()
            } else if is_intention {
                intention_payload(workload_index)
            } else {
                commit_payload()
            };
            let appended = bus
                .append(AppendRequest {
                    agent_bus_id: BUS_ID.to_string(),
                    bus_id: Some(BusId {
                        agent_bus_id: BUS_ID.to_string(),
                    }),
                    payload: Some(payload),
                })
                .await?;
            tail = appended.log_position + 1;
            if is_intention {
                intention_log_positions.push(appended.log_position);
            }
        }
        Ok((intention_log_positions, tail))
    }

    async fn assert_replay_decisions<E: Environment>(
        bus: &impl AgentBus,
        environment: &E,
        seeded_intentions: &[i64],
        seeded_tail: i64,
        fresh_intention: i64,
    ) -> Result<()> {
        let (entries, _) =
            read_linearizable_snapshot(bus, BUS_ID.to_string(), 0, None, environment, Some(64))
                .await;
        let mut expected_intentions = seeded_intentions.to_vec();
        expected_intentions.push(fresh_intention);

        let decisions = entries
            .iter()
            .filter_map(|entry| {
                let decision_position = entry.header.as_ref()?.log_position;
                if decision_position < seeded_tail {
                    return None;
                }
                match entry.payload.as_ref()?.payload.as_ref()? {
                    payload::Payload::Commit(commit) => {
                        Some((commit.intention_id, true, decision_position))
                    }
                    payload::Payload::Abort(abort) => {
                        Some((abort.intention_id, false, decision_position))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();

        anyhow::ensure!(
            decisions.len() == expected_intentions.len(),
            "expected {} replay decisions, found {}: {decisions:?}",
            expected_intentions.len(),
            decisions.len(),
        );
        for intention_position in expected_intentions {
            let matching = decisions
                .iter()
                .filter(|(intention_id, _, _)| *intention_id == intention_position)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                matching.len() == 1,
                "intention at {intention_position} must have exactly one decision, found {matching:?}"
            );
            let (_, approved, decision_position) = matching[0];
            anyhow::ensure!(
                *decision_position > intention_position,
                "decision at {decision_position} must follow intention at {intention_position}"
            );
            anyhow::ensure!(
                *approved,
                "default-policy intention at {intention_position} must be committed"
            );
        }
        Ok(())
    }

    #[scenario_for(CommitServiceV1LatencyFixture, suffix = v1_latency, sim_only)]
    pub async fn run_test_commit_after_replay_backlog<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
    {
        let service = fixture.create_impl();
        let (seeded_intentions, seeded_tail) = seed_workload(service.agent_bus()).await?;

        let environment = fixture.get_env();
        let start = environment.with_clock(|clock| clock.monotonic_time());
        let outcome = service
            .commit_intention(CommitIntentionCommand {
                bus_id: agentbus_api::BusId {
                    agent_bus_id: BUS_ID.to_string(),
                },
                intention: intention::Intention::StringIntention(
                    "synthetic benchmark intention".to_string(),
                ),
            })
            .await?;
        let elapsed = environment.with_clock(|clock| clock.monotonic_time()) - start;

        anyhow::ensure!(
            outcome.approved,
            "the replayed intention should be approved"
        );
        anyhow::ensure!(
            elapsed <= REPLAY_LATENCY_CEILING,
            "replay took {elapsed:?}, exceeding {REPLAY_LATENCY_CEILING:?}"
        );
        assert_replay_decisions(
            service.agent_bus(),
            environment.as_ref(),
            &seeded_intentions,
            seeded_tail,
            outcome.log_position,
        )
        .await?;
        Ok(())
    }
}

pub use defs::*;
