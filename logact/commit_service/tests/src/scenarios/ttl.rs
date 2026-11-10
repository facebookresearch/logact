/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_ttl_list)]
mod defs {
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::Abort;
    use agent_bus_proto_rust::agent_bus::AppendRequest;
    use agent_bus_proto_rust::agent_bus::CheckTailRequest;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::ReadNextRequest;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::AgentBus;
    use agentbus_api::AgentBusError;
    use agentbus_api::BusId;
    use agentbus_api::Environment;
    use anyhow::Result;
    use logact_commit_service_api::CommitError;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use logact_commit_service_api::CreateBusCommand;
    use rand::RngExt as _;

    use crate::fixtures::CommitServiceTestFixture;
    use crate::fixtures::DeciderPolicyFixture;
    use crate::simulator::Simulator;

    fn command(bus_id: &str, value: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: BusId {
                agent_bus_id: bus_id.to_owned(),
            },
            intention: intention::Intention::StringIntention(value.to_owned()),
        }
    }

    async fn check_checkpointed_history_can_expire<F: CommitServiceTestFixture>(
        fixture: &F,
        allow_data_loss: bool,
    ) -> Result<()> {
        let env = fixture.get_env();
        let bus_id = format!("ttl-{}", env.with_rng(|rng| rng.random::<u64>()));
        let service = fixture.create_impl();
        for value in ["a", "b"] {
            assert!(
                service
                    .commit_intention(command(&bus_id, value))
                    .await?
                    .approved
            );
        }
        env.sleep(Duration::from_secs(18 * 24 * 60 * 60)).await;
        assert!(
            service
                .commit_intention(command(&bus_id, "c"))
                .await?
                .approved
        );
        env.sleep(Duration::from_secs(18 * 24 * 60 * 60)).await;
        assert!(matches!(
            service
                .agent_bus()
                .read_next(ReadNextRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                    start_log_position: 0,
                    end_log_position: 1,
                    max_entries: 1,
                    ..Default::default()
                })
                .await,
            Err(AgentBusError::PositionNotFound(_))
        ));
        match service.commit_intention(command(&bus_id, "d")).await {
            Ok(outcome) => assert!(outcome.approved),
            Err(CommitError::BusDataLoss(_)) if allow_data_loss => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    #[scenario(sim_only, ttl_ms = 2592000000, sticky = 1.0)]
    pub async fn run_test_checkpointed_history_can_expire<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        check_checkpointed_history_can_expire(fixture, false).await
    }

    #[scenario(sim_only, ttl_ms = 2592000000, sticky = 0.0)]
    pub async fn run_test_checkpointed_history_can_expire_with_misrouting<
        F: CommitServiceTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        check_checkpointed_history_can_expire(fixture, true).await
    }

    #[scenario(sim_only, ttl_ms = 1000, sticky = 1.0)]
    pub async fn run_test_replay_detects_expired_history<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let bus_id = format!("ttl-replay-{}", env.with_rng(|rng| rng.random::<u64>()));
        let service = fixture.create_impl();
        service
            .create_bus(CreateBusCommand {
                bus_id: bus_id.clone(),
            })
            .await?;
        let append = AppendRequest {
            bus_id: Some(BusId {
                agent_bus_id: bus_id.clone(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Abort(Abort {
                    intention_id: -1,
                    ..Default::default()
                })),
            }),
        };
        assert_eq!(
            service
                .agent_bus()
                .append(append.clone())
                .await?
                .log_position,
            1
        );
        env.sleep(Duration::from_millis(1200)).await;
        assert_eq!(service.agent_bus().append(append).await?.log_position, 2);
        assert!(matches!(
            service
                .commit_intention(command(&bus_id, "after expiry"))
                .await,
            Err(CommitError::BusDataLoss(_))
        ));
        assert_eq!(
            service
                .agent_bus()
                .check_tail(CheckTailRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id
                    }),
                })
                .await?
                .tail_position,
            4,
            "data loss must not append a fallback abort"
        );
        Ok(())
    }

    /// `create_impl()` may build a fresh server or return a handle to existing
    /// replicas. A fresh write-once bus can reclaim expired positions, so the new
    /// policy and intention advance its post-expiry tail by two. A bus retaining
    /// its append position writes past the expired entries, advancing the
    /// pre-expiry tail by two. Both must report `BusDataLoss` without a fallback
    /// abort.
    #[scenario(sim_only, ttl_ms = 1000, sticky = 1.0)]
    pub async fn run_test_detects_lost_bus_tail<F>(fixture: &F) -> Result<()>
    where
        F: DeciderPolicyFixture<Env = Simulator>,
    {
        let env = fixture.get_env();
        let bus_id = format!("ttl-policy-{}", env.with_rng(|rng| rng.random::<u64>()));
        let service = fixture.create_impl();
        service
            .create_bus(CreateBusCommand {
                bus_id: bus_id.clone(),
            })
            .await?;
        service
            .agent_bus()
            .append(AppendRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::Abort(Abort {
                        intention_id: -1,
                        ..Default::default()
                    })),
                }),
            })
            .await?;
        let tail_before_expiry = service
            .agent_bus()
            .check_tail(CheckTailRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
            })
            .await?
            .tail_position;
        env.sleep(Duration::from_millis(1200)).await;
        fixture
            .set_decider_policy(DeciderPolicy::OffByDefault)
            .await?;
        let fresh = fixture.create_impl();
        let tail = fresh
            .agent_bus()
            .check_tail(CheckTailRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
            })
            .await?
            .tail_position;
        assert!(matches!(
            fresh
                .commit_intention(command(&bus_id, "after expiry"))
                .await,
            Err(CommitError::BusDataLoss(_))
        ));
        let tail_after = fresh
            .agent_bus()
            .check_tail(CheckTailRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id,
                }),
            })
            .await?
            .tail_position;
        assert!(
            tail_after == tail + 2 || tail_after == tail_before_expiry + 2,
            "data loss must leave only the new policy and intention appended"
        );
        Ok(())
    }
}

pub use defs::*;
