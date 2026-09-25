/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_test_scenarios_list)]
mod defs {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::*;
    use agentbus_api::AgentBus;
    use agentbus_api::environment::Environment;
    use rand::RngExt as _;

    use crate::common::helpers::ALL_PAYLOAD_SELECTIVE_POLL_TYPES;
    use crate::common::helpers::append_commit;
    use crate::common::helpers::append_decider_policy;
    use crate::common::helpers::append_string_intention;
    use crate::common::helpers::append_vote;
    use crate::common::helpers::assert_read_response_invariants;
    use crate::common::helpers::payload_to_selective_poll_type;
    use crate::common::helpers::poll;
    use crate::common::helpers::poll_selective;
    use crate::common::helpers::read_linearizable_snapshot;
    use crate::common::helpers::selective_poll_type_to_payload;
    use crate::fixtures::AgentBusTestFixture;

    #[scenario]
    pub async fn run_test_typed_bus_id<F: AgentBusTestFixture>(fixture: &F) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id = BusId {
            agent_bus_id: format!(
                "typed-bus-{}",
                environment.with_rng(|rng| rng.random::<u64>())
            ),
        };
        let payload = Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention("test".to_owned())),
                ..Default::default()
            })),
        };

        let append = bus
            .append(AppendRequest {
                agent_bus_id: "ignored-legacy-id".to_owned(),
                bus_id: Some(bus_id.clone()),
                payload: Some(payload),
            })
            .await?;
        assert_eq!(append.log_position, 0);

        let poll = bus
            .poll(PollRequest {
                agent_bus_id: String::new(),
                bus_id: Some(bus_id.clone()),
                start_log_position: 0,
                max_entries: 1,
                filter: None,
            })
            .await?;
        assert_eq!(poll.entries.len(), 1);

        let tail = bus
            .check_tail(CheckTailRequest {
                agent_bus_id: String::new(),
                bus_id: Some(bus_id.clone()),
            })
            .await?;
        assert_eq!(tail.tail_position, 1);

        let read = bus
            .read_next(ReadNextRequest {
                agent_bus_id: String::new(),
                bus_id: Some(bus_id.clone()),
                start_log_position: 0,
                end_log_position: 1,
                max_entries: 1,
                filter: None,
            })
            .await?;
        assert_eq!(read.entries.len(), 1);

        let blocking = bus
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: String::new(),
                bus_id: Some(bus_id),
                start_log_position: 0,
                max_entries: 1,
                filter: None,
                timeout_ms: 0,
            })
            .await?;
        assert_eq!(blocking.entries.len(), 1);

        Ok(())
    }

    #[scenario]
    pub async fn run_test_bounds<F: AgentBusTestFixture>(fixture: &F) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let impl_instance = fixture.create_impl();
        let agent_bus_id: String =
            format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));
        let num_entries: usize = environment.with_rng(|rng| rng.random_range(5..15));
        for i in 1..=num_entries {
            append_string_intention(&impl_instance, agent_bus_id.clone(), format!("entry{}", i))
                .await;
        }

        let max_entries_request: i16 =
            environment.with_rng(|rng| rng.random_range((num_entries as i16)..100));
        let result = poll(&impl_instance, agent_bus_id.clone(), 0, max_entries_request).await;

        assert_eq!(
            result.entries.len(),
            num_entries,
            "Should retrieve all {} entries",
            num_entries
        );

        let result = poll(&impl_instance, agent_bus_id.clone(), 0, 0).await;

        assert_eq!(
            result.entries.len(),
            0,
            "maxEntries=0 should return no entries"
        );

        let limited_max_entries: i16 =
            environment.with_rng(|rng| rng.random_range(1..(num_entries as i16)));
        let result = poll(&impl_instance, agent_bus_id.clone(), 0, limited_max_entries).await;

        assert_eq!(
            result.entries.len(),
            limited_max_entries as usize,
            "Should respect maxEntries limit"
        );

        let beyond_offset: i64 = environment.with_rng(|rng| rng.random_range(1..20));
        let max_entries_beyond: i16 = environment.with_rng(|rng| rng.random_range(5..50));
        let result = poll(
            &impl_instance,
            agent_bus_id.clone(),
            num_entries as i64 + beyond_offset,
            max_entries_beyond,
        )
        .await;

        assert_eq!(
            result.entries.len(),
            0,
            "Polling beyond available entries should return empty"
        );

        let middle_position: i64 =
            environment.with_rng(|rng| rng.random_range(1..(num_entries as i64)));
        let max_entries_middle: i16 = environment.with_rng(|rng| rng.random_range(5..50));
        let result = poll(
            &impl_instance,
            agent_bus_id.clone(),
            middle_position,
            max_entries_middle,
        )
        .await;

        assert!(
            result.entries.len() <= (num_entries - middle_position as usize),
            "Should return entries from middle of log"
        );

        let different_agent_bus_id = format!("{}-different", agent_bus_id);
        let max_entries_diff: i16 = environment.with_rng(|rng| rng.random_range(5..50));
        let result = poll(&impl_instance, different_agent_bus_id, 0, max_entries_diff).await;

        assert_eq!(
            result.entries.len(),
            0,
            "Different agentBusId should have no entries"
        );

        let server_max_entries = 64;
        let extra_entries: usize = environment.with_rng(|rng| rng.random_range(10..50));
        let total_entries = server_max_entries + extra_entries;

        let test_agent_bus_id = format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));

        for i in 1..=total_entries {
            append_string_intention(
                &impl_instance,
                test_agent_bus_id.clone(),
                format!("entry{}", i),
            )
            .await;
        }

        let request_max_entries: i16 = environment.with_rng(|rng| rng.random_range(500..2000));
        let result = poll(
            &impl_instance,
            test_agent_bus_id.clone(),
            0,
            request_max_entries,
        )
        .await;

        assert_eq!(
            result.entries.len(),
            server_max_entries,
            "Server should cap at MAX_POLL_ENTRIES ({}), even when requesting more",
            server_max_entries
        );

        let request_max_entries_second: i16 =
            environment.with_rng(|rng| rng.random_range(500..2000));
        let result = poll(
            &impl_instance,
            test_agent_bus_id.clone(),
            server_max_entries as i64,
            request_max_entries_second,
        )
        .await;

        assert_eq!(
            result.entries.len(),
            extra_entries,
            "Should get remaining entries on second poll"
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_complete_flag<F: AgentBusTestFixture>(fixture: &F) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let impl_instance = fixture.create_impl();
        let agent_bus_id: String =
            format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));
        let num_intentions = 5;
        let num_votes = 3;

        for i in 0..num_intentions {
            append_string_intention(
                &impl_instance,
                agent_bus_id.clone(),
                format!("intention{}", i),
            )
            .await;
        }
        for i in 0..num_votes {
            append_vote(&impl_instance, agent_bus_id.clone(), i, i % 2 == 0).await;
        }

        let mut intention_filter = vec![];
        intention_filter.push(SelectivePollType::Intention as i32);

        let result = poll_selective(
            &impl_instance,
            agent_bus_id.clone(),
            0,
            num_intentions,
            intention_filter.clone(),
        )
        .await;
        assert_eq!(
            result.entries.len(),
            num_intentions as usize,
            "Should return all intentions"
        );
        assert!(
            result.complete,
            "Should be complete when all matching entries returned despite votes after"
        );

        let result =
            poll_selective(&impl_instance, agent_bus_id.clone(), 0, 3, intention_filter).await;
        assert_eq!(
            result.entries.len(),
            3,
            "Should respect maxEntries with filter"
        );
        assert!(
            !result.complete,
            "Should not be complete when limited by maxEntries with filter"
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_selective_poll<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let impl_instance = fixture.create_impl();
        let agent_bus_id: String =
            format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));
        let num_intentions: usize = environment.with_rng(|rng| rng.random_range(3..8));
        let num_votes: usize = environment.with_rng(|rng| rng.random_range(3..8));
        let num_policies: usize = environment.with_rng(|rng| rng.random_range(2..5));

        let mut intention_positions = Vec::new();
        let mut vote_positions = Vec::new();
        let mut policy_positions = Vec::new();

        for i in 0..num_intentions {
            let pos = append_string_intention(
                &impl_instance,
                agent_bus_id.clone(),
                format!("intention{}", i),
            )
            .await;
            intention_positions.push(pos);
        }

        for i in 0..num_votes {
            let intention_id = if !intention_positions.is_empty() {
                intention_positions[i % intention_positions.len()]
            } else {
                0
            };
            let vote = i % 2 == 0;
            let pos = append_vote(&impl_instance, agent_bus_id.clone(), intention_id, vote).await;
            vote_positions.push(pos);
        }

        for i in 0..num_policies {
            let policy = match i % 3 {
                0 => DeciderPolicy::OffByDefault as i32,
                1 => DeciderPolicy::OnByDefault as i32,
                _ => DeciderPolicy::FirstBooleanWins as i32,
            };
            let pos = append_decider_policy(&impl_instance, agent_bus_id.clone(), policy).await;
            policy_positions.push(pos);
        }

        let total_entries = num_intentions + num_votes + num_policies;

        let result = poll(&impl_instance, agent_bus_id.clone(), 0, 100).await;
        assert_eq!(
            result.entries.len(),
            total_entries,
            "Poll without filter should return all entries"
        );

        let mut intention_filter = vec![];
        intention_filter.push(SelectivePollType::Intention as i32);
        let result = poll_selective(
            &impl_instance,
            agent_bus_id.clone(),
            0,
            100,
            intention_filter,
        )
        .await;
        assert_eq!(
            result.entries.len(),
            num_intentions,
            "Selective poll should return only intentions"
        );
        for entry in &result.entries {
            assert!(
                matches!(&entry.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(_)))),
                "All entries should be intentions"
            );
        }

        let mut vote_filter = vec![];
        vote_filter.push(SelectivePollType::Vote as i32);
        let result =
            poll_selective(&impl_instance, agent_bus_id.clone(), 0, 100, vote_filter).await;
        assert_eq!(
            result.entries.len(),
            num_votes,
            "Selective poll should return only votes"
        );
        for entry in &result.entries {
            assert!(
                matches!(&entry.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Vote(_)))),
                "All entries should be votes"
            );
        }

        let mut policy_filter = vec![];
        policy_filter.push(SelectivePollType::DeciderPolicy as i32);
        let result =
            poll_selective(&impl_instance, agent_bus_id.clone(), 0, 100, policy_filter).await;
        assert_eq!(
            result.entries.len(),
            num_policies,
            "Selective poll should return only policies"
        );
        for entry in &result.entries {
            assert!(
                matches!(&entry.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::DeciderPolicy(_)))),
                "All entries should be policies"
            );
        }

        let mut intention_vote_filter = vec![];
        intention_vote_filter.push(SelectivePollType::Intention as i32);
        intention_vote_filter.push(SelectivePollType::Vote as i32);
        let result = poll_selective(
            &impl_instance,
            agent_bus_id.clone(),
            0,
            100,
            intention_vote_filter,
        )
        .await;
        assert_eq!(
            result.entries.len(),
            num_intentions + num_votes,
            "Selective poll should return intentions and votes"
        );
        for entry in &result.entries {
            assert!(
                matches!(&entry.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(_))))
                    || matches!(&entry.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Vote(_)))),
                "All entries should be intentions or votes"
            );
        }

        let empty_filter = vec![];
        let result =
            poll_selective(&impl_instance, agent_bus_id.clone(), 0, 100, empty_filter).await;
        assert_eq!(
            result.entries.len(),
            0,
            "Empty filter should return no entries"
        );

        let limited_max: i16 =
            environment.with_rng(|rng| rng.random_range(1..num_intentions as i16));
        let mut intention_filter = vec![];
        intention_filter.push(SelectivePollType::Intention as i32);
        let result = poll_selective(
            &impl_instance,
            agent_bus_id.clone(),
            0,
            limited_max,
            intention_filter,
        )
        .await;
        assert_eq!(
            result.entries.len(),
            limited_max as usize,
            "Selective poll should respect maxEntries"
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_with_decider<F: AgentBusTestFixture>(fixture: &F) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let decider_impl = fixture.create_impl();
        let agent_bus = fixture.create_impl();
        use agentbus_core::decider::Decider;
        use agentbus_core::vote_trackers::create_vote_tracker_for_decider_policy;

        let agent_bus_id: String =
            format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));

        append_decider_policy(
            &agent_bus,
            agent_bus_id.clone(),
            DeciderPolicy::OnByDefault as i32,
        )
        .await;

        let mut decider = Decider::new(
            decider_impl,
            agent_bus_id.clone(),
            0,
            create_vote_tracker_for_decider_policy,
        );

        for i in 1..=3 {
            append_string_intention(&agent_bus, agent_bus_id.clone(), format!("command{}", i))
                .await;
        }

        let entries_processed = decider
            .poll_and_decide(None)
            .await
            .expect("Decider should process entries");

        assert_eq!(entries_processed, 4, "Should process 4 entries");

        let (entries, tail) = read_linearizable_snapshot(
            &agent_bus,
            agent_bus_id.clone(),
            0,
            None,
            &*environment,
            None,
        )
        .await;

        assert_eq!(
            entries.len(),
            7,
            "Should have 1 policy + 3 intentions + 3 commits"
        );
        assert_eq!(tail, 7);
        assert_eq!(
        entries
            .iter()
            .filter(|e| matches!(&e.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Commit(_)))))
            .count(),
        3,
        "Should have 3 commits"
    );

        append_decider_policy(
            &agent_bus,
            agent_bus_id.clone(),
            DeciderPolicy::FirstBooleanWins as i32,
        )
        .await;

        let intention_pos1 =
            append_string_intention(&agent_bus, agent_bus_id.clone(), "vote1".to_string()).await;
        let intention_pos2 =
            append_string_intention(&agent_bus, agent_bus_id.clone(), "vote2".to_string()).await;

        decider
            .poll_and_decide(None)
            .await
            .expect("Decider should process policy change and intentions");

        append_vote(&agent_bus, agent_bus_id.clone(), intention_pos1, true).await;
        append_vote(&agent_bus, agent_bus_id.clone(), intention_pos2, false).await;

        decider
            .poll_and_decide(None)
            .await
            .expect("Decider should process votes");

        // 1 policy + 3 intentions + 3 commits + 1 policy + 2 intentions + 2 votes + 1 commit + 1 abort = 14
        let (entries2, tail_2) = read_linearizable_snapshot(
            &agent_bus,
            agent_bus_id.clone(),
            0,
            None,
            &*environment,
            None,
        )
        .await;
        assert_eq!(
        entries2
            .iter()
            .filter(|e| matches!(&e.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Commit(_)))))
            .count(),
        4,
        "Should have 4 total commits"
    );
        assert_eq!(
        entries2
            .iter()
            .filter(|e| matches!(&e.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Abort(_)))))
            .count(),
        1,
        "Should have 1 abort"
    );
        assert_eq!(tail_2, 14);

        // Test vote for non-existent intention
        let nonexistent_intention_id = 999999;
        let entries_before_len = entries2.len();

        append_vote(
            &agent_bus,
            agent_bus_id.clone(),
            nonexistent_intention_id,
            true,
        )
        .await;

        // Decider should process the vote without crashing
        decider
            .poll_and_decide(None)
            .await
            .expect("Decider should handle vote for non-existent intention gracefully");

        // Verify no commit or abort was created for the non-existent intention
        let (entries_after, tail_3) = read_linearizable_snapshot(
            &agent_bus,
            agent_bus_id.clone(),
            0,
            None,
            &*environment,
            None,
        )
        .await;
        assert_eq!(tail_3, tail_2 + 1);
        assert_eq!(
            entries_after.len(),
            entries_before_len + 1,
            "Should have only the vote entry added, no commit/abort"
        );

        // Verify the last entry is the vote (not a commit or abort)
        assert!(
            matches!(
                &entries_after.last().unwrap().payload,
                Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Vote(_)))
            ),
            "Last entry should be the vote for non-existent intention"
        );

        // Verify still only 4 commits and 1 abort (no new decisions)
        assert_eq!(
        entries_after
            .iter()
            .filter(|e| matches!(&e.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Commit(_)))))
            .count(),
        4,
        "Should still have only 4 commits"
    );
        assert_eq!(
        entries_after
            .iter()
            .filter(|e| matches!(&e.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Abort(_)))))
            .count(),
        1,
        "Should still have only 1 abort"
    );
        Ok(())
    }

    /// Test that verifies every payload-bearing SelectivePollType has a corresponding Payload
    /// sample. Exhaustive matching in payload_to_selective_poll_type catches new Payload
    /// variants at compile time.
    #[test]
    fn test_selective_poll_type_coverage() {
        use crate::common::helpers::selective_poll_type_to_payload;

        for &poll_type in ALL_PAYLOAD_SELECTIVE_POLL_TYPES {
            let payload = selective_poll_type_to_payload(poll_type).unwrap_or_else(|| {
                panic!(
                    "No corresponding Payload variant found for SelectivePollType::{:?}. \
             Please add a mapping in selective_poll_type_to_payload.",
                    poll_type
                )
            });

            // Verify the reverse mapping works correctly
            let mapped_poll_type = payload_to_selective_poll_type(&payload);
            assert_eq!(
                mapped_poll_type, poll_type,
                "Payload created for {:?} maps back to {:?} instead",
                poll_type, mapped_poll_type
            );
        }
    }

    /// Test that appends a representative payload for each payload-bearing SelectivePollType.
    #[scenario]
    pub async fn run_test_all_payload_types<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let impl_instance = fixture.create_impl();
        let agent_bus_id: String =
            format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));

        let payloads = ALL_PAYLOAD_SELECTIVE_POLL_TYPES
            .iter()
            .map(|&poll_type| {
                selective_poll_type_to_payload(poll_type).unwrap_or_else(|| {
                    panic!(
                        "No corresponding Payload variant found for SelectivePollType::{:?}. \
                         Please add a mapping in selective_poll_type_to_payload.",
                        poll_type
                    )
                })
            })
            .collect::<Vec<_>>();
        let payload_count = payloads.len();

        for payload in payloads {
            let request = agent_bus_proto_rust::agent_bus::AppendRequest {
                agent_bus_id: agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: agent_bus_id.clone(),
                }),
                payload: Some(payload),
                ..Default::default()
            };
            impl_instance
                .append(request)
                .await
                .expect("Append should succeed");
        }

        let (all_entries, tail) = read_linearizable_snapshot(
            &impl_instance,
            agent_bus_id.clone(),
            0,
            None,
            &*environment,
            None,
        )
        .await;
        assert_eq!(all_entries.len(), payload_count);
        assert_eq!(tail, payload_count as i64);

        for entry in &all_entries {
            let poll_type = payload_to_selective_poll_type(entry.payload.as_ref().unwrap());
            let filter = Some(PayloadTypeFilter {
                payload_types: vec![poll_type],
            });

            let (filtered, _) = read_linearizable_snapshot(
                &impl_instance,
                agent_bus_id.clone(),
                0,
                filter,
                &*environment,
                None,
            )
            .await;

            assert_eq!(
                filtered.len(),
                1,
                "Should return exactly one entry for {:?}",
                poll_type
            );

            assert_eq!(
                payload_to_selective_poll_type(filtered[0].payload.as_ref().unwrap()),
                poll_type,
                "Returned entry should match the requested poll type"
            );
        }
        Ok(())
    }

    /// Test that appended entries have non-zero rt_timestamp_ms and that timestamps are persistent
    /// across multiple polls of the same log positions.
    #[scenario(int_only)]
    pub async fn run_test_rt_timestamp<F: AgentBusTestFixture>(fixture: &F) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let impl_instance = fixture.create_impl();
        let agent_bus_id: String =
            format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));

        let num_entries: usize = environment.with_rng(|rng| rng.random_range(3..8));
        for i in 0..num_entries {
            append_string_intention(
                &impl_instance,
                agent_bus_id.clone(),
                format!("ts-test-{}", i),
            )
            .await;
        }

        // First read: verify all entries have non-zero timestamps
        let (entries1, tail_1) = read_linearizable_snapshot(
            &impl_instance,
            agent_bus_id.clone(),
            0,
            None,
            &*environment,
            None,
        )
        .await;
        assert_eq!(entries1.len(), num_entries);
        assert_eq!(tail_1, num_entries as i64);

        let timestamps: Vec<i64> = entries1
            .iter()
            .map(|e| {
                let h = e.header.as_ref().expect("entry should have header");
                assert!(
                    h.rt_timestamp_ms > 0,
                    "rt_timestamp_ms should be positive for entry at position {}",
                    h.log_position
                );
                h.rt_timestamp_ms
            })
            .collect();

        // Second read: verify timestamps are identical
        let (entries2, tail_2) = read_linearizable_snapshot(
            &impl_instance,
            agent_bus_id.clone(),
            0,
            None,
            &*environment,
            None,
        )
        .await;
        assert_eq!(entries2.len(), num_entries);
        assert_eq!(tail_2, tail_1);

        for (i, entry) in entries2.iter().enumerate() {
            let h = entry.header.as_ref().expect("entry should have header");
            assert_eq!(
                h.rt_timestamp_ms, timestamps[i],
                "rt_timestamp_ms should be persistent across reads for position {}",
                h.log_position
            );
        }
        Ok(())
    }

    /// Test that validates bus ID string validation is enforced across all AgentBus implementations.
    #[scenario]
    pub async fn run_test_bus_id_validation<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        use agentbus_api::validation::MAX_BUS_ID_LEN;
        use agentbus_api::validation::is_valid_bus_id_char;

        let impl_instance = fixture.create_impl();

        let make_append_request = |bus_id: String| {
            agent_bus_proto_rust::agent_bus::AppendRequest {
        agent_bus_id: bus_id.clone(),
        bus_id: Some(BusId {
            agent_bus_id: bus_id,
        }),
        payload: Some(agent_bus_proto_rust::agent_bus::Payload {
            payload: Some(
                agent_bus_proto_rust::agent_bus::payload::Payload::Intention(
                    agent_bus_proto_rust::agent_bus::Intention {
                        intention: Some(
                            agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(
                                "test".to_string(),
                            ),
                        ),
                        ..Default::default()
                    },
                ),
            ),
        }),
        ..Default::default()
    }
        };

        let make_poll_request = |bus_id: String| agent_bus_proto_rust::agent_bus::PollRequest {
            agent_bus_id: bus_id.clone(),
            bus_id: Some(BusId {
                agent_bus_id: bus_id,
            }),
            ..Default::default()
        };

        // Test valid bus IDs
        let max_length_id = "a".repeat(MAX_BUS_ID_LEN);
        let valid_ids = vec![
            "a",
            "valid-bus-id",
            "valid_bus_id",
            "valid.bus.id",
            "valid/bus/id",
            "a1b2c3",
            "MixedCase123",
            &max_length_id, // exactly at max length
        ];
        for valid_id in valid_ids {
            let result =
                append_string_intention(&impl_instance, valid_id.to_string(), "test".to_string())
                    .await;
            assert!(
                result >= 0,
                "Valid bus ID '{}' should be accepted",
                valid_id
            );
        }

        // Test empty bus ID (should fail)
        let empty_result = impl_instance
            .append(make_append_request("".to_string()))
            .await;
        assert!(empty_result.is_err(), "Empty bus ID should be rejected");
        let empty_poll = impl_instance.poll(make_poll_request("".to_string())).await;
        assert!(
            empty_poll.is_err(),
            "Empty bus ID should be rejected by poll"
        );

        // Test bus ID exceeding max length (should fail)
        let long_id = "a".repeat(MAX_BUS_ID_LEN + 1);
        let long_result = impl_instance
            .append(make_append_request(long_id.clone()))
            .await;
        assert!(
            long_result.is_err(),
            "Bus ID exceeding max length should be rejected"
        );
        let long_poll = impl_instance.poll(make_poll_request(long_id)).await;
        assert!(
            long_poll.is_err(),
            "Bus ID exceeding max length should be rejected by poll"
        );

        // Test invalid characters (should fail)
        // Check all possible byte values to find invalid characters
        let invalid_chars: Vec<char> = (0u8..=255u8)
            .filter_map(|b| {
                let c = b as char;
                if !is_valid_bus_id_char(c) {
                    Some(c)
                } else {
                    None
                }
            })
            .collect();
        assert!(
            invalid_chars.contains(&'#'),
            "Test setup error: should have '#' as invalid character"
        );

        for invalid_char in invalid_chars {
            let invalid_id = format!("bus{}id", invalid_char);
            let append_result = impl_instance
                .append(make_append_request(invalid_id.clone()))
                .await;
            assert!(
                append_result.is_err(),
                "Bus ID with invalid character '{}' (byte {}) should be rejected by append",
                invalid_char.escape_debug(),
                invalid_char as u8
            );
            let poll_result = impl_instance.poll(make_poll_request(invalid_id)).await;
            assert!(
                poll_result.is_err(),
                "Bus ID with invalid character '{}' (byte {}) should be rejected by poll",
                invalid_char.escape_debug(),
                invalid_char as u8
            );
        }
        Ok(())
    }

    /// Test that check_tail returns 0 for an empty/nonexistent bus.
    #[scenario]
    pub async fn run_test_check_tail_empty_bus<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));

        let result = bus
            .check_tail(CheckTailRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
            })
            .await?;
        assert_eq!(
            result.tail_position, 0,
            "Empty bus should have tail_position 0"
        );

        Ok(())
    }

    /// Test that check_tail returns the correct position after each append.
    #[scenario]
    pub async fn run_test_check_tail_after_appends<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));

        let num_entries: usize = environment.with_rng(|rng| rng.random_range(3..10));
        for i in 0..num_entries {
            append_string_intention(&bus, bus_id.clone(), format!("entry-{}", i)).await;
            let expected_tail = (i + 1) as i64;

            let result = bus
                .check_tail(CheckTailRequest {
                    agent_bus_id: bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                })
                .await?;
            assert_eq!(
                result.tail_position, expected_tail,
                "after append {}, check_tail should return {}",
                i, expected_tail
            );
        }
        Ok(())
    }

    /// Test that check_tail is monotonically non-decreasing under concurrent writes,
    /// and never exceeds the total number of appends.
    #[scenario]
    pub async fn run_test_check_tail_monotonic_under_concurrent_writes<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let write_bus = fixture.create_impl();
        let tail_bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));
        let total_writes: usize = 50;

        let writes_done = Rc::new(Cell::new(false));

        let write_bus_id = bus_id.clone();
        let writes_done_w = writes_done.clone();
        let write_env = env.clone();
        let writer = async move {
            for i in 0..total_writes {
                append_string_intention(&write_bus, write_bus_id.clone(), format!("w-{}", i)).await;
                write_env.sleep(Duration::from_millis(1)).await;
            }
            writes_done_w.set(true);
        };

        let tail_bus_id = bus_id.clone();
        let writes_done_r = writes_done.clone();
        let tail_env = env.clone();
        let reader = async move {
            let mut prev_tail: i64 = 0;
            loop {
                let tail_position = tail_bus
                    .check_tail(CheckTailRequest {
                        agent_bus_id: tail_bus_id.clone(),
                        bus_id: Some(BusId {
                            agent_bus_id: tail_bus_id.clone(),
                        }),
                    })
                    .await
                    .expect("check_tail should not fail")
                    .tail_position;
                assert!(
                    tail_position >= prev_tail,
                    "tail went backwards: {} -> {}",
                    prev_tail,
                    tail_position
                );
                assert!(
                    tail_position <= total_writes as i64,
                    "tail {} exceeds total writes {}",
                    tail_position,
                    total_writes
                );
                prev_tail = tail_position;
                if writes_done_r.get() {
                    break;
                }
                tail_env.sleep(Duration::from_millis(1)).await;
            }
        };

        futures::join!(writer, reader);

        let final_bus = fixture.create_impl();
        let final_tail = final_bus
            .check_tail(CheckTailRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id,
                }),
            })
            .await?;
        assert_eq!(
            final_tail.tail_position, total_writes as i64,
            "final tail should equal total writes"
        );
        Ok(())
    }

    /// Test that check_tail tracks each bus independently.
    #[scenario]
    pub async fn run_test_check_tail_bus_isolation<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_a: String = format!("bus-a-{}", env.with_rng(|rng| rng.random::<u64>()));
        let bus_b: String = format!("bus-b-{}", env.with_rng(|rng| rng.random::<u64>()));

        let count_a: usize = env.with_rng(|rng| rng.random_range(3..8));
        let count_b: usize = env.with_rng(|rng| rng.random_range(5..12));

        for i in 0..count_a {
            append_string_intention(&bus, bus_a.clone(), format!("a-{}", i)).await;
        }
        for i in 0..count_b {
            append_string_intention(&bus, bus_b.clone(), format!("b-{}", i)).await;
        }

        let tail_a = bus
            .check_tail(CheckTailRequest {
                agent_bus_id: bus_a.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_a.clone(),
                }),
            })
            .await?;
        let tail_b = bus
            .check_tail(CheckTailRequest {
                agent_bus_id: bus_b.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_b.clone(),
                }),
            })
            .await?;
        assert_eq!(tail_a.tail_position, count_a as i64);
        assert_eq!(tail_b.tail_position, count_b as i64);
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // read_next tests
    // ---------------------------------------------------------------------------

    /// Append N entries, drain them all via read_next (randomly bounded or unbounded).
    #[scenario]
    pub async fn run_test_read_next_basic<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        let n: usize = env.with_rng(|rng| rng.random_range(3..64));
        for i in 0..n {
            append_string_intention(&bus, bus_id.clone(), format!("entry-{}", i)).await;
        }

        let (entries, tail) =
            read_linearizable_snapshot(&bus, bus_id.clone(), 0, None, &*env, None).await;
        assert_eq!(entries.len(), n, "should return all entries");
        assert_eq!(tail, n as i64);

        Ok(())
    }

    /// Filter for one type, verify only matching entries returned.
    #[scenario]
    pub async fn run_test_read_next_with_filter<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        append_string_intention(&bus, bus_id.clone(), "i0".to_string()).await;
        append_vote(&bus, bus_id.clone(), 0, true).await;
        append_string_intention(&bus, bus_id.clone(), "i1".to_string()).await;
        append_vote(&bus, bus_id.clone(), 2, false).await;

        let filter = Some(PayloadTypeFilter {
            payload_types: vec![SelectivePollType::Intention as i32],
        });

        // tail should be 4 (total entries), not 2 (filtered count)
        let (entries, tail) =
            read_linearizable_snapshot(&bus, bus_id.clone(), 0, filter, &*env, None).await;
        assert_eq!(entries.len(), 2, "should return only 2 intentions");
        assert_eq!(tail, 4);

        Ok(())
    }

    /// max_entries=1 pagination: each call returns exactly 1 match.
    #[scenario]
    pub async fn run_test_read_next_pagination<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        // intention, vote, intention, vote, intention
        append_string_intention(&bus, bus_id.clone(), "i0".to_string()).await;
        append_vote(&bus, bus_id.clone(), 0, true).await;
        append_string_intention(&bus, bus_id.clone(), "i1".to_string()).await;
        append_vote(&bus, bus_id.clone(), 2, false).await;
        append_string_intention(&bus, bus_id.clone(), "i2".to_string()).await;

        let filter = Some(PayloadTypeFilter {
            payload_types: vec![SelectivePollType::Intention as i32],
        });

        let (entries, tail) =
            read_linearizable_snapshot(&bus, bus_id.clone(), 0, filter, &*env, Some(1)).await;
        assert_eq!(entries.len(), 3, "should see all 3 intentions");
        assert_eq!(tail, 5);
        let positions: Vec<i64> = entries
            .iter()
            .map(|e| e.header.as_ref().unwrap().log_position)
            .collect();
        assert_eq!(positions, vec![0, 2, 4]);

        Ok(())
    }

    /// Filter that matches nothing: repeated read_next calls must make strict
    /// forward progress on every iteration until the range is exhausted.
    #[scenario]
    pub async fn run_test_read_next_forward_progress<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        let n: usize = env.with_rng(|rng| rng.random_range(50..150));
        for i in 0..n {
            append_string_intention(&bus, bus_id.clone(), format!("intention-{}", i)).await;
        }

        let tail = bus
            .check_tail(CheckTailRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
            })
            .await?
            .tail_position;
        let vote_filter = Some(PayloadTypeFilter {
            payload_types: vec![SelectivePollType::Vote as i32],
        });
        let page_size: i32 = env.with_rng(|rng| 1 << rng.random_range(0..8));

        let mut cursor = 0i64;
        while cursor < tail {
            let resp = bus
                .read_next(ReadNextRequest {
                    agent_bus_id: bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                    start_log_position: cursor,
                    end_log_position: tail,
                    max_entries: page_size,
                    filter: vote_filter.clone(),
                })
                .await?;
            assert_read_response_invariants(
                &resp.entries,
                cursor,
                resp.next_start_position,
                Some(tail),
            );
            assert!(resp.entries.is_empty());
            assert!(
                resp.next_start_position > cursor,
                "cursor must strictly advance, was {} got {}",
                cursor,
                resp.next_start_position
            );
            cursor = resp.next_start_position;
        }

        Ok(())
    }

    /// Rejects invalid arguments. Each case has exactly one thing wrong.
    #[scenario]
    pub async fn run_test_read_next_validates_args<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let bus = fixture.create_impl();
        let bus_id = "bus-v".to_string();

        // Seed the bus so tail=10.
        for i in 0..10 {
            append_string_intention(&bus, bus_id.clone(), format!("e-{}", i)).await;
        }

        let cases: Vec<(&str, ReadNextRequest)> = vec![
            (
                "max_entries=0",
                ReadNextRequest {
                    agent_bus_id: bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                    start_log_position: 0,
                    end_log_position: 10,
                    max_entries: 0,
                    filter: None,
                },
            ),
            (
                "max_entries=-1",
                ReadNextRequest {
                    agent_bus_id: bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                    start_log_position: 0,
                    end_log_position: 10,
                    max_entries: -1,
                    filter: None,
                },
            ),
            (
                "start=-1",
                ReadNextRequest {
                    agent_bus_id: bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                    start_log_position: -1,
                    end_log_position: 10,
                    max_entries: 10,
                    filter: None,
                },
            ),
            (
                "end < start",
                ReadNextRequest {
                    agent_bus_id: bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                    start_log_position: 5,
                    end_log_position: 3,
                    max_entries: 10,
                    filter: None,
                },
            ),
            (
                "empty filter",
                ReadNextRequest {
                    agent_bus_id: bus_id.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.clone(),
                    }),
                    start_log_position: 0,
                    end_log_position: 10,
                    max_entries: 10,
                    filter: Some(PayloadTypeFilter {
                        payload_types: vec![],
                    }),
                },
            ),
        ];

        for (label, req) in cases {
            assert!(
                bus.read_next(req).await.is_err(),
                "{} should be rejected",
                label
            );
        }
        Ok(())
    }

    /// timeout=0 on empty bus returns empty immediately.
    #[scenario]
    pub async fn run_test_read_next_empty<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        let resp = bus
            .read_next(ReadNextRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
                start_log_position: 0,
                end_log_position: 0,
                max_entries: 10,
                filter: None,
            })
            .await?;
        assert_read_response_invariants(&resp.entries, 0, resp.next_start_position, Some(0));
        assert!(resp.entries.is_empty());
        assert_eq!(resp.next_start_position, 0);

        Ok(())
    }

    /// Multi-type filter: filter for intentions + votes, verify commits/aborts excluded.
    #[scenario]
    pub async fn run_test_read_next_multi_type_filter<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        append_string_intention(&bus, bus_id.clone(), "i0".to_string()).await;
        append_vote(&bus, bus_id.clone(), 0, true).await;
        append_commit(&bus, bus_id.clone(), 0, "approved").await;
        append_string_intention(&bus, bus_id.clone(), "i1".to_string()).await;

        let filter = Some(PayloadTypeFilter {
            payload_types: vec![
                SelectivePollType::Intention as i32,
                SelectivePollType::Vote as i32,
            ],
        });

        let (entries, tail) =
            read_linearizable_snapshot(&bus, bus_id.clone(), 0, filter, &*env, None).await;
        assert_eq!(entries.len(), 3, "should return 2 intentions + 1 vote");
        assert_eq!(tail, 4);

        // Verify the commit at position 2 was skipped
        let positions: Vec<i64> = entries
            .iter()
            .map(|e| e.header.as_ref().unwrap().log_position)
            .collect();
        assert_eq!(positions, vec![0, 1, 3]);

        Ok(())
    }

    /// Bus isolation: appends to bus B don't appear in reads from bus A.
    #[scenario]
    pub async fn run_test_read_next_bus_isolation<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_a: String = format!("bus-a-{}", env.with_rng(|rng| rng.random::<u64>()));
        let bus_b: String = format!("bus-b-{}", env.with_rng(|rng| rng.random::<u64>()));

        // Write to both buses
        for i in 0..3 {
            append_string_intention(&bus, bus_a.clone(), format!("a-{}", i)).await;
        }
        for i in 0..5 {
            append_string_intention(&bus, bus_b.clone(), format!("b-{}", i)).await;
        }

        // Read from bus A — should only see bus A's entries
        let (entries, tail) =
            read_linearizable_snapshot(&bus, bus_a.clone(), 0, None, &*env, None).await;
        assert_eq!(entries.len(), 3);
        assert_eq!(tail, 3);

        Ok(())
    }

    // ---------------------------------------------------------------------------
    // blocking_poll tests
    // ---------------------------------------------------------------------------

    /// Test that blocking_poll returns empty after timeout on an empty bus.
    #[scenario(sim_only)]
    pub async fn run_test_blocking_poll_timeout<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        let timeout_ms = env.with_rng(|rng| rng.random_range(0..=5));
        let resp = bus
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id,
                }),
                start_log_position: 0,
                max_entries: 100,
                filter: None,
                timeout_ms,
            })
            .await?;

        assert_read_response_invariants(&resp.entries, 0, resp.next_start_position, None);
        assert!(resp.entries.is_empty());
        assert_eq!(resp.next_start_position, 0);
        Ok(())
    }

    /// Test that blocking_poll rejects invalid arguments.
    #[scenario(sim_only)]
    pub async fn run_test_blocking_poll_validates_args<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let bus = fixture.create_impl();
        let bus_id = "bus-v".to_string();

        assert!(
            bus.blocking_poll(BlockingPollRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
                start_log_position: 0,
                max_entries: 0,
                filter: None,
                timeout_ms: 0,
            })
            .await
            .is_err(),
            "max_entries=0 should be rejected"
        );

        assert!(
            bus.blocking_poll(BlockingPollRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
                start_log_position: 0,
                max_entries: 10,
                filter: None,
                timeout_ms: -1,
            })
            .await
            .is_err(),
            "timeout_ms=-1 should be rejected"
        );

        Ok(())
    }

    /// Test that blocking_poll reads already-available entries without relying on timeout behavior.
    #[scenario]
    pub async fn run_test_blocking_poll_ready_entries<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));

        append_string_intention(&bus, bus_id.clone(), "i0".to_string()).await;
        append_commit(&bus, bus_id.clone(), 0, "approved").await;
        append_vote(&bus, bus_id.clone(), 0, true).await;
        append_string_intention(&bus, bus_id.clone(), "i1".to_string()).await;

        let filter = Some(PayloadTypeFilter {
            payload_types: vec![
                SelectivePollType::Intention as i32,
                SelectivePollType::Vote as i32,
            ],
        });

        let first = bus
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.clone(),
                }),
                start_log_position: 0,
                max_entries: 2,
                filter: filter.clone(),
                timeout_ms: 0,
            })
            .await?;
        assert_read_response_invariants(&first.entries, 0, first.next_start_position, None);
        assert_eq!(
            first.entries.len(),
            2,
            "should stop after max_entries matching payloads"
        );
        assert_eq!(first.next_start_position, 3);
        let first_positions: Vec<i64> = first
            .entries
            .iter()
            .map(|entry| entry.header.as_ref().unwrap().log_position)
            .collect();
        assert_eq!(first_positions, vec![0, 2]);
        let first_payload_types: Vec<i32> = first
            .entries
            .iter()
            .map(|entry| payload_to_selective_poll_type(entry.payload.as_ref().unwrap()))
            .collect();
        assert_eq!(
            first_payload_types,
            vec![
                SelectivePollType::Intention as i32,
                SelectivePollType::Vote as i32,
            ]
        );

        let second = bus
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id,
                }),
                start_log_position: first.next_start_position,
                max_entries: 2,
                filter,
                timeout_ms: 0,
            })
            .await?;
        assert_read_response_invariants(
            &second.entries,
            first.next_start_position,
            second.next_start_position,
            None,
        );
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.next_start_position, 4);
        assert_eq!(
            second
                .entries
                .first()
                .unwrap()
                .header
                .as_ref()
                .unwrap()
                .log_position,
            3
        );
        assert_eq!(
            payload_to_selective_poll_type(
                second.entries.first().unwrap().payload.as_ref().unwrap()
            ),
            SelectivePollType::Intention as i32
        );

        Ok(())
    }

    /// Test monotonic cursor advancement under concurrent writes using blocking_poll.
    /// Reader keeps calling blocking_poll until it has seen every entry.
    #[scenario(sim_only)]
    pub async fn run_test_blocking_poll_monotonic_under_concurrent_writes<
        F: AgentBusTestFixture,
    >(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let env = fixture.get_env();
        let write_bus = fixture.create_impl();
        let poll_bus = fixture.create_impl();
        let bus_id: String = format!("bus-{}", env.with_rng(|rng| rng.random::<u64>()));
        let total_writes: usize = env.with_rng(|rng| rng.random_range(50..=100));

        let write_bus_id = bus_id.clone();
        let write_env = env.clone();
        let writer = async move {
            for i in 0..total_writes {
                append_string_intention(&write_bus, write_bus_id.clone(), format!("w-{}", i)).await;
                write_env.sleep(Duration::from_millis(1)).await;
            }
        };

        let poll_bus_id = bus_id.clone();
        let poll_env = env.clone();
        let reader = async move {
            let mut cursor = 0i64;
            let mut total_read = 0;
            while total_read < total_writes {
                let timeout_ms = poll_env.with_rng(|rng| rng.random_range(0..=3));
                let max_entries = poll_env.with_rng(|rng| rng.random_range(1..=5));
                let resp = poll_bus
                    .blocking_poll(BlockingPollRequest {
                        agent_bus_id: poll_bus_id.clone(),
                        bus_id: Some(BusId {
                            agent_bus_id: poll_bus_id.clone(),
                        }),
                        start_log_position: cursor,
                        max_entries,
                        filter: None,
                        timeout_ms,
                    })
                    .await
                    .expect("blocking_poll should not fail");
                assert_read_response_invariants(
                    &resp.entries,
                    cursor,
                    resp.next_start_position,
                    None,
                );
                total_read += resp.entries.len();
                cursor = resp.next_start_position;
            }
            assert_eq!(total_read, total_writes, "reader should see all entries");
        };

        futures::join!(writer, reader);
        Ok(())
    }
}
pub use defs::*;
