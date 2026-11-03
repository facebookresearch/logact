/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_sim_only_scenarios_list)]
mod defs {
    use agentbus_api::AgentBus;
    use agentbus_api::BusId;
    use agentbus_api::PollRequest;
    use agentbus_api::environment::Clock;
    use agentbus_api::environment::Environment;
    use agentbus_api::payload;
    use agentbus_core::decider::Decider;
    use agentbus_core::vote_trackers::create_vote_tracker_for_decider_policy;
    use rand::RngExt as _;

    use crate::common::helpers::append_decider_policy;
    use crate::common::helpers::append_string_intention;
    use crate::common::helpers::read_linearizable_snapshot;
    use crate::fixtures::AgentBusTestFixture;
    use crate::fixtures::ConformanceFixture;
    use crate::fixtures::simtest::InMemoryWriteOnceAgentBusFixture;
    use crate::simulator::Simulator;

    #[scenario(sim_only)]
    pub async fn run_test_with_decider_run_loop<
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    >(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let environment = fixture.get_env();
        let decider_impl = fixture.create_impl();
        let impl_instance = fixture.create_impl();
        let agent_bus_id = format!("bus-{}", environment.with_rng(|rng| rng.random::<u64>()));
        let poll_interval = std::time::Duration::from_millis(10);

        let start_time = environment.with_clock(|clock| clock.monotonic_time());

        let sleep1 = environment.sleep(poll_interval);
        let sleep2 = environment.sleep(poll_interval * 2);

        append_decider_policy(
            &impl_instance,
            agent_bus_id.clone(),
            agent_bus_proto_rust::agent_bus::DeciderPolicy::OnByDefault as i32,
        )
        .await;

        let mut decider = Decider::new(
            decider_impl,
            agent_bus_id.clone(),
            0,
            create_vote_tracker_for_decider_policy,
        );

        decider.poll_and_decide(None).await.expect("poll 1");

        sleep1.await;

        decider.poll_and_decide(None).await.expect("poll 2");

        sleep2.await;

        append_string_intention(&impl_instance, agent_bus_id.clone(), "test".to_string()).await;

        let count = decider.poll_and_decide(None).await.expect("poll 3");
        assert_eq!(count, 1, "Should process 1 intention");

        let (entries, tail) =
            read_linearizable_snapshot(&impl_instance, agent_bus_id, 0, None, &*environment, None)
                .await;
        assert_eq!(tail, 3, "should have 1 policy + 1 intention + 1 commit");
        assert!(
        entries
            .iter()
            .any(|e| matches!(&e.payload, Some(payload) if matches!(&payload.payload, Some(agent_bus_proto_rust::agent_bus::payload::Payload::Commit(_)))))
    );

        let end_time = environment.with_clock(|clock| clock.monotonic_time());
        let elapsed = end_time - start_time;
        let expected = poll_interval * 2;
        assert!(
            elapsed >= expected,
            "Clock should advance by at least 2 poll intervals (expected: {:?}, got: {:?})",
            expected,
            elapsed
        );
        Ok(())
    }

    #[scenario_for(agentbus_tests::fixtures::simtest::InMemoryWriteOnceAgentBusFixture<2>, suffix = write_once_in_memory, sim_only)]
    pub async fn recently_used_positions_survive_eviction_across_clones(
        fixture: &InMemoryWriteOnceAgentBusFixture<2>,
    ) -> anyhow::Result<()> {
        let metrics = fixture.metrics();
        let bus = fixture.create_impl();
        let clone = bus.clone();
        assert_eq!(append_decider_policy(&bus, "a".to_owned(), 0).await, 0);
        assert_eq!(append_decider_policy(&bus, "b".to_owned(), 0).await, 0);
        assert_eq!(append_decider_policy(&clone, "a".to_owned(), 1).await, 1);
        assert_eq!(append_decider_policy(&bus, "c".to_owned(), 0).await, 0);
        assert_eq!(append_decider_policy(&clone, "a".to_owned(), 2).await, 2);
        assert_eq!(
            metrics.counter("tail.write_once_space.num_calls"),
            0,
            "cache hits must refresh recency and preserve the shared append hint"
        );

        assert_eq!(append_decider_policy(&clone, "b".to_owned(), 1).await, 1);
        assert_eq!(
            metrics.counter("tail.write_once_space.num_calls"),
            1,
            "the least recently used bus must lose its cached position"
        );
        assert_eq!(append_decider_policy(&bus, "d".to_owned(), 0).await, 0);
        assert_eq!(append_decider_policy(&clone, "e".to_owned(), 0).await, 0);
        let other = fixture.create_impl();
        assert_eq!(append_decider_policy(&other, "b".to_owned(), 2).await, 2);
        assert_eq!(
            metrics.counter("tail.write_once_space.num_calls"),
            2,
            "a separate writer must discover its own append position"
        );
        assert_eq!(append_decider_policy(&clone, "b".to_owned(), 3).await, 3);
        assert_eq!(
            metrics.counter("tail.write_once_space.num_calls"),
            3,
            "an evicted position must be rediscovered from storage"
        );
        let response = bus
            .poll(PollRequest {
                bus_id: Some(BusId {
                    agent_bus_id: "b".to_owned(),
                }),
                max_entries: 4,
                ..Default::default()
            })
            .await
            .expect("poll should recover all written entries");
        assert!(response.complete);
        let entries: Vec<_> = response
            .entries
            .into_iter()
            .map(|entry| {
                (
                    entry
                        .header
                        .expect("entry should have a header")
                        .log_position,
                    entry.payload.expect("entry should have a payload").payload,
                )
            })
            .collect();
        assert_eq!(
            entries,
            vec![
                (0, Some(payload::Payload::DeciderPolicy(0))),
                (1, Some(payload::Payload::DeciderPolicy(1))),
                (2, Some(payload::Payload::DeciderPolicy(2))),
                (3, Some(payload::Payload::DeciderPolicy(3))),
            ],
            "eviction must preserve existing entries and append ordering"
        );
        Ok(())
    }
}
pub use defs::*;
