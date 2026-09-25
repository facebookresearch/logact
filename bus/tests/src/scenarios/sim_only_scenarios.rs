/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_sim_only_scenarios_list)]
mod defs {
    use agentbus_api::environment::Clock;
    use agentbus_api::environment::Environment;
    use agentbus_core::decider::Decider;
    use agentbus_core::vote_trackers::create_vote_tracker_for_decider_policy;
    use rand::RngExt as _;

    use crate::common::helpers::append_decider_policy;
    use crate::common::helpers::append_string_intention;
    use crate::common::helpers::read_linearizable_snapshot;
    use crate::fixtures::AgentBusTestFixture;
    use crate::fixtures::ConformanceFixture;
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
}
pub use defs::*;
