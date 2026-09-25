/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_multi_node_list)]
mod defs {
    //! Generic test scenarios that can run against any AgentBus implementation

    use std::rc::Rc;

    use agent_bus_proto_rust::agent_bus::AppendRequest;
    use agent_bus_proto_rust::agent_bus::BusId;
    use agent_bus_proto_rust::agent_bus::PollRequest;
    use agentbus_api::AgentBus;
    use agentbus_api::environment::Environment;
    use anyhow::Result;
    use rand::RngExt as _;

    use crate::fixtures::AgentBusTestFixture;
    use crate::fixtures::ConformanceFixture;
    use crate::simulator::Simulator;
    use crate::simulator::SimulatorBarrier;

    // Test an AgentBus service from multiple nodes (/clients)
    // in a deterministic simulation test.
    // The test is generic and can be run against any AgentBus implementation.

    struct WorkloadClient<T: AgentBus> {
        agent_bus_impl: T,
        agent_bus_id: String,
    }

    impl<T: AgentBus> WorkloadClient<T> {
        fn new(agent_bus_impl: T, agent_bus_id: String) -> Self {
            Self {
                agent_bus_impl,
                agent_bus_id,
            }
        }

        async fn append(&self, payload_str: String) {
            let payload = agent_bus_proto_rust::agent_bus::Payload {
            payload: Some(
                agent_bus_proto_rust::agent_bus::payload::Payload::Intention(
                    agent_bus_proto_rust::agent_bus::Intention {
                        intention: Some(
                            agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(
                                payload_str,
                            ),
                        ),
                        ..Default::default()
                    },
                ),
            ),
        };
            let request = AppendRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                payload: Some(payload),
                ..Default::default()
            };
            self.agent_bus_impl
                .append(request)
                .await
                .expect("Append should succeed");
        }

        async fn run_workload(&self, prefix: &str, count: usize) {
            for i in 1..=count {
                self.append(format!("{}{}", prefix, i)).await;
            }
        }

        async fn get_concatenated_commands(&self) -> String {
            let poll_request = PollRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                start_log_position: 0,
                max_entries: 1000, // Request up to 1000 entries
                ..Default::default()
            };

            let poll_result = self
                .agent_bus_impl
                .poll(poll_request)
                .await
                .expect("Poll should succeed");

            poll_result
                .entries
                .iter()
                .filter_map(|entry| {
                    // Extract the stringIntention from Intention as the command string
                    // Skip commit/abort entries
                    if let Some(ref payload) = entry.payload {
                        if let Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(
                            ref intention,
                        )) = payload.payload
                        {
                            if let Some(
                            agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(
                                ref string_data,
                            ),
                        ) = intention.intention
                        {
                            return Some(string_data.clone());
                        }
                        }
                    }
                    None
                })
                .collect::<Vec<_>>()
                .join("")
        }
    }

    /// Runs the multi-node workload against `fixture` and returns the concatenated
    /// command log every client converges on (after asserting they agree).
    ///
    /// Spawns one client per simulated node via the deterministic `Simulator::spawn`
    /// and awaits them, so it runs as an ordinary async scenario under
    /// `conformance::sim_test!` (which drives `env.run()`) rather than a self-driven
    /// loop.
    async fn run_multi_node_workload<F>(fixture: &F) -> String
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        let env_rc = fixture.get_env();

        let agent_bus_id = format!("bus-{}", env_rc.with_rng(|rng| rng.random::<u64>()));
        let num_threads: usize = env_rc.with_rng(|rng| rng.random_range(1..5));
        let num_appends_per_thread: usize = env_rc.with_rng(|rng| rng.random_range(1..5));

        // Create barrier for synchronization between threads
        let barrier = SimulatorBarrier::new(num_threads);

        // Create multiple clients upfront - each thread gets its own client and implementation
        let clients: Vec<_> = (0..num_threads)
            .map(|_| {
                let impl_instance = fixture.create_impl();
                WorkloadClient::new(impl_instance, agent_bus_id.clone())
            })
            .collect();

        let prefixes = ["A", "B", "C", "D", "E", "F"];

        // Spawn K threads; each one has its own client and runs workload
        let mut handles = Vec::new();
        for (i, (client, prefix)) in clients.into_iter().zip(prefixes.iter()).enumerate() {
            let barrier_clone: Rc<SimulatorBarrier> = barrier.clone();
            let prefix = *prefix;
            let is_first = i == 0;
            let agent_bus_id_clone = agent_bus_id.clone();
            let handle = env_rc.spawn(async move {
                // First task sets the policy
                if is_first {
                    let policy_payload = agent_bus_proto_rust::agent_bus::Payload {
                        payload: Some(
                            agent_bus_proto_rust::agent_bus::payload::Payload::DeciderPolicy(
                                agent_bus_proto_rust::agent_bus::DeciderPolicy::FirstBooleanWins
                                    as i32,
                            ),
                        ),
                    };
                    let policy_request = AppendRequest {
                        agent_bus_id: agent_bus_id_clone.clone(),
                        bus_id: Some(BusId {
                            agent_bus_id: agent_bus_id_clone.clone(),
                        }),
                        payload: Some(policy_payload),
                    };
                    client
                        .agent_bus_impl
                        .append(policy_request)
                        .await
                        .expect("Policy change should succeed");
                }

                // Run workload: issue N appends in a loop
                client.run_workload(prefix, num_appends_per_thread).await;

                // Wait on barrier until all threads reach this point
                barrier_clone.wait().await;

                // Verify results independently
                let result = client.get_concatenated_commands().await;
                assert_eq!(result.len(), 2 * num_appends_per_thread * num_threads);

                drop(client);

                result
            });
            handles.push(handle);
        }

        // Await all spawned clients; `conformance::sim_test!` drives `env.run()`.
        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await.expect("Task should complete successfully");
            results.push(result);
        }

        // All results should be identical (shared state)
        let first_result = &results[0];
        for result in &results[1..] {
            assert_eq!(
                result, first_result,
                "All clients should see the same concatenated commands"
            );
        }

        first_result.clone()
    }

    /// Multi-node consistency: every client converges on the same committed log.
    /// Sim-only — spawns one client per node via the simulator's deterministic
    /// scheduler (`Simulator::spawn`).
    #[scenario(sim_only)]
    pub async fn run_test_multiple_nodes<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        run_multi_node_workload(fixture).await;
        Ok(())
    }

    /// Determinism guard: the multi-node workload replayed under the same seed must
    /// produce byte-identical output — catching an implementation that reaches
    /// outside the `Environment` (real clock/RNG/threads). `cardinality = 2` makes
    /// `conformance::sim_determinism_test!` run this twice on fresh fixtures built
    /// from one held-fixed seed and compare the results — so, unlike a plain
    /// `#[scenario]`, it returns the committed-log string rather than `Result<()>`.
    #[scenario(sim_only, cardinality = 2)]
    pub async fn run_multi_node_determinism<F>(fixture: &F) -> String
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        run_multi_node_workload(fixture).await
    }
}
pub use defs::*;
