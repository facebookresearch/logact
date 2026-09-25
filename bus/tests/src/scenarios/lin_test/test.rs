/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_lin_test_list)]
mod defs {
    //! Multi-worker counter linearizability test

    use std::cell::Cell;
    use std::rc::Rc;

    use agent_bus_proto_rust::agent_bus::AppendRequest;
    use agent_bus_proto_rust::agent_bus::BusId;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agentbus_api::AgentBus;
    use agentbus_api::environment::Environment;
    use agentbus_core::decider::Decider;
    use agentbus_core::vote_trackers::create_vote_tracker_for_decider_policy;
    use anyhow::Result;
    use rand::Rng;
    use rand::RngExt as _;

    use super::super::counter_impl::AgentBusCounter;
    use super::super::counter_impl::SequentialCounter;
    use super::super::counter_trait::Counter;
    use super::super::counter_worker::CounterWorker;
    use super::super::linearizability_tracker::ExecutedCommand;
    use super::super::linearizability_tracker::LinearizabilityTracker;
    use super::super::random_voter::RandomVoter;
    use super::super::tracking_counter::TrackingCounter;
    use crate::fixtures::AgentBusTestFixture;
    use crate::fixtures::ConformanceFixture;
    use crate::simulator::Simulator;
    use crate::simulator::SimulatorBarrier;

    #[derive(Clone, Copy)]
    pub enum Op {
        Increment,
        Decrement,
        Read,
    }

    #[derive(Clone)]
    pub struct WorkerWorkload {
        pub operations: Vec<Op>,
        pub max_poll_entries: i32,
    }

    #[derive(Clone)]
    pub struct Workload {
        pub agent_bus_id: String,
        pub workers: Vec<WorkerWorkload>,
    }

    impl Workload {
        pub fn generate(
            rng: &mut impl Rng,
            num_workers: usize,
            num_ops: usize,
            max_poll_entries: Option<i32>,
        ) -> Self {
            let agent_bus_id = format!("bus-{}", rng.random::<u64>());
            let workers = (0..num_workers)
                .map(|_| {
                    let operations = (0..num_ops)
                        .map(|_| match rng.random_range(0..3) {
                            0 => Op::Increment,
                            1 => Op::Decrement,
                            _ => Op::Read,
                        })
                        .collect();
                    let actual_max = max_poll_entries.unwrap_or_else(|| rng.random_range(1..=64));
                    WorkerWorkload {
                        operations,
                        max_poll_entries: actual_max,
                    }
                })
                .collect();
            Self {
                agent_bus_id,
                workers,
            }
        }
    }

    /// Test with skip_commits mode - no decider/voter needed
    pub async fn run_lin_test_multi_worker_counter_skip_commits<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        run_multi_worker_counter_test(fixture, true, None).await
    }

    /// Pinned to the deliberately-buggy fixture (`ChainedAgentBusBuggyPollFixture`,
    /// whose poll skips commits): asserts the linearizability checker *catches* the
    /// violation. This inverts the usual `Ok` contract, so it can't fan over the
    /// real fixtures — hence `#[scenario_for]` — and the fixed seed reproduces a
    /// schedule that reliably surfaces the bug.
    #[scenario_for(
        agentbus_tests::fixtures::simtest::ChainedAgentBusBuggyPollFixture,
        suffix = chained_agentbus_buggy_poll,
        sim_only,
        seed = 11
    )]
    pub async fn run_buggy_poll_is_caught<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        let result = run_multi_worker_counter_test(fixture, true, None).await;
        anyhow::ensure!(
            result.is_err(),
            "expected the linearizability checker to catch the buggy poll, but the scenario passed"
        );
        Ok(())
    }

    /// Default test uses random poll size (1-64)
    #[scenario(sim_only)]
    pub async fn run_lin_test_multi_worker_counter<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        run_multi_worker_counter_test(fixture, false, None).await
    }

    /// Test with large poll batch size (1000)
    #[scenario(sim_only)]
    pub async fn run_lin_test_multi_worker_counter_large_poll<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        run_multi_worker_counter_test(fixture, false, Some(1000)).await
    }

    /// Test with small poll batch size (1) - catches noop position bugs
    #[scenario(sim_only)]
    pub async fn run_lin_test_multi_worker_counter_small_poll<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        run_multi_worker_counter_test(fixture, false, Some(1)).await
    }

    async fn run_multi_worker_counter_test<F>(
        fixture: &F,
        skip_commits: bool,
        max_poll_entries: Option<i32>,
    ) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        let env_rc = fixture.get_env();
        let num_workers: usize = env_rc.with_rng(|rng| rng.random_range(2..5));
        let num_ops_per_worker: usize = env_rc.with_rng(|rng| rng.random_range(2..6));
        let workload = env_rc.with_rng(|rng| {
            Workload::generate(rng, num_workers, num_ops_per_worker, max_poll_entries)
        });
        run_multi_worker_counter_test_with_workload(fixture, skip_commits, &workload).await
    }

    pub async fn run_multi_worker_counter_test_with_workload<F>(
        fixture: &F,
        skip_commits: bool,
        workload: &Workload,
    ) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        let env_rc = fixture.get_env();
        let num_workers = workload.workers.len();

        let barrier = SimulatorBarrier::new(num_workers);

        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();

        let workers: Vec<_> = (0..num_workers)
            .map(|idx| {
                let impl_instance = fixture.create_impl();
                let mut counter = AgentBusCounter::new(
                    impl_instance,
                    workload.agent_bus_id.clone(),
                    env_rc.clone(),
                    idx,
                );
                if skip_commits {
                    counter = counter.with_skip_commits();
                }
                counter = counter.with_max_poll_entries(workload.workers[idx].max_poll_entries);
                let client_id = format!("w{}", idx);
                let tracking_counter =
                    TrackingCounter::new(counter, env_rc.clone(), tracker.clone(), client_id);
                CounterWorker::new(tracking_counter, workload.workers[idx].operations.clone())
            })
            .collect();

        let stop_components = Rc::new(Cell::new(false));

        if !skip_commits {
            spawn_decider_and_voters(
                fixture,
                &env_rc,
                &workload.agent_bus_id,
                stop_components.clone(),
            );
        }

        let handles = spawn_workers(
            workers,
            &env_rc,
            barrier,
            num_workers,
            if skip_commits {
                None
            } else {
                Some(stop_components)
            },
        );

        env_rc.run();

        let mut worker_histories: Vec<Vec<ExecutedCommand>> = Vec::new();
        for handle in handles {
            let command_history: Vec<ExecutedCommand> =
                handle.await.expect("Worker should complete");
            worker_histories.push(command_history);
        }

        let sequential_counter = SequentialCounter::new();
        tracker.verify(&worker_histories, |operation| {
            sequential_counter.apply_operation(0, operation)
        })
    }

    fn spawn_decider_and_voters<F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>>(
        fixture: &F,
        env_rc: &Rc<Simulator>,
        agent_bus_id: &str,
        stop_components: Rc<Cell<bool>>,
    ) {
        let policy = DeciderPolicy::FirstBooleanWins as i32;

        let policy_impl = fixture.create_impl();
        let agent_bus_id_for_policy = agent_bus_id.to_string();
        env_rc.spawn_named(
            async move {
                let policy_payload = agent_bus_proto_rust::agent_bus::Payload {
                    payload: Some(
                        agent_bus_proto_rust::agent_bus::payload::Payload::DeciderPolicy(policy),
                    ),
                };
                policy_impl
                    .append(AppendRequest {
                        agent_bus_id: agent_bus_id_for_policy.clone(),
                        bus_id: Some(BusId {
                            agent_bus_id: agent_bus_id_for_policy,
                        }),
                        payload: Some(policy_payload),
                    })
                    .await
                    .expect("Policy change should succeed");
            },
            Some("policy_setter".to_string()),
        );

        let decider_impl = fixture.create_impl();
        let decider_env = env_rc.clone();
        let agent_bus_id_for_decider = agent_bus_id.to_string();
        let stop_for_decider = stop_components.clone();
        env_rc.spawn_named(
            async move {
                let mut decider = Decider::new(
                    decider_impl,
                    agent_bus_id_for_decider,
                    0,
                    create_vote_tracker_for_decider_policy,
                );
                loop {
                    if stop_for_decider.get() {
                        break;
                    }
                    decider
                        .poll_and_decide(None)
                        .await
                        .expect("Decider should succeed");
                    let sleep_millis = decider_env.with_rng(|rng| rng.random_range(1..5));
                    decider_env
                        .sleep(std::time::Duration::from_millis(sleep_millis))
                        .await;
                }
            },
            Some("decider".to_string()),
        );

        let voter_impl = fixture.create_impl();
        let voter = RandomVoter::new(voter_impl, agent_bus_id.to_string(), env_rc.clone());
        env_rc.spawn_named(
            async move {
                loop {
                    if stop_components.get() {
                        break;
                    }
                    let timeout_ms = voter.env.with_rng(|rng| rng.random_range(0..5));
                    voter.poll_and_vote_once(timeout_ms).await;
                }
            },
            Some("voter_0".to_string()),
        );
    }

    fn spawn_workers<C: Counter + 'static>(
        workers: Vec<CounterWorker<C>>,
        env_rc: &Rc<Simulator>,
        barrier: Rc<SimulatorBarrier>,
        num_workers: usize,
        stop_components: Option<Rc<Cell<bool>>>,
    ) -> Vec<crate::simulator::SimulatorHandle<Vec<ExecutedCommand>>> {
        let mut handles = Vec::new();
        for (idx, worker) in workers.into_iter().enumerate() {
            let barrier_clone = barrier.clone();
            let stop_clone = stop_components.clone();
            let is_last_worker = idx == num_workers - 1;
            let handle = env_rc.spawn_named(
                async move {
                    worker.run_workload().await;
                    barrier_clone.wait().await;
                    let command_history = worker.get_command_history().await;
                    if is_last_worker {
                        if let Some(stop) = stop_clone {
                            stop.set(true);
                        }
                    }
                    command_history
                },
                Some(format!("worker_{}", idx)),
            );
            handles.push(handle);
        }
        handles
    }
}
pub use defs::*;
