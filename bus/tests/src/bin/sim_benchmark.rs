/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Benchmark binary that runs the same workload against each AgentBus fixture
//! and compares simulation times.

use std::rc::Rc;

use agentbus_api::Clock;
use agentbus_api::environment::Environment;
use agentbus_tests::agentbus_fixtures;
use agentbus_tests::fixtures::AgentBusTestFixture;
use agentbus_tests::fixtures::ConformanceFixture;
use agentbus_tests::fixtures::SimulatorFixture;
use agentbus_tests::simulator::Simulator;
use counter_suite::agentbus::AgentBusCounterImplementation;
use counter_suite::agentbus::AgentBusCounterOptions;
use counter_suite::agentbus::PollEntries;
use counter_suite::counter::CounterWorkload;
use linearizability::object::run_linearizability_test;
use rand::RngExt as _;
use rand::SeedableRng as _;
use rand::rngs::StdRng;

struct BenchResult {
    name: &'static str,
    sim_time_us: u128,
    total_events: usize,
}

#[derive(Clone)]
struct Workload {
    counter: CounterWorkload,
    agent_bus_id: String,
    max_poll_entries: Vec<i32>,
}

fn run_one<F>(name: &'static str, sim_seed: u64, workload: &Workload) -> BenchResult
where
    F: SimulatorFixture + AgentBusTestFixture + ConformanceFixture<Env = Simulator> + 'static,
    F::Impl: agentbus_api::AgentBus + 'static,
{
    let simulator = Simulator::new(sim_seed);
    let fixture = Rc::new(F::new(simulator));
    let env_rc = fixture.get_env();
    let start_time = env_rc.with_clock(|c| c.monotonic_time());

    let workload_clone = workload.clone();
    let handle = env_rc.spawn(async move {
        run_linearizability_test(
            workload_clone.counter,
            AgentBusCounterImplementation::with_configuration(
                fixture,
                workload_clone.agent_bus_id,
                AgentBusCounterOptions {
                    skip_commits: false,
                    poll_entries: PollEntries::PerClient(workload_clone.max_poll_entries),
                },
            ),
        )
        .await
    });

    let stats = env_rc.run();
    futures::executor::block_on(handle)
        .expect("Task should complete")
        .expect("Linearizability check should pass");

    let sim_time = env_rc.with_clock(|c| c.monotonic_time()) - start_time;
    BenchResult {
        name,
        sim_time_us: sim_time.as_micros(),
        total_events: stats.total_events,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let top_seed: u64 = if args.len() > 1 {
        args[1].parse().expect("First argument must be a u64 seed")
    } else {
        rand::random()
    };
    let filter: Option<&str> = args.get(2).map(|s| s.as_str());
    eprintln!("Top-level seed: {top_seed}");

    let mut workload_rng = StdRng::seed_from_u64(top_seed);
    let num_workers = workload_rng.random_range(2..5);
    let num_ops = workload_rng.random_range(2..6);
    let workload = Workload {
        counter: CounterWorkload::random(&mut workload_rng, num_workers, num_ops),
        agent_bus_id: format!("bus-{}", workload_rng.random::<u64>()),
        max_poll_entries: (0..num_workers)
            .map(|_| workload_rng.random_range(1..=64))
            .collect(),
    };

    eprintln!(
        "Workload: {} workers, {} ops/worker, {} total ops, bus_id={}",
        workload.counter.client_count(),
        workload.counter.operations_per_client(),
        workload.counter.operation_count(),
        workload.agent_bus_id,
    );

    let mut seed_rng = StdRng::seed_from_u64(top_seed.wrapping_add(1));

    let mut results = Vec::new();
    // The integration fixture isn't a `SimulatorFixture`, so it can't be benchmarked;
    // the `integration` arm skips it.
    macro_rules! bench_one {
        ([$fixture:ty, $suffix:ident, sim]) => {{
            let seed: u64 = seed_rng.random();
            if filter.map_or(true, |f| f == stringify!($suffix)) {
                results.push(run_one::<$fixture>(stringify!($suffix), seed, &workload));
            }
        }};
        ([$fixture:ty, $suffix:ident, integration]) => {{}};
    }
    agentbus_fixtures!(bench_one);

    println!();
    println!("{:<25} {:>15} {:>12}", "Fixture", "Sim Time (us)", "Events");
    println!("{:-<25} {:-<15} {:-<12}", "", "", "");
    for r in &results {
        println!(
            "{:<25} {:>15} {:>12}",
            r.name, r.sim_time_us, r.total_events
        );
    }
}
