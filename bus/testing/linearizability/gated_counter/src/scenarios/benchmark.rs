/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Benchmark workloads: longer fault-free runs with injected latency, pinned to
//! the commit service's production shape, which print their timings. Run them,
//! with any configuration and feature flags, with
//! `logact/commit_service/tests:sim_benchmark`.

#[conformance_macros::scenarios(gated_counter_benchmark_list)]
mod defs {
    use std::rc::Rc;
    use std::time::Duration;

    use agentbus_simulator::Simulator;
    use anyhow::Result;
    use conformance::ConformanceFixture;
    use linearizability::object::LinearizabilityImplementation;
    use linearizability::object::LinearizableObject;
    use linearizability::object::run_linearizability_test_timed;

    use crate::model::GatedCounterSpec;
    use crate::model::GatedCounterWorkload;

    /// Per bus.
    const COUNTER_OPERATIONS: usize = 24;
    const BUSES: usize = 3;
    const CLIENTS_PER_BUS: usize = 4;

    /// Four concurrent counter clients on each of three buses, one
    /// linearizability run per bus, each in its own task. Without gate writes,
    /// the buses stay independent even though fixtures share one policy. Prints
    /// the completed operations' mean and p99 latency.
    #[scenario_for(
        gated_counter_suite::fixtures::CommitServiceGatedCounterFixture<
            logact_commit_service_tests::fixtures::CommitServiceProductionShape,
        >,
        suffix = commit_service_production_shape,
        sim_only,
        latency_ms = 50,
        sticky = 0.99
    )]
    pub async fn run_benchmark_independent_buses_concurrent<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation + 'static,
        <F::Impl as LinearizabilityImplementation>::Object:
            LinearizableObject<Spec = GatedCounterSpec>,
    {
        let environment = fixture.get_env();
        let runs: Vec<_> = (0..BUSES)
            .map(|_| {
                environment.spawn(run_linearizability_test_timed(
                    GatedCounterWorkload::new(
                        CLIENTS_PER_BUS,
                        COUNTER_OPERATIONS / CLIENTS_PER_BUS,
                        0,
                    ),
                    Rc::new(fixture.create_impl()),
                ))
            })
            .collect();
        let mut latencies = Vec::new();
        for run in runs {
            latencies.extend(run.await??);
        }
        anyhow::ensure!(!latencies.is_empty(), "no operation completed");
        latencies.sort_unstable();
        let mean = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let p99 = latencies[(latencies.len() * 99).div_ceil(100) - 1];
        eprintln!(
            "{} operations: mean {} ms, p99 {} ms",
            latencies.len(),
            mean.as_millis(),
            p99.as_millis()
        );
        Ok(())
    }
}
pub use defs::*;
