/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(gated_counter_gated_counter_list)]
mod defs {
    use agentbus_simulator::Simulator;
    use anyhow::Result;
    use conformance::ConformanceFixture;
    use counter_suite::counter::CounterOperation;
    use linearizability::object::Client;
    use linearizability::object::LinearizabilityImplementation;
    use linearizability::object::LinearizableObject;
    use linearizability::object::run_linearizability_test;

    use crate::model::GatedCounterOperation;
    use crate::model::GatedCounterSpec;
    use crate::model::GatedCounterWorkload;

    #[scenario(sim_only)]
    pub async fn run_gated_counter_linearizability<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object:
            LinearizableObject<Spec = GatedCounterSpec>,
    {
        run_linearizability_test(GatedCounterWorkload::new(3, 8, 8), fixture.create_impl()).await?;
        Ok(())
    }

    async fn check_gate_race<F>(fixture: &F) -> Result<usize>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object:
            LinearizableObject<Spec = GatedCounterSpec>,
    {
        run_linearizability_test(
            vec![
                Client::new(
                    "gate",
                    vec![
                        GatedCounterOperation::SetGate(false),
                        GatedCounterOperation::ReadGate,
                    ],
                ),
                Client::new(
                    "counter",
                    vec![
                        GatedCounterOperation::Counter(CounterOperation::Increment),
                        GatedCounterOperation::Counter(CounterOperation::Read),
                    ],
                ),
            ],
            fixture.create_impl(),
        )
        .await
    }

    #[scenario_for(
        gated_counter_suite::gate_race::CorrectGateRaceFixture,
        suffix = atomic_gate,
        sim_only,
        seed = 0
    )]
    pub async fn run_gate_race_control<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object:
            LinearizableObject<Spec = GatedCounterSpec>,
    {
        anyhow::ensure!(check_gate_race(fixture).await? == 4);
        Ok(())
    }

    #[scenario_for(
        gated_counter_suite::gate_race::BuggyGateRaceFixture,
        suffix = buggy_gate,
        sim_only,
        seed = 0
    )]
    pub async fn run_buggy_gate_is_caught<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object:
            LinearizableObject<Spec = GatedCounterSpec>,
    {
        let result = check_gate_race(fixture).await;
        let error = result.expect_err(
            "checker should reject a lost counter update during a concurrent gate write",
        );
        anyhow::ensure!(
            error
                .to_string()
                .contains("observed Counter(Ok(0)), but sequential replay produced Counter(Ok(1))"),
            "expected a gated-counter verification failure, got {error:#}"
        );
        Ok(())
    }
}
pub use defs::*;
