/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(counter_common_list)]
mod defs {
    use agentbus_api::Environment;
    use agentbus_simulator::Simulator;
    use anyhow::Result;
    use conformance::ConformanceFixture;
    use linearizability::object::LinearizabilityImplementation;
    use linearizability::object::LinearizableObject;
    use linearizability::object::run_linearizability_test;

    use crate::commit_service::run_counter_with_denials as execute_counter_with_denials;
    use crate::counter::CounterSpec;
    use crate::counter::CounterWorkload;
    use crate::fixtures::CommitServiceCounterTestFixture;

    async fn check_counter_linearizability<F>(
        fixture: &F,
        clients: usize,
        operations: usize,
    ) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object: LinearizableObject<Spec = CounterSpec>,
    {
        let workload = fixture
            .get_env()
            .with_rng(|rng| CounterWorkload::random(rng, clients, operations));
        run_linearizability_test(workload, fixture.create_impl()).await?;
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_counter_linearizability_3x8<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object: LinearizableObject<Spec = CounterSpec>,
    {
        check_counter_linearizability(fixture, 3, 8).await
    }

    #[scenario(sim_only)]
    pub async fn run_counter_linearizability_2x2<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object: LinearizableObject<Spec = CounterSpec>,
    {
        check_counter_linearizability(fixture, 2, 2).await
    }

    #[scenario(sim_only)]
    pub async fn run_counter_linearizability_4x5<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object: LinearizableObject<Spec = CounterSpec>,
    {
        check_counter_linearizability(fixture, 4, 5).await
    }

    #[scenario(sim_only)]
    pub async fn run_counter_linearizability_3x100<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object: LinearizableObject<Spec = CounterSpec>,
    {
        check_counter_linearizability(fixture, 3, 100).await
    }

    #[scenario(sim_only)]
    pub async fn run_counter_linearizability_10x3<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object: LinearizableObject<Spec = CounterSpec>,
    {
        check_counter_linearizability(fixture, 10, 3).await
    }

    #[scenario_for(
        counter_suite::fixtures::BuggyPollCounterFixture,
        suffix = chained_agentbus_buggy_poll,
        sim_only,
        seed = 14165384662629589995
    )]
    pub async fn run_buggy_poll_is_caught<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object: LinearizableObject<Spec = CounterSpec>,
    {
        let result = check_counter_linearizability(fixture, 3, 8).await;
        anyhow::ensure!(
            result.is_err(),
            "expected the linearizability checker to catch the buggy poll, but the scenario passed"
        );
        Ok(())
    }

    #[scenario_for(
        counter_suite::fixtures::CountingVoterCounterFixture,
        suffix = v1_counting_voter_simple_memory,
        sim_only
    )]
    #[scenario_for(
        counter_suite::fixtures::FaultyCountingVoterCounterFixture,
        suffix = v1_counting_voter_fault,
        sim_only
    )]
    pub async fn run_counter_with_denials<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceCounterTestFixture,
        <F::Inner as ConformanceFixture>::Impl: 'static,
    {
        execute_counter_with_denials(fixture.inner()).await
    }
}
pub use defs::*;
