/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Failure-injection conformance scenarios (sim-only).
//!
//! Faults are baked into the fixture (`FaultFixture<Base, Mix>`, see
//! `fixtures::simtest`) rather than the scenario body, because space-level faults
//! are injected below the bus where a scenario can't reach. Each scenario is
//! therefore pinned via `#[scenario_for]` to the fault fixtures it needs, selecting
//! both the injection layer (`Base`) and the fault mix (`Mix`):
//!
//! - single-fate tests force one fate (lost / commit-then-error / error-then-commit)
//!   and assert the resulting append/poll outcome;
//! - one-shot ambiguous-write tests cover reporting an unknown append outcome
//!   whether the write applied or not;
//! - permutation tests run the mixed-fault workload and assert the exact set of
//!   legal commit orderings, which differs by injection layer (16 at the bus layer,
//!   12 at the space layer, where per-client FIFO is preserved).

#[conformance_macros::scenarios(agentbus_fault_injection_list)]
mod defs {
    use anyhow::Result;

    use crate::fault_injection_helpers::BUS_LEVEL_OUTCOMES;
    use crate::fault_injection_helpers::SPACE_LEVEL_OUTCOMES;
    use crate::fault_injection_helpers::run_fault_test;
    use crate::fault_injection_helpers::run_permutations_with_faults_test;
    use crate::fixtures::AgentBusTestFixture;
    use crate::fixtures::ConformanceFixture;
    use crate::simulator::Simulator;

    // No faults: append succeeds and commits. Pinned to the bare base fault
    // fixtures (whose `SimulatorFixture::new` is zero-fault).
    #[scenario_for(agentbus_tests::BusSimpleMemory, suffix = bus_simple_memory, sim_only)]
    #[scenario_for(agentbus_tests::BusChanneled, suffix = bus_channeled, sim_only)]
    #[scenario_for(agentbus_tests::BusChained, suffix = bus_chained, sim_only)]
    #[scenario_for(agentbus_tests::SpaceInMemory, suffix = space_in_memory, sim_only)]
    #[scenario_for(agentbus_tests::SpaceChanneled, suffix = space_channeled, sim_only)]
    #[scenario_for(agentbus_tests::SpaceCaching, suffix = space_caching, sim_only)]
    pub async fn run_fi_success<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_fault_test(fixture.create_impl(), fixture.get_env(), true, true, None);
        Ok(())
    }

    #[scenario_for(agentbus_tests::SpaceAmbiguousWriteEmpty, suffix = empty_slot, sim_only)]
    pub async fn run_ambiguous_write_without_commit<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_fault_test(
            fixture.create_impl(),
            fixture.get_env(),
            false,
            false,
            Some("write outcome unknown"),
        );
        Ok(())
    }

    #[scenario_for(agentbus_tests::SpaceAmbiguousWriteOccupied, suffix = occupied_slot, sim_only)]
    pub async fn run_ambiguous_write_with_commit<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_fault_test(
            fixture.create_impl(),
            fixture.get_env(),
            false,
            true,
            Some("write outcome unknown"),
        );
        Ok(())
    }

    // Every append is lost: no success, no commit.
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusSimpleMemory, agentbus_tests::Lost>, suffix = bus_simple_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChanneled, agentbus_tests::Lost>, suffix = bus_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChained, agentbus_tests::Lost>, suffix = bus_chained, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceInMemory, agentbus_tests::Lost>, suffix = space_in_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceChanneled, agentbus_tests::Lost>, suffix = space_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceCaching, agentbus_tests::Lost>, suffix = space_caching, sim_only)]
    pub async fn run_fi_lost<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_fault_test(
            fixture.create_impl(),
            fixture.get_env(),
            false,
            false,
            Some("Lost"),
        );
        Ok(())
    }

    // Every append commits but returns an error.
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusSimpleMemory, agentbus_tests::CommitThenError>, suffix = bus_simple_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChanneled, agentbus_tests::CommitThenError>, suffix = bus_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChained, agentbus_tests::CommitThenError>, suffix = bus_chained, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceInMemory, agentbus_tests::CommitThenError>, suffix = space_in_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceChanneled, agentbus_tests::CommitThenError>, suffix = space_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceCaching, agentbus_tests::CommitThenError>, suffix = space_caching, sim_only)]
    pub async fn run_fi_commit_then_error<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_fault_test(
            fixture.create_impl(),
            fixture.get_env(),
            false,
            true,
            Some("CommitThenError"),
        );
        Ok(())
    }

    // Every append returns an error but later commits.
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusSimpleMemory, agentbus_tests::ErrorThenCommit>, suffix = bus_simple_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChanneled, agentbus_tests::ErrorThenCommit>, suffix = bus_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChained, agentbus_tests::ErrorThenCommit>, suffix = bus_chained, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceInMemory, agentbus_tests::ErrorThenCommit>, suffix = space_in_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceChanneled, agentbus_tests::ErrorThenCommit>, suffix = space_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceCaching, agentbus_tests::ErrorThenCommit>, suffix = space_caching, sim_only)]
    pub async fn run_fi_error_then_commit<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_fault_test(
            fixture.create_impl(),
            fixture.get_env(),
            false,
            true,
            Some("ErrorThenCommit"),
        );
        Ok(())
    }

    // Mixed faults at the bus layer: all 16 orderings are legal.
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusSimpleMemory, agentbus_tests::Mixed>, suffix = bus_simple_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChanneled, agentbus_tests::Mixed>, suffix = bus_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::BusChained, agentbus_tests::Mixed>, suffix = bus_chained, sim_only)]
    pub async fn run_fi_permutations_bus<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_permutations_with_faults_test(
            fixture.create_impl(),
            fixture.get_env(),
            &BUS_LEVEL_OUTCOMES,
        );
        Ok(())
    }

    // Mixed faults at the space layer: per-client FIFO preserved, so 12 orderings.
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceInMemory, agentbus_tests::Mixed>, suffix = space_in_memory, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceChanneled, agentbus_tests::Mixed>, suffix = space_channeled, sim_only)]
    #[scenario_for(agentbus_tests::FaultFixture<agentbus_tests::SpaceCaching, agentbus_tests::Mixed>, suffix = space_caching, sim_only)]
    pub async fn run_fi_permutations_space<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator, Impl: Clone>,
    {
        run_permutations_with_faults_test(
            fixture.create_impl(),
            fixture.get_env(),
            &SPACE_LEVEL_OUTCOMES,
        );
        Ok(())
    }
}

pub use defs::*;
