/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_policy_register_list)]
mod defs {
    //! Basic checks that the commit decision follows the `SynchronousRegisterProvider`:
    //! a policy installed via `set_decider_policy` is reflected by the next
    //! `commit_intention`, because the engine reads the register on the critical path
    //! of every propose.
    //!
    //! Pinned with `#[scenario_for]` to the register-backed fixtures (a single V1
    //! engine and a router), since only those implement `DeciderPolicyFixture`.

    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::intention as proto_intention;
    use agentbus_api::Environment;
    use anyhow::Result;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::DeciderPolicyFixture;
    use crate::simulator::Simulator;

    fn intention(agent_id: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: agent_id.to_string(),
            },
            intention: proto_intention::Intention::StringIntention("probe".to_string()),
        }
    }

    /// Install `OFF_BY_DEFAULT` and confirm the next intention is denied, then
    /// `ON_BY_DEFAULT` and confirm it is approved — i.e. the engine decides each
    /// intention under whatever the register holds at propose time.
    #[scenario_for(CommitServiceV1Fixture<SimpleMemoryFixture>, suffix = v1_simple_memory, sim_only)]
    #[scenario_for(RandomRoutingCommitServiceFixture<SimpleMemoryFixture>, suffix = random_routing, sim_only)]
    pub async fn run_policy_register_gates_commit<F>(fixture: &F) -> Result<()>
    where
        F: DeciderPolicyFixture<Env = Simulator>,
    {
        let env = fixture.get_env();
        let svc = fixture.create_impl();
        let agent_id = format!("gate-{}", env.with_rng(|rng| rng.random::<u64>()));

        // Closed: OFF_BY_DEFAULT with no voter denies.
        fixture
            .set_decider_policy(DeciderPolicy::OffByDefault)
            .await?;
        let denied = svc.commit_intention(intention(&agent_id)).await?;
        anyhow::ensure!(
            !denied.approved,
            "OFF_BY_DEFAULT (no voter) must deny: {}",
            denied.reason
        );

        // Open: ON_BY_DEFAULT approves.
        fixture
            .set_decider_policy(DeciderPolicy::OnByDefault)
            .await?;
        let approved = svc.commit_intention(intention(&agent_id)).await?;
        anyhow::ensure!(
            approved.approved,
            "ON_BY_DEFAULT must approve: {}",
            approved.reason
        );

        Ok(())
    }
}
pub use defs::*;
