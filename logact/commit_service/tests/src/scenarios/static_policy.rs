/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_static_policy_list)]
mod defs {
    //! Checks that a fixture wired with a `StaticConfigPolicyProvider` decides under
    //! the policy fixed at construction.
    //!
    //! The provider here is configured `OFF_BY_DEFAULT` with no voters (see
    //! `TestPolicyProvider::for_test`), so every `commit_intention` is denied. That
    //! verdict differs from the register-backed default the generic suite runs
    //! against, and a static provider has no runtime setter, so this is pinned with
    //! `#[scenario_for]` to the static-provider fixtures rather than run as a generic
    //! `#[scenario]`.

    use agent_bus_proto_rust::agent_bus::intention as proto_intention;
    use agentbus_api::Environment;
    use anyhow::Result;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::CommitServiceTestFixture;
    use crate::simulator::Simulator;

    fn intention(agent_id: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: agent_id.to_string(),
            },
            intention: proto_intention::Intention::StringIntention("probe".to_string()),
        }
    }

    /// A static provider configured `OFF_BY_DEFAULT` with no voters denies every
    /// intention — the construction-time policy is observable in the verdict.
    #[scenario_for(CommitServiceV1Fixture<SimpleMemoryFixture, StaticConfigPolicyProvider>, suffix = static_v1_simple_memory, sim_only)]
    #[scenario_for(RandomRoutingCommitServiceFixture<SimpleMemoryFixture, StaticConfigPolicyProvider>, suffix = static_router_simple_memory, sim_only)]
    pub async fn run_static_configured_policy_denies<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
    {
        let env = fixture.get_env();
        let svc = fixture.create_impl();
        let agent_id = format!("static-{}", env.with_rng(|rng| rng.random::<u64>()));

        let denied = svc.commit_intention(intention(&agent_id)).await?;
        anyhow::ensure!(
            !denied.approved,
            "static OFF_BY_DEFAULT (no voter) must deny: {}",
            denied.reason
        );

        Ok(())
    }
}
pub use defs::*;
