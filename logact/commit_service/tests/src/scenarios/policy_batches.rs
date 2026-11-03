/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Replay of `PolicyBatch` entries appended directly to the exposed bus, e.g. by
//! another commit-service instance sharing it.

#[conformance_macros::scenarios(commit_service_policy_batches_list)]
mod defs {
    use agent_bus_proto_rust::agent_bus::BaseEngineControl;
    use agent_bus_proto_rust::agent_bus::Control;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::PolicyBatch;
    use agent_bus_proto_rust::agent_bus::base_engine_control;
    use agent_bus_proto_rust::agent_bus::control;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::AgentBus;
    use agentbus_api::AppendRequest;
    use agentbus_api::Environment;
    use anyhow::Context as _;
    use anyhow::Result;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::DeciderPolicyFixture;
    use crate::fixtures::SettableDeciderPolicy;

    fn intention(bus_id: &str, body: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: bus_id.to_string(),
            },
            intention: intention::Intention::StringIntention(body.to_string()),
        }
    }

    async fn append_policy_batch(
        bus: &impl AgentBus,
        bus_id: &str,
        batch: PolicyBatch,
    ) -> Result<()> {
        bus.append(AppendRequest {
            bus_id: Some(agentbus_api::BusId {
                agent_bus_id: bus_id.to_string(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Control(Control {
                    control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::PolicyBatch(batch)),
                    })),
                })),
            }),
        })
        .await?;
        Ok(())
    }

    /// `PolicyBatch.decider_policy` is optional: an absent value leaves the
    /// decider unchanged, as when a batch only bumps the version or edits voters.
    #[scenario]
    pub async fn run_test_policy_batch_without_decider_policy_keeps_decider<
        F: DeciderPolicyFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        let controller = fixture.policy_controller();
        let version = controller
            .set_decider_policy(DeciderPolicy::OffByDefault)
            .await?;
        let service = fixture.create_impl();
        let bus_id = format!(
            "decider-less-batch-{}",
            fixture.get_env().with_rng(|rng| rng.random::<u64>())
        );
        let initial = service
            .commit_intention(intention(&bus_id, "initial"))
            .await?;
        anyhow::ensure!(!initial.approved, "OFF_BY_DEFAULT must deny");

        let next_version = controller
            .set_decider_policy(DeciderPolicy::OffByDefault)
            .await?;
        append_policy_batch(
            service.agent_bus(),
            &bus_id,
            PolicyBatch {
                expected_current_version: Some(version),
                new_version: next_version,
                ..Default::default()
            },
        )
        .await?;
        let outcome = service
            .commit_intention(intention(&bus_id, "after decider-less batch"))
            .await
            .context("a PolicyBatch without a decider policy must replay")?;
        anyhow::ensure!(
            !outcome.approved,
            "a PolicyBatch without a decider policy must keep OFF_BY_DEFAULT; returned {outcome:?}"
        );
        Ok(())
    }

    /// `PolicyBatch.new_version` must exceed `expected_current_version`; a batch
    /// that does not advance the version is skipped.
    #[scenario]
    pub async fn run_test_non_advancing_policy_batch_is_skipped<F: DeciderPolicyFixture>(
        fixture: &F,
    ) -> Result<()> {
        let controller = fixture.policy_controller();
        let version = controller
            .set_decider_policy(DeciderPolicy::OnByDefault)
            .await?;
        let service = fixture.create_impl();
        let bus_id = format!(
            "non-advancing-batch-{}",
            fixture.get_env().with_rng(|rng| rng.random::<u64>())
        );
        let initial = service
            .commit_intention(intention(&bus_id, "initial"))
            .await?;
        anyhow::ensure!(initial.approved, "ON_BY_DEFAULT must approve");

        append_policy_batch(
            service.agent_bus(),
            &bus_id,
            PolicyBatch {
                expected_current_version: Some(version),
                new_version: version,
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        let outcome = service
            .commit_intention(intention(&bus_id, "after non-advancing batch"))
            .await?;
        anyhow::ensure!(
            outcome.approved,
            "a PolicyBatch with new_version == expected_current_version ({version}) must be \
             skipped, keeping ON_BY_DEFAULT; returned {outcome:?}"
        );
        Ok(())
    }
}

pub use defs::*;
