/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_policy_versions_list)]
mod defs {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::BaseEngineControl;
    use agent_bus_proto_rust::agent_bus::BusEntry;
    use agent_bus_proto_rust::agent_bus::Control;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::base_engine_control;
    use agent_bus_proto_rust::agent_bus::control;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::Environment;
    use agentbus_tests::common::helpers::read_linearizable_snapshot;
    use anyhow::Context as _;
    use anyhow::Result;
    use futures::StreamExt as _;
    use futures::TryStreamExt as _;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitIntentionOutcome;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::DeciderPolicyFixture;
    use crate::fixtures::SettableDeciderPolicy;
    use crate::simulator::Simulator;

    #[scenario(sim_only)]
    pub async fn run_test_policy_version_rejections_are_justified<F>(fixture: &F) -> Result<()>
    where
        F: DeciderPolicyFixture<Env = Simulator>,
    {
        let environment = fixture.get_env();
        let controller = fixture.policy_controller();
        controller
            .set_decider_policy(DeciderPolicy::OnByDefault)
            .await?;
        let service = fixture.create_impl();
        let bus_id = format!(
            "policy-versions-{}",
            environment.with_rng(|rng| rng.random::<u64>())
        );
        let requests = futures::stream::iter(0..3)
            .map(|client| {
                let service = &service;
                let environment = &environment;
                let bus_id = &bus_id;
                async move {
                    let mut outcomes = Vec::new();
                    for sequence in 0..8 {
                        let delay = environment.with_rng(|rng| rng.random_range(0..=3));
                        environment.sleep(Duration::from_millis(delay)).await;
                        let body = format!("policy-version probe {client}:{sequence}");
                        let outcome = service
                            .commit_intention(CommitIntentionCommand {
                                bus_id: agentbus_api::BusId {
                                    agent_bus_id: bus_id.clone(),
                                },
                                intention: intention::Intention::StringIntention(body.clone()),
                            })
                            .await?;
                        outcomes.push((body, outcome));
                    }
                    Ok::<_, anyhow::Error>(outcomes)
                }
            })
            .buffer_unordered(3)
            .try_collect::<Vec<_>>();
        let updates = async {
            for _ in 0..8 {
                let delay = environment.with_rng(|rng| rng.random_range(0..=3));
                environment.sleep(Duration::from_millis(delay)).await;
                controller
                    .set_decider_policy(DeciderPolicy::OnByDefault)
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let (outcomes, ()) = futures::try_join!(requests, updates)?;
        let (entries, _) = read_linearizable_snapshot(
            service.agent_bus(),
            bus_id,
            0,
            None,
            environment.as_ref(),
            Some(64),
        )
        .await;
        verify_policy_version_outcomes(&entries, outcomes.into_iter().flatten())
    }

    fn verify_policy_version_outcomes(
        entries: &[BusEntry],
        outcomes: impl IntoIterator<Item = (String, CommitIntentionOutcome)>,
    ) -> Result<()> {
        let mut pending = BTreeMap::new();
        for (body, outcome) in outcomes {
            anyhow::ensure!(
                pending
                    .insert(outcome.log_position, (body, outcome))
                    .is_none(),
                "concurrent requests returned the same intention position"
            );
        }
        let mut applied_version = None;
        for entry in entries {
            let position = entry
                .header
                .as_ref()
                .context("entry has no header")?
                .log_position;
            let inner = entry.payload.as_ref().and_then(|p| p.payload.as_ref());
            if let Some(payload::Payload::Control(Control {
                control:
                    Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::PolicyBatch(batch)),
                    })),
            })) = inner
                && batch.expected_current_version == applied_version
            {
                anyhow::ensure!(
                    applied_version.is_none_or(|current| batch.new_version > current),
                    "policy versions must increase"
                );
                applied_version = Some(batch.new_version);
            }
            let Some((body, outcome)) = pending.remove(&position) else {
                continue;
            };
            let Some(payload::Payload::Intention(proposed)) = inner else {
                anyhow::bail!("returned position {position} is not an intention");
            };
            anyhow::ensure!(
                proposed.intention == Some(intention::Intention::StringIntention(body)),
                "returned position {position} contains a different intention"
            );
            let Some(intention::PolicyVersionConstraint::MinimumPolicyVersion(minimum)) =
                proposed.policy_version_constraint
            else {
                anyhow::bail!("intention at {position} has no minimum policy version");
            };
            let satisfied = applied_version.is_some_and(|version| version >= minimum);
            anyhow::ensure!(
                outcome.approved == satisfied,
                "intention={position} minimum={minimum} applied_policy={applied_version:?}: \
                 ON_BY_DEFAULT requires approved={satisfied}, returned {outcome:?}"
            );
        }
        anyhow::ensure!(
            pending.is_empty(),
            "returned intentions are missing from the log"
        );
        Ok(())
    }
}

pub use defs::*;
