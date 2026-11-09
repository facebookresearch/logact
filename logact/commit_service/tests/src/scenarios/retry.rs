/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_retry_list)]
mod defs {
    //! CommitSvc behavior under injected latency and loss with retries: a commit
    //! either returns its intention's decision or fails transiently, its intention
    //! lands at most once, and every intention is decided once the faults stop.

    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::BusEntry;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::BusId;
    use agentbus_api::Environment;
    use agentbus_api::intention_texts;
    use agentbus_tests::common::helpers::read_linearizable_snapshot;
    use anyhow::Result;
    use logact_commit_service_api::CommitError;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use logact_commit_service_api::CreateBusCommand;
    use rand::RngExt as _;

    use crate::fixtures::CommitServiceTestFixture;
    use crate::simulator::Simulator;

    /// Longer than a request still in flight when injection stops can take to
    /// land: these scenarios' hops take at most 65s.
    const DRAIN: Duration = Duration::from_secs(120);

    fn command(bus_id: &str, value: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: BusId {
                agent_bus_id: bus_id.to_owned(),
            },
            intention: intention::Intention::StringIntention(value.to_owned()),
        }
    }

    fn is_transient(error: &CommitError) -> bool {
        match error {
            CommitError::Timeout(_) | CommitError::Unavailable(_) => true,
            CommitError::BusDataLoss(_)
            | CommitError::InvalidArgument(_)
            | CommitError::Concurrency(_)
            | CommitError::Internal(_) => false,
        }
    }

    /// Each intention's text, by position.
    fn intentions(entries: &[BusEntry]) -> HashMap<i64, String> {
        entries
            .iter()
            .filter_map(|entry| match entry.payload.as_ref()?.payload.as_ref()? {
                payload::Payload::Intention(Intention {
                    intention: Some(intention),
                    ..
                }) => Some((
                    entry.header.as_ref()?.log_position,
                    intention_texts(intention).ok()?.concat(),
                )),
                _ => None,
            })
            .collect()
    }

    /// Whether each decided intention, by position, was approved by its first
    /// decision.
    fn decisions(entries: &[BusEntry]) -> HashMap<i64, bool> {
        let mut decisions = HashMap::new();
        for entry in entries {
            let (intention_id, approved) =
                match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
                    Some(payload::Payload::Commit(commit)) => (commit.intention_id, true),
                    Some(payload::Payload::Abort(abort)) => (abort.intention_id, false),
                    _ => continue,
                };
            decisions.entry(intention_id).or_insert(approved);
        }
        decisions
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_commits_under_loss_are_decided_at_most_once<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
        F::Impl: 'static,
    {
        const CLIENTS: usize = 4;
        const COMMITS: usize = 8;
        let env = fixture.get_env();
        let bus_id = format!("retry-{}", env.with_rng(|rng| rng.random::<u64>()));

        // `create_bus` is idempotent, so its caller can retry it.
        let service = fixture.create_impl();
        let mut attempts = 0;
        while let Err(error) = service
            .create_bus(CreateBusCommand {
                bus_id: bus_id.clone(),
            })
            .await
        {
            attempts += 1;
            anyhow::ensure!(
                is_transient(&error) && attempts < 10,
                "create_bus failed: {error:?}"
            );
        }

        let clients: Vec<_> = (0..CLIENTS)
            .map(|client| {
                let (service, bus_id) = (fixture.create_impl(), bus_id.clone());
                env.spawn(async move {
                    let mut acked = Vec::new();
                    for i in 0..COMMITS {
                        let text = format!("{client}-{i}");
                        match service.commit_intention(command(&bus_id, &text)).await {
                            Ok(outcome) => acked.push((text, outcome)),
                            Err(error) if is_transient(&error) => {}
                            Err(error) => panic!("commit {text} failed: {error:?}"),
                        }
                    }
                    acked
                })
            })
            .collect();
        let mut acked = Vec::new();
        for client in clients {
            acked.extend(client.await?);
        }

        env.latency_injector().disable();
        env.sleep(DRAIN).await;
        let service = fixture.create_impl();
        let last = service.commit_intention(command(&bus_id, "last")).await?;
        acked.push(("last".to_owned(), last));

        let (entries, _) =
            read_linearizable_snapshot(service.agent_bus(), bus_id.clone(), 0, None, &*env, None)
                .await;
        let intentions = intentions(&entries);
        let decisions = decisions(&entries);
        let mut texts = HashSet::new();
        for (position, text) in &intentions {
            anyhow::ensure!(texts.insert(text), "intention {text} landed twice");
            anyhow::ensure!(
                decisions.contains_key(position),
                "intention {text} at {position} is undecided"
            );
        }
        for (text, outcome) in &acked {
            assert_eq!(intentions.get(&outcome.log_position), Some(text));
            assert_eq!(
                decisions.get(&outcome.log_position),
                Some(&outcome.approved),
                "the outcome of {text} should be its intention's first decision"
            );
        }
        Ok(())
    }
}
pub use defs::*;
