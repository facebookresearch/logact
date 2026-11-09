/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_retry_list)]
mod defs {
    //! AgentBus behavior under injected latency and loss with retries: reads
    //! always succeed, and appends land at most once.

    use std::collections::HashMap;
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::*;
    use agentbus_api::AgentBus;
    use agentbus_api::AgentBusError;
    use agentbus_api::environment::Environment;
    use agentbus_api::intention_texts;
    use agentbus_simulator::Simulator;
    use anyhow::Result;
    use conformance::ConformanceFixture;
    use rand::RngExt as _;

    use crate::common::helpers::append_string_intention;
    use crate::common::helpers::read_linearizable_snapshot;
    use crate::fixtures::AgentBusTestFixture;

    /// Longer than a request still in flight when injection stops can take to
    /// land: these scenarios' hops take at most 65s.
    const DRAIN: Duration = Duration::from_secs(120);

    fn texts(entries: &[BusEntry]) -> Vec<String> {
        entries
            .iter()
            .map(
                |entry| match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
                    Some(payload::Payload::Intention(Intention {
                        intention: Some(intention),
                        ..
                    })) => intention_texts(intention)
                        .expect("string intention")
                        .concat(),
                    other => panic!("expected an intention, got {other:?}"),
                },
            )
            .collect()
    }

    fn append_request(bus_id: &str, text: String) -> AppendRequest {
        AppendRequest {
            bus_id: Some(BusId {
                agent_bus_id: bus_id.to_owned(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention(text)),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_reads_under_loss_succeed<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        const ENTRIES: i64 = 5;
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id = format!("retry-{}", env.with_rng(|rng| rng.random::<u64>()));
        let id = Some(BusId {
            agent_bus_id: bus_id.clone(),
        });
        let values: Vec<String> = (0..ENTRIES).map(|i| i.to_string()).collect();

        env.latency_injector().disable();
        for value in &values {
            append_string_intention(&bus, bus_id.clone(), value.clone()).await;
        }
        env.latency_injector().enable();

        for _ in 0..20 {
            let tail = bus
                .check_tail(CheckTailRequest { bus_id: id.clone() })
                .await?;
            assert_eq!(tail.tail_position, ENTRIES);
            let poll = bus
                .poll(PollRequest {
                    bus_id: id.clone(),
                    start_log_position: 0,
                    max_entries: ENTRIES as i32,
                    filter: None,
                })
                .await?;
            assert_eq!(texts(&poll.entries), values);
            let read = bus
                .read_next(ReadNextRequest {
                    bus_id: id.clone(),
                    start_log_position: 0,
                    end_log_position: ENTRIES,
                    max_entries: ENTRIES as i32,
                    filter: None,
                })
                .await?;
            assert_eq!(texts(&read.entries), values);
            assert_eq!(read.next_start_position, ENTRIES);
            let blocking = bus
                .blocking_poll(BlockingPollRequest {
                    bus_id: id.clone(),
                    start_log_position: 0,
                    max_entries: ENTRIES as i32,
                    filter: None,
                    timeout_ms: 1000,
                })
                .await?;
            assert_eq!(texts(&blocking.entries), values);
            assert_eq!(blocking.next_start_position, ENTRIES);
        }
        Ok(())
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_appends_under_loss_land_at_most_once<F>(fixture: &F) -> Result<()>
    where
        F: AgentBusTestFixture + ConformanceFixture<Env = Simulator>,
    {
        const CLIENTS: usize = 4;
        const APPENDS: usize = 16;
        let env = fixture.get_env();
        let bus_id = format!("retry-{}", env.with_rng(|rng| rng.random::<u64>()));

        let clients: Vec<_> = (0..CLIENTS)
            .map(|client| {
                let (bus, bus_id) = (fixture.create_impl(), bus_id.clone());
                env.spawn(async move {
                    let mut acked = Vec::new();
                    for i in 0..APPENDS {
                        let text = format!("{client}-{i}");
                        match bus.append(append_request(&bus_id, text.clone())).await {
                            Ok(response) => acked.push((text, response.log_position)),
                            Err(AgentBusError::Timeout(_) | AgentBusError::Unavailable(_)) => {}
                            Err(error) => panic!("append {text} failed: {error:?}"),
                        }
                        bus.check_tail(CheckTailRequest {
                            bus_id: Some(BusId {
                                agent_bus_id: bus_id.clone(),
                            }),
                        })
                        .await
                        .expect("check_tail should be retried to success");
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
        let bus = fixture.create_impl();
        let (entries, tail) =
            read_linearizable_snapshot(&bus, bus_id.clone(), 0, None, &*env, None).await;
        let log = texts(&entries);
        assert_eq!(log.len() as i64, tail, "the log should have no holes");
        let positions: HashMap<&str, usize> = log
            .iter()
            .enumerate()
            .map(|(position, text)| (text.as_str(), position))
            .collect();
        assert_eq!(
            positions.len(),
            log.len(),
            "an append landed twice: {log:?}"
        );
        for text in &log {
            let (client, i) = text.split_once('-').expect("an appended payload");
            assert!(client.parse::<usize>()? < CLIENTS && i.parse::<usize>()? < APPENDS);
        }
        for (text, position) in &acked {
            assert_eq!(positions.get(text.as_str()), Some(&(*position as usize)));
        }

        let position = append_string_intention(&bus, bus_id, "after".to_owned()).await;
        assert_eq!(position, tail);
        Ok(())
    }
}
pub use defs::*;
