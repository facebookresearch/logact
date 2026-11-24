/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_ttl_list)]
mod defs {
    //! AgentBus behavior when entries expire underneath it, selected with
    //! `ttl_ms = <int>`.

    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::*;
    use agentbus_api::AgentBus;
    use agentbus_api::AgentBusError;
    use agentbus_api::environment::Environment;
    use anyhow::Result;
    use rand::RngExt as _;

    use crate::common::helpers::append_string_intention;
    use crate::fixtures::TailWindowFixture;

    fn positions(entries: &[BusEntry]) -> Vec<i64> {
        entries
            .iter()
            .filter_map(|entry| entry.header.as_ref().map(|header| header.log_position))
            .collect()
    }

    #[scenario(sim_only, ttl_ms = 1000)]
    pub async fn run_test_expired_entries_are_not_found<F: TailWindowFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id = format!("ttl-{}", env.with_rng(|rng| rng.random::<u64>()));
        for position in 0..3 {
            append_string_intention(&bus, bus_id.clone(), position.to_string()).await;
        }
        env.sleep(Duration::from_millis(600)).await;
        // A tail window of entries survives, so the bus finds the tail above them.
        let surviving = 3..3 + fixture.tail_window_size() as i64;
        for position in surviving.clone() {
            append_string_intention(&bus, bus_id.clone(), position.to_string()).await;
        }
        env.sleep(Duration::from_millis(600)).await;

        let id = Some(BusId {
            agent_bus_id: bus_id.clone(),
        });
        let tail = bus
            .check_tail(CheckTailRequest { bus_id: id.clone() })
            .await?
            .tail_position;
        assert_eq!(tail, surviving.end);
        for filter in [
            None,
            Some(PayloadTypeFilter {
                payload_types: vec![SelectivePollType::Vote as i32],
            }),
        ] {
            assert!(matches!(
                bus.read_next(ReadNextRequest {
                    bus_id: id.clone(),
                    start_log_position: 0,
                    end_log_position: tail,
                    max_entries: 10,
                    filter: filter.clone(),
                })
                .await,
                Err(AgentBusError::PositionNotFound(_))
            ));
            assert!(matches!(
                bus.poll(PollRequest {
                    bus_id: id.clone(),
                    start_log_position: 0,
                    max_entries: 10,
                    filter: filter.clone(),
                })
                .await,
                Err(AgentBusError::PositionNotFound(_))
            ));
            for timeout_ms in [0, 1000] {
                assert!(matches!(
                    bus.blocking_poll(BlockingPollRequest {
                        bus_id: id.clone(),
                        start_log_position: 0,
                        max_entries: 10,
                        filter: filter.clone(),
                        timeout_ms,
                    })
                    .await,
                    Err(AgentBusError::PositionNotFound(_))
                ));
            }
        }
        let read = bus
            .read_next(ReadNextRequest {
                bus_id: id.clone(),
                start_log_position: surviving.start,
                end_log_position: tail,
                max_entries: 10,
                ..Default::default()
            })
            .await?;
        assert_eq!(positions(&read.entries), Vec::from_iter(surviving.clone()));
        assert_eq!(read.next_start_position, tail);
        let polled = bus
            .poll(PollRequest {
                bus_id: id,
                start_log_position: surviving.start,
                max_entries: 10,
                ..Default::default()
            })
            .await?;
        assert_eq!(positions(&polled.entries), Vec::from_iter(surviving));
        assert_eq!(
            append_string_intention(&bus, bus_id, tail.to_string()).await,
            tail
        );
        Ok(())
    }

    #[scenario(sim_only, ttl_ms = 1000)]
    pub async fn run_test_append_discovers_surviving_tail_after_expiry<F: TailWindowFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let append = async |bus: &F::Impl, bus_id: &str, position: i64| {
            append_string_intention(bus, bus_id.to_owned(), position.to_string()).await
        };
        for position in 0..2 {
            assert_eq!(append(&bus, "a", position).await, position);
        }
        env.sleep(Duration::from_millis(600)).await;
        // A tail window of entries survives, so the bus finds the tail above them.
        let surviving = 2..2 + fixture.tail_window_size() as i64;
        for position in surviving.clone() {
            assert_eq!(append(&bus, "a", position).await, position);
        }
        env.sleep(Duration::from_millis(600)).await;

        let fresh = fixture.create_impl();
        let tail = surviving.end;
        assert_eq!(
            append(&fresh, "a", tail).await,
            tail,
            "a fresh writer must discover the surviving suffix before writing"
        );
        assert_eq!(append(&bus, "b", 0).await, 0);
        assert_eq!(
            append(&bus, "a", tail + 1).await,
            tail + 1,
            "a writer must follow the surviving tail without reclaiming expired positions"
        );
        assert!(matches!(
            bus.read_next(ReadNextRequest {
                bus_id: Some(BusId {
                    agent_bus_id: "a".to_owned(),
                }),
                start_log_position: 0,
                end_log_position: 1,
                max_entries: 1,
                ..Default::default()
            })
            .await,
            Err(AgentBusError::PositionNotFound(_))
        ));
        let read = bus
            .read_next(ReadNextRequest {
                bus_id: Some(BusId {
                    agent_bus_id: "a".to_owned(),
                }),
                start_log_position: surviving.start,
                end_log_position: tail + 2,
                max_entries: 10,
                ..Default::default()
            })
            .await?;
        assert_eq!(
            positions(&read.entries),
            Vec::from_iter(surviving.start..tail + 2)
        );
        Ok(())
    }
}

pub use defs::*;
