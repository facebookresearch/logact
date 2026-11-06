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
    use crate::fixtures::AgentBusTestFixture;

    fn positions(entries: &[BusEntry]) -> Vec<i64> {
        entries
            .iter()
            .filter_map(|entry| entry.header.as_ref().map(|header| header.log_position))
            .collect()
    }

    #[scenario(sim_only, ttl_ms = 1000)]
    pub async fn run_test_expired_entries_are_not_found<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let bus_id = format!("ttl-{}", env.with_rng(|rng| rng.random::<u64>()));
        for value in ["0", "1", "2"] {
            append_string_intention(&bus, bus_id.clone(), value.to_owned()).await;
        }
        env.sleep(Duration::from_millis(600)).await;
        for value in ["3", "4"] {
            append_string_intention(&bus, bus_id.clone(), value.to_owned()).await;
        }
        env.sleep(Duration::from_millis(600)).await;

        let id = Some(BusId {
            agent_bus_id: bus_id.clone(),
        });
        let tail = bus
            .check_tail(CheckTailRequest { bus_id: id.clone() })
            .await?
            .tail_position;
        assert_eq!(tail, 5);
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
                start_log_position: 3,
                end_log_position: tail,
                max_entries: 10,
                ..Default::default()
            })
            .await?;
        assert_eq!(positions(&read.entries), [3, 4]);
        assert_eq!(read.next_start_position, tail);
        let polled = bus
            .poll(PollRequest {
                bus_id: id,
                start_log_position: 3,
                max_entries: 10,
                ..Default::default()
            })
            .await?;
        assert_eq!(positions(&polled.entries), [3, 4]);
        assert_eq!(
            append_string_intention(&bus, bus_id, "5".to_owned()).await,
            5
        );
        Ok(())
    }
}

pub use defs::*;
