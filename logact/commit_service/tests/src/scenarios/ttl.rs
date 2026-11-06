/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_ttl_list)]
mod defs {
    //! `CommitSvc` behavior when entries expire underneath its AgentBus, selected
    //! with `ttl_ms = <int>`.

    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::CheckTailRequest;
    use agent_bus_proto_rust::agent_bus::ReadNextRequest;
    use agent_bus_proto_rust::agent_bus::intention;
    use agentbus_api::AgentBus;
    use agentbus_api::BusId;
    use agentbus_api::Clock;
    use agentbus_api::Environment;
    use anyhow::Result;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::CommitServiceTestFixture;

    /// Logs commits and reads around entry expiry without asserting on them.
    #[scenario(sim_only, ttl_ms = 1000)]
    pub async fn run_test_observe_expired_bus_entries<F: CommitServiceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let env = fixture.get_env();
        let start = env.with_clock(|clock| clock.monotonic_time());
        let log = |event: String| {
            let elapsed = env.with_clock(|clock| clock.monotonic_time()) - start;
            eprintln!("ttl [{elapsed:?}] {event}");
        };
        let bus_id = format!("ttl-{}", env.with_rng(|rng| rng.random::<u64>()));
        let command = |value: &str| CommitIntentionCommand {
            bus_id: BusId {
                agent_bus_id: bus_id.clone(),
            },
            intention: intention::Intention::StringIntention(value.to_owned()),
        };
        let service = fixture.create_impl();

        for value in ["a", "b"] {
            let outcome = service.commit_intention(command(value)).await;
            log(format!("commit {value}: {outcome:?}"));
        }
        env.sleep(Duration::from_millis(600)).await;
        let outcome = service.commit_intention(command("c")).await;
        log(format!("commit c: {outcome:?}"));
        env.sleep(Duration::from_millis(600)).await;

        let id = Some(agent_bus_proto_rust::agent_bus::BusId {
            agent_bus_id: bus_id.clone(),
        });
        let tail = service
            .agent_bus()
            .check_tail(CheckTailRequest { bus_id: id.clone() })
            .await;
        log(format!("check_tail after a and b expire: {tail:?}"));
        let read = service
            .agent_bus()
            .read_next(ReadNextRequest {
                bus_id: id,
                start_log_position: 0,
                end_log_position: tail.as_ref().map_or(0, |tail| tail.tail_position),
                max_entries: 64,
                ..Default::default()
            })
            .await;
        log(format!(
            "read_next from 0: {:?}",
            read.map(|response| response.entries.len())
        ));
        let outcome = service.commit_intention(command("d")).await;
        log(format!("commit d on the same service: {outcome:?}"));
        let fresh = fixture.create_impl();
        let outcome = fresh.commit_intention(command("e")).await;
        log(format!("commit e on a new service: {outcome:?}"));
        Ok(())
    }
}

pub use defs::*;
