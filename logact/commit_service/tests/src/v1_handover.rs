/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! V1 and the shared replay service hand a bus back and forth through V1's
//! engine-state record, with no migration step.

use std::collections::HashMap;
use std::rc::Rc;

use agentbus_api::BusId;
use agentbus_tests::common::helpers::read_linearizable_snapshot;
use agentbus_tests::decisions::DecisionReader;
use agentbus_tests::fixtures::simtest::SimpleMemoryFixture;
use conformance::ConformanceFixture;
use conformance::SimulatorFixture;
use futures::executor::block_on;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::PerBusEngineState;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::engine_state_key;
use logact_commit_service_replay::ReplayCommitService;
use logact_commit_service_replay::StorageCheckpoints;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::SynchronousRegisterProvider;
use prost::Message;

use crate::fixtures::TestPolicyProvider;
use crate::fixtures::build_v1;
use crate::simulator::Simulator;

const BUS: &str = "handover";

fn command(value: &str) -> CommitIntentionCommand {
    CommitIntentionCommand {
        bus_id: BusId {
            agent_bus_id: BUS.to_owned(),
        },
        intention: agentbus_api::intention::Intention::StringIntention(value.to_owned()),
    }
}

/// The engine-state record's position, checking that V1 can decode it.
async fn engine_record_position(storage: &InMemoryStorage) -> i64 {
    let (bytes, position) = storage
        .get(&engine_state_key(BUS))
        .await
        .unwrap()
        .expect("the bus should have an engine-state record");
    PerBusEngineState::decode(bytes.as_ref()).expect("V1 should decode the record");
    position
}

#[test]
fn v1_and_shared_replay_hand_over_a_bus_without_migration() {
    let bus_fixture = SimpleMemoryFixture::new(Simulator::new(agentbus_simulator::generate_seed()));
    let environment = bus_fixture.get_env();
    let task_environment = environment.clone();
    let handle = environment.spawn(async move {
        let environment = task_environment;
        let storage = Rc::new(InMemoryStorage::new());
        let (policy, _controller) =
            <SynchronousRegisterProvider<InMemoryStorage> as TestPolicyProvider>::for_test(
                environment.clone(),
            );
        let v1 = || {
            build_v1(
                bus_fixture.create_impl(),
                storage.clone(),
                |_| DelegatingVoterFactory::default(),
                policy.clone(),
                environment.clone(),
            )
        };
        let replay = ReplayCommitService::new(
            bus_fixture.create_impl(),
            DelegatingVoterFactory::default(),
            policy.clone(),
            StorageCheckpoints::new(storage.clone()),
        );

        let mut intentions = Vec::new();
        for value in ["a", "b"] {
            let outcome = v1().commit_intention(command(value)).await.unwrap();
            assert!(outcome.approved);
            intentions.push(outcome.log_position);
        }
        let v1_position = engine_record_position(&storage).await;

        let outcome = replay.commit_intention(command("c")).await.unwrap();
        assert!(outcome.approved);
        intentions.push(outcome.log_position);
        assert!(
            outcome.log_position > v1_position,
            "replay resumes from V1's record instead of replaying the log"
        );
        let replay_position = engine_record_position(&storage).await;
        assert!(replay_position > v1_position);

        let outcome = v1().commit_intention(command("d")).await.unwrap();
        assert!(outcome.approved);
        intentions.push(outcome.log_position);
        assert!(engine_record_position(&storage).await > replay_position);

        let (entries, _) = read_linearizable_snapshot(
            replay.agent_bus(),
            BUS.to_owned(),
            0,
            None,
            environment.as_ref(),
            None,
        )
        .await;
        let mut reader = DecisionReader::default();
        let mut decisions: HashMap<i64, usize> = HashMap::new();
        for decision in entries.iter().filter_map(|entry| reader.observe(entry)) {
            *decisions.entry(decision.intention_id).or_default() += 1;
        }
        for intention in intentions {
            assert_eq!(
                decisions.get(&intention),
                Some(&1),
                "intention at {intention} should be decided exactly once"
            );
        }
    });
    environment.run();
    block_on(handle).expect("the handover task should complete");
}
