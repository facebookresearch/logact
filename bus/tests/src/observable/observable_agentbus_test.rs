/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::AppendRequest;
use agentbus_api::BlockingPollRequest;
use agentbus_api::BusId;
use agentbus_api::CheckTailRequest;
use agentbus_api::InMemoryLogger;
use agentbus_api::InMemoryMetrics;
use agentbus_api::Intention;
use agentbus_api::LogRow;
use agentbus_api::Payload;
use agentbus_api::PayloadTypeFilter;
use agentbus_api::PollRequest;
use agentbus_api::ReadNextRequest;
use agentbus_api::intention;
use agentbus_api::payload;
use agentbus_observable::ObservableAgentBus;
use agentbus_simple::InMemoryAgentBus;

use crate::simulator::Simulator;

fn field(row: &LogRow, key: &str) -> Option<String> {
    row.additional_fields
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.to_string())
}

#[test]
fn test_metrics_recorded_on_append_and_poll() {
    let seed: u64 = rand::random();
    let simulator = Simulator::new(seed);
    let env = Rc::new(simulator);
    let inner = InMemoryAgentBus::new(env.clone());
    let metrics = Rc::new(InMemoryMetrics::new());
    let logger = Rc::new(InMemoryLogger::new());
    let bus = ObservableAgentBus::new(inner, metrics.clone(), logger.clone(), env.clone());

    let handle = env.spawn(async move {
        let payload = Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention("hello".to_string())),
                ..Default::default()
            })),
        };
        bus.append(AppendRequest {
            agent_bus_id: "test-bus".to_string(),
            bus_id: Some(BusId {
                agent_bus_id: "test-bus".to_string(),
            }),
            payload: Some(payload),
            ..Default::default()
        })
        .await
        .expect("append should succeed");

        bus.poll(PollRequest {
            agent_bus_id: "test-bus".to_string(),
            bus_id: Some(BusId {
                agent_bus_id: "test-bus".to_string(),
            }),
            start_log_position: 0,
            max_entries: 10,
            ..Default::default()
        })
        .await
        .expect("poll should succeed");

        bus.check_tail(CheckTailRequest {
            agent_bus_id: "test-bus".to_string(),
            bus_id: Some(BusId {
                agent_bus_id: "test-bus".to_string(),
            }),
        })
        .await
        .expect("check_tail should succeed");

        bus.read_next(ReadNextRequest {
            agent_bus_id: "test-bus".to_string(),
            bus_id: Some(BusId {
                agent_bus_id: "test-bus".to_string(),
            }),
            start_log_position: 0,
            end_log_position: 1,
            max_entries: 10,
            filter: None,
        })
        .await
        .expect("read_next should succeed");

        bus.blocking_poll(BlockingPollRequest {
            agent_bus_id: "test-bus".to_string(),
            bus_id: Some(BusId {
                agent_bus_id: "test-bus".to_string(),
            }),
            start_log_position: 0,
            max_entries: 10,
            filter: None,
            timeout_ms: 100,
        })
        .await
        .expect("blocking_poll should succeed");
    });

    env.run();
    futures::executor::block_on(handle).expect("test task should complete");

    assert_eq!(metrics.counter("append.agent_bus.num_calls"), 1);
    assert_eq!(metrics.histogram("append.agent_bus.latency_us").len(), 1);
    assert_eq!(metrics.counter("append.agent_bus.num_errors"), 0);

    assert_eq!(metrics.counter("poll.agent_bus.num_calls"), 1);
    assert_eq!(metrics.histogram("poll.agent_bus.latency_us").len(), 1);
    assert_eq!(
        metrics.histogram("poll.agent_bus.entries_returned").len(),
        1
    );
    assert_eq!(metrics.counter("poll.agent_bus.num_errors"), 0);

    assert_eq!(metrics.counter("check_tail.agent_bus.num_calls"), 1);
    assert_eq!(
        metrics.histogram("check_tail.agent_bus.latency_us").len(),
        1
    );
    assert_eq!(metrics.counter("check_tail.agent_bus.num_errors"), 0);

    assert_eq!(metrics.counter("read_next.agent_bus.num_calls"), 1);
    assert_eq!(metrics.histogram("read_next.agent_bus.latency_us").len(), 1);
    assert_eq!(
        metrics
            .histogram("read_next.agent_bus.entries_returned")
            .len(),
        1
    );
    assert_eq!(metrics.counter("read_next.agent_bus.num_errors"), 0);

    assert_eq!(metrics.counter("blocking_poll.agent_bus.num_calls"), 1);
    assert_eq!(
        metrics
            .histogram("blocking_poll.agent_bus.latency_us")
            .len(),
        1
    );
    assert_eq!(
        metrics
            .histogram("blocking_poll.agent_bus.entries_returned")
            .len(),
        1
    );
    assert_eq!(metrics.counter("blocking_poll.agent_bus.num_errors"), 0);

    let rows = logger.rows();
    assert_eq!(rows.len(), 5, "one structured row per AgentBus call");

    let append = &rows[0];
    assert_eq!(append.api_call_name, "append");
    assert_eq!(field(append, "component"), Some("agent_bus".to_string()));
    assert_eq!(field(append, "resource_id"), Some("test-bus".to_string()));
    assert_eq!(field(append, "payload_type"), Some("intention".to_string()));
    assert_eq!(field(append, "success"), Some("1".to_string()));
    assert_eq!(field(append, "position"), Some("0".to_string()));

    let poll = &rows[1];
    assert_eq!(poll.api_call_name, "poll");
    assert_eq!(field(poll, "position"), Some("0".to_string()));
    assert_eq!(field(poll, "start_position"), None);
    assert_eq!(field(poll, "max_entries"), Some("10".to_string()));
    assert_eq!(field(poll, "entries_returned"), Some("1".to_string()));
    assert_eq!(field(poll, "complete"), Some("1".to_string()));

    let check_tail = &rows[2];
    assert_eq!(check_tail.api_call_name, "check_tail");
    assert_eq!(field(check_tail, "tail_position"), Some("1".to_string()));

    let read_next = &rows[3];
    assert_eq!(read_next.api_call_name, "read_next");
    assert_eq!(field(read_next, "position"), Some("0".to_string()));
    assert_eq!(field(read_next, "start_position"), None);
    assert_eq!(field(read_next, "end_position"), None);
    assert_eq!(field(read_next, "entries_returned"), Some("1".to_string()));
    assert_eq!(field(read_next, "next_position"), Some("1".to_string()));

    let blocking_poll = &rows[4];
    assert_eq!(blocking_poll.api_call_name, "blocking_poll");
    assert_eq!(field(blocking_poll, "position"), Some("0".to_string()));
    assert_eq!(field(blocking_poll, "start_position"), None);
    assert_eq!(field(blocking_poll, "timeout_ms"), Some("100".to_string()));
    assert_eq!(
        field(blocking_poll, "entries_returned"),
        Some("1".to_string())
    );
}

#[test]
fn test_filter_payload_types_are_not_logged() {
    let seed: u64 = rand::random();
    let simulator = Simulator::new(seed);
    let env = Rc::new(simulator);
    let inner = InMemoryAgentBus::new(env.clone());
    let metrics = Rc::new(InMemoryMetrics::new());
    let logger = Rc::new(InMemoryLogger::new());
    let bus = ObservableAgentBus::new(inner, metrics, logger.clone(), env.clone());

    let handle = env.spawn(async move {
        bus.poll(PollRequest {
            agent_bus_id: "test-bus".to_string(),
            bus_id: Some(BusId {
                agent_bus_id: "test-bus".to_string(),
            }),
            start_log_position: 0,
            max_entries: 10,
            filter: Some(PayloadTypeFilter {
                payload_types: Vec::new(),
            }),
        })
        .await
        .expect("poll with an empty filter should succeed");
    });

    env.run();
    futures::executor::block_on(handle).expect("test task should complete");

    let rows = logger.rows();
    assert_eq!(rows.len(), 1);
    let poll = &rows[0];
    assert_eq!(poll.api_call_name, "poll");
    assert_eq!(field(poll, "filter_payload_types"), None);
    assert_eq!(field(poll, "position"), Some("0".to_string()));
    assert_eq!(field(poll, "entries_returned"), Some("0".to_string()));
}
