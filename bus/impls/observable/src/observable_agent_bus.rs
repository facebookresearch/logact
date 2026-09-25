/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;
use std::time::Duration;

use agentbus_api::AgentBus;
use agentbus_api::AgentBusError;
use agentbus_api::AppendRequest;
use agentbus_api::AppendResponse;
use agentbus_api::BlockingPollRequest;
use agentbus_api::BlockingPollResponse;
use agentbus_api::BusResult;
use agentbus_api::CheckTailRequest;
use agentbus_api::CheckTailResponse;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::LogRow;
use agentbus_api::MonotonicInstant;
use agentbus_api::Payload;
use agentbus_api::PollRequest;
use agentbus_api::PollResponse;
use agentbus_api::ReadNextRequest;
use agentbus_api::ReadNextResponse;
use agentbus_api::logger::AgentbusLogger;
use agentbus_api::metrics::AgentBusMetrics;
use agentbus_api::payload;
use agentbus_api::resolve_bus_id;

const NO_TAGS: &[(&str, &str)] = &[];
const COMPONENT: &str = "agent_bus";

pub struct ObservableAgentBus<T, E: Environment> {
    inner: T,
    metrics: Rc<dyn AgentBusMetrics>,
    logger: Rc<dyn AgentbusLogger>,
    environment: Rc<E>,
}

impl<T: Clone, E: Environment> Clone for ObservableAgentBus<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
            logger: self.logger.clone(),
            environment: self.environment.clone(),
        }
    }
}

impl<T: AgentBus, E: Environment> ObservableAgentBus<T, E> {
    pub fn new(
        inner: T,
        metrics: Rc<dyn AgentBusMetrics>,
        logger: Rc<dyn AgentbusLogger>,
        environment: Rc<E>,
    ) -> Self {
        Self {
            inner,
            metrics,
            logger,
            environment,
        }
    }

    fn monotonic_time(&self) -> MonotonicInstant {
        self.environment.with_clock(|c| c.monotonic_time())
    }

    fn elapsed(&self, start: MonotonicInstant) -> Duration {
        self.monotonic_time().duration_since(start)
    }

    fn base_row(&self, method: &str, bus_id: &str, elapsed: Duration) -> LogRow {
        LogRow::default()
            .api_call_name(method)
            .latency_ms(elapsed.as_millis() as u64)
            .add_string_field("component", COMPONENT)
            .add_string_field("resource_id", bus_id)
    }
}

fn payload_type_name(payload: &Payload) -> &'static str {
    match payload.payload.as_ref() {
        Some(payload::Payload::Intention(_)) => "intention",
        Some(payload::Payload::Vote(_)) => "vote",
        Some(payload::Payload::DeciderPolicy(_)) => "decider_policy",
        Some(payload::Payload::Commit(_)) => "commit",
        Some(payload::Payload::Abort(_)) => "abort",
        Some(payload::Payload::VoterPolicy(_)) => "voter_policy",
        Some(payload::Payload::Control(_)) => "control",
        Some(payload::Payload::InferenceInput(_)) => "inference_input",
        Some(payload::Payload::InferenceOutput(_)) => "inference_output",
        Some(payload::Payload::ActionOutput(_)) => "action_output",
        Some(payload::Payload::AgentInput(_)) => "agent_input",
        Some(payload::Payload::AgentOutput(_)) => "agent_output",
        Some(payload::Payload::Mail(_)) => "mail",
        None => "unknown",
    }
}

fn error_code(error: &AgentBusError) -> &'static str {
    match error {
        AgentBusError::InvalidArgument(_) => "BusInvalidArgument",
        AgentBusError::Timeout(_) => "BusTimeout",
        AgentBusError::Unavailable(_) => "BusUnavailable",
        AgentBusError::Internal(_) => "BusInternal",
    }
}

fn add_result_status<T>(row: LogRow, result: &BusResult<T>) -> LogRow {
    match result {
        Ok(_) => row.add_i64_field("success", 1),
        Err(err) => row
            .add_i64_field("success", 0)
            .error_code(error_code(err))
            .error_message(format!("{err:#}")),
    }
}

impl<T: AgentBus, E: Environment> AgentBus for ObservableAgentBus<T, E> {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        self.metrics
            .record_counter("append.agent_bus.num_calls", 1, NO_TAGS);
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        let payload_type = request
            .payload
            .as_ref()
            .map(payload_type_name)
            .unwrap_or("unknown");
        let start = self.monotonic_time();

        let result = self.inner.append(request).await;

        let elapsed = self.elapsed(start);
        self.metrics.record_histogram(
            "append.agent_bus.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );

        if result.is_err() {
            self.metrics
                .record_counter("append.agent_bus.num_errors", 1, NO_TAGS);
        }

        let mut row = add_result_status(
            self.base_row("append", &bus_id, elapsed)
                .add_string_field("payload_type", payload_type),
            &result,
        );
        if let Ok(response) = &result {
            row = row.add_i64_field("position", response.log_position);
        }
        self.logger.log_row(row);

        result
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        self.metrics
            .record_counter("poll.agent_bus.num_calls", 1, NO_TAGS);
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        let start_position = request.start_log_position;
        let max_entries = request.max_entries;
        let start = self.monotonic_time();

        let result = self.inner.poll(request).await;

        let elapsed = self.elapsed(start);
        self.metrics.record_histogram(
            "poll.agent_bus.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );

        if let Ok(ref response) = result {
            self.metrics.record_histogram(
                "poll.agent_bus.entries_returned",
                response.entries.len() as i64,
                NO_TAGS,
            );
        } else {
            self.metrics
                .record_counter("poll.agent_bus.num_errors", 1, NO_TAGS);
        }

        let row = self
            .base_row("poll", &bus_id, elapsed)
            .add_i64_field("position", start_position)
            .add_i64_field("max_entries", max_entries as i64);
        let mut row = add_result_status(row, &result);
        if let Ok(response) = &result {
            row = row
                .add_i64_field("entries_returned", response.entries.len() as i64)
                .add_i64_field("complete", if response.complete { 1 } else { 0 });
        }
        self.logger.log_row(row);

        result
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        self.metrics
            .record_counter("read_next.agent_bus.num_calls", 1, NO_TAGS);
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        let start_position = request.start_log_position;
        let max_entries = request.max_entries;
        let start = self.monotonic_time();

        let result = self.inner.read_next(request).await;

        let elapsed = self.elapsed(start);
        self.metrics.record_histogram(
            "read_next.agent_bus.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );

        if let Ok(ref response) = result {
            self.metrics.record_histogram(
                "read_next.agent_bus.entries_returned",
                response.entries.len() as i64,
                NO_TAGS,
            );
        } else {
            self.metrics
                .record_counter("read_next.agent_bus.num_errors", 1, NO_TAGS);
        }

        let row = self
            .base_row("read_next", &bus_id, elapsed)
            .add_i64_field("position", start_position)
            .add_i64_field("max_entries", max_entries as i64);
        let mut row = add_result_status(row, &result);
        if let Ok(response) = &result {
            row = row
                .add_i64_field("entries_returned", response.entries.len() as i64)
                .add_i64_field("next_position", response.next_start_position);
        }
        self.logger.log_row(row);

        result
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        self.metrics
            .record_counter("check_tail.agent_bus.num_calls", 1, NO_TAGS);
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        let start = self.monotonic_time();

        let result = self.inner.check_tail(request).await;

        let elapsed = self.elapsed(start);
        self.metrics.record_histogram(
            "check_tail.agent_bus.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );

        if result.is_err() {
            self.metrics
                .record_counter("check_tail.agent_bus.num_errors", 1, NO_TAGS);
        }

        let mut row = add_result_status(self.base_row("check_tail", &bus_id, elapsed), &result);
        if let Ok(response) = &result {
            row = row.add_i64_field("tail_position", response.tail_position);
        }
        self.logger.log_row(row);

        result
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        self.metrics
            .record_counter("blocking_poll.agent_bus.num_calls", 1, NO_TAGS);
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref()).to_owned();
        let start_position = request.start_log_position;
        let max_entries = request.max_entries;
        let timeout_ms = request.timeout_ms;
        let start = self.monotonic_time();

        let result = self.inner.blocking_poll(request).await;

        let elapsed = self.elapsed(start);
        self.metrics.record_histogram(
            "blocking_poll.agent_bus.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );

        if let Ok(ref response) = result {
            self.metrics.record_histogram(
                "blocking_poll.agent_bus.entries_returned",
                response.entries.len() as i64,
                NO_TAGS,
            );
        } else {
            self.metrics
                .record_counter("blocking_poll.agent_bus.num_errors", 1, NO_TAGS);
        }

        let row = self
            .base_row("blocking_poll", &bus_id, elapsed)
            .add_i64_field("position", start_position)
            .add_i64_field("max_entries", max_entries as i64)
            .add_i64_field("timeout_ms", timeout_ms as i64);
        let mut row = add_result_status(row, &result);
        if let Ok(response) = &result {
            row = row
                .add_i64_field("entries_returned", response.entries.len() as i64)
                .add_i64_field("next_position", response.next_start_position);
        }
        self.logger.log_row(row);

        result
    }
}
