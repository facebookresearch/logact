/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;
use std::time::Duration;

use agentbus_api::AgentBusMetrics;
use agentbus_api::AgentbusLogger;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::LogRow;
use agentbus_api::MonotonicInstant;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;

const NO_TAGS: &[(&str, &str)] = &[];
const COMPONENT: &str = "commit_service";

/// Decorates a Rust [`CommitSvc`] with metrics and structured-row logging.
pub struct ObservableCommitSvc<T, E: Environment> {
    inner: T,
    metrics: Rc<dyn AgentBusMetrics>,
    logger: Rc<dyn AgentbusLogger>,
    environment: Rc<E>,
}

impl<T, E: Environment> ObservableCommitSvc<T, E> {
    /// Wrap `inner` with the supplied metric, row-logging, and clock backends.
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
        self.environment.with_clock(|clock| clock.monotonic_time())
    }

    fn elapsed(&self, start: MonotonicInstant) -> Duration {
        self.monotonic_time().duration_since(start)
    }

    fn base_row(&self, bus_id: &str, elapsed: Duration) -> LogRow {
        LogRow::default()
            .api_call_name("commit_intention")
            .latency_ms(elapsed.as_millis() as u64)
            .add_string_field("component", COMPONENT)
            .add_string_field("resource_id", bus_id)
    }
}

impl<T: Clone, E: Environment> Clone for ObservableCommitSvc<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
            logger: self.logger.clone(),
            environment: self.environment.clone(),
        }
    }
}

fn error_code(error: &CommitError) -> &'static str {
    match error {
        CommitError::InvalidArgument(_) => "CommitInvalidArgument",
        CommitError::Concurrency(_) => "CommitConcurrency",
        CommitError::Timeout(_) => "CommitTimeout",
        CommitError::Unavailable(_) => "CommitUnavailable",
        CommitError::Internal(_) => "CommitInternal",
    }
}

fn add_result_status<T>(row: LogRow, result: &CommitResult<T>) -> LogRow {
    match result {
        Ok(_) => row.add_i64_field("success", 1),
        Err(error) => row
            .add_i64_field("success", 0)
            .error_code(error_code(error))
            .error_message(format!("{error:#}")),
    }
}

impl<T: CommitSvc, E: Environment> CommitSvc for ObservableCommitSvc<T, E> {
    type Bus = T::Bus;

    fn agent_bus(&self) -> &Self::Bus {
        self.inner.agent_bus()
    }

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.inner.create_bus(request).await
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.metrics
            .record_counter("commit_intention.commit_service.num_calls", 1, NO_TAGS);
        let bus_id = request.bus_id.agent_bus_id.clone();
        let start = self.monotonic_time();

        let result = self.inner.commit_intention(request).await;

        let elapsed = self.elapsed(start);
        self.metrics.record_histogram(
            "commit_intention.commit_service.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );
        if result.is_err() {
            self.metrics
                .record_counter("commit_intention.commit_service.num_errors", 1, NO_TAGS);
        }

        let mut row = add_result_status(self.base_row(&bus_id, elapsed), &result);
        if let Ok(outcome) = &result {
            row = row
                .add_i64_field("approved", i64::from(outcome.approved))
                .add_string_field("reason", outcome.reason.as_str())
                .add_i64_field("position", outcome.log_position);
        }
        self.logger.log_row(row);

        result
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentbus_api::BusId;
    use agentbus_api::InMemoryLogger;
    use agentbus_api::InMemoryMetrics;
    use agentbus_api::LogFieldValue;
    use agentbus_api::RealEnvironment;
    use agentbus_api::intention;
    use agentbus_simple::InMemoryAgentBus;
    use futures::executor::block_on;

    use super::*;

    #[derive(Clone, Copy)]
    enum FakeResponse {
        Approved,
        Timeout,
    }

    #[derive(Clone)]
    struct FakeCommitSvc {
        bus: InMemoryAgentBus<RealEnvironment>,
        requests: Rc<RefCell<Vec<CommitIntentionCommand>>>,
        response: FakeResponse,
    }

    impl CommitSvc for FakeCommitSvc {
        type Bus = InMemoryAgentBus<RealEnvironment>;

        fn agent_bus(&self) -> &Self::Bus {
            &self.bus
        }

        async fn create_bus(&self, _request: CreateBusCommand) -> CommitResult<()> {
            Ok(())
        }

        async fn commit_intention(
            &self,
            request: CommitIntentionCommand,
        ) -> CommitResult<CommitIntentionOutcome> {
            self.requests.borrow_mut().push(request);
            match self.response {
                FakeResponse::Approved => Ok(CommitIntentionOutcome {
                    approved: true,
                    reason: "allowed".to_string(),
                    log_position: 7,
                }),
                FakeResponse::Timeout => Err(CommitError::Timeout(
                    std::io::Error::other("commit timed out").into(),
                )),
            }
        }
    }

    fn request() -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: BusId {
                agent_bus_id: "bus-1".to_string(),
            },
            intention: intention::Intention::StringIntention("do work".to_string()),
        }
    }

    fn observable(
        response: FakeResponse,
    ) -> (
        ObservableCommitSvc<FakeCommitSvc, RealEnvironment>,
        Rc<RefCell<Vec<CommitIntentionCommand>>>,
        Rc<InMemoryMetrics>,
        InMemoryLogger,
    ) {
        let environment = Rc::new(RealEnvironment::new());
        let requests = Rc::new(RefCell::new(Vec::new()));
        let metrics = Rc::new(InMemoryMetrics::new());
        let logger = InMemoryLogger::new();
        let inner = FakeCommitSvc {
            bus: InMemoryAgentBus::new(environment.clone()),
            requests: requests.clone(),
            response,
        };
        let wrapper =
            ObservableCommitSvc::new(inner, metrics.clone(), Rc::new(logger.clone()), environment);
        (wrapper, requests, metrics, logger)
    }

    #[test]
    fn forwards_success_and_records_observability() {
        let (wrapper, requests, metrics, logger) = observable(FakeResponse::Approved);
        let input = request();

        let result = block_on(wrapper.commit_intention(input.clone()))
            .expect("the fake commit should succeed");

        assert_eq!(requests.borrow().as_slice(), &[input]);
        assert_eq!(
            result,
            CommitIntentionOutcome {
                approved: true,
                reason: "allowed".to_string(),
                log_position: 7,
            }
        );
        assert_eq!(
            metrics.counter("commit_intention.commit_service.num_calls"),
            1
        );
        assert_eq!(
            metrics.counter("commit_intention.commit_service.num_errors"),
            0
        );
        assert_eq!(
            metrics
                .histogram("commit_intention.commit_service.latency_us")
                .len(),
            1
        );

        let rows = logger.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].api_call_name, "commit_intention");
        assert_eq!(rows[0].error_code, None);
        assert_eq!(rows[0].error_message, None);
        assert_eq!(
            rows[0].additional_fields,
            vec![
                (
                    "component".to_string(),
                    LogFieldValue::String("commit_service".to_string()),
                ),
                (
                    "resource_id".to_string(),
                    LogFieldValue::String("bus-1".to_string()),
                ),
                ("success".to_string(), LogFieldValue::I64(1)),
                ("approved".to_string(), LogFieldValue::I64(1)),
                (
                    "reason".to_string(),
                    LogFieldValue::String("allowed".to_string()),
                ),
                ("position".to_string(), LogFieldValue::I64(7)),
            ]
        );
        assert!(std::ptr::eq(wrapper.agent_bus(), wrapper.inner.agent_bus()));
    }

    #[test]
    fn preserves_errors_and_records_their_category() {
        let (wrapper, requests, metrics, logger) = observable(FakeResponse::Timeout);
        let input = request();

        let error = block_on(wrapper.commit_intention(input.clone()))
            .expect_err("the fake commit should time out");

        assert!(
            matches!(error, CommitError::Timeout(ref source) if source.to_string() == "commit timed out")
        );
        assert_eq!(requests.borrow().as_slice(), &[input]);
        assert_eq!(
            metrics.counter("commit_intention.commit_service.num_calls"),
            1
        );
        assert_eq!(
            metrics.counter("commit_intention.commit_service.num_errors"),
            1
        );
        assert_eq!(
            metrics
                .histogram("commit_intention.commit_service.latency_us")
                .len(),
            1
        );

        let rows = logger.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].api_call_name, "commit_intention");
        assert_eq!(rows[0].error_code.as_deref(), Some("CommitTimeout"));
        assert_eq!(rows[0].error_message.as_deref(), Some("commit timed out"));
        assert_eq!(
            rows[0].additional_fields,
            vec![
                (
                    "component".to_string(),
                    LogFieldValue::String("commit_service".to_string()),
                ),
                (
                    "resource_id".to_string(),
                    LogFieldValue::String("bus-1".to_string()),
                ),
                ("success".to_string(), LogFieldValue::I64(0)),
            ]
        );
    }
}
