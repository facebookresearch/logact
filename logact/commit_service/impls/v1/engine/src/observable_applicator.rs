/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Observability wrapper for the [`Applicator`] API.
//!
//! `ObservableApplicator` wraps any inner [`Applicator`] and, on every `apply`
//! call, emits:
//!
//! - **Metrics** via [`AgentBusMetrics`]: a throughput counter, a latency
//!   histogram, and (on failure) an error counter. All are tagged by applicator
//!   kind, and the error counter is also tagged by stable error code.
//! - **A structured row** via [`AgentbusLogger`]: the applicator `component`,
//!   resource ID, payload type, log position, latency, `success`, and the
//!   `ApplyError` kind + message on failure.
//!
//! It is a pure pass-through: the inner `Result` is returned unchanged, so the
//! wrapper can transparently replace any applicator (e.g. a voter adapter or the
//! decider). Logging and metrics backends are injected, mirroring
//! `ObservableAgentBus`; in-memory implementations are available for tests.

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Payload;
use agentbus_api::AgentBusError;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::LogRow;
use agentbus_api::logger::AgentbusLogger;
use agentbus_api::metrics::AgentBusMetrics;
use agentbus_api::parse_entry;

use crate::Applicator;
use crate::ApplyError;
use crate::ConcurrencyError;
use crate::DeciderFactory;
use crate::DeciderFactoryImpl;
use crate::InMemoryStorage;
use crate::StateMachineSpec;
use crate::Storage;
use crate::StorageError;

/// The observability backends shared by engine decorators for metrics,
/// structured logging, and latency timing.
pub struct Observability<L, E> {
    pub metrics: Rc<dyn AgentBusMetrics>,
    pub logger: Rc<L>,
    pub environment: Rc<E>,
}

impl<L, E> Clone for Observability<L, E> {
    fn clone(&self) -> Self {
        Self {
            metrics: self.metrics.clone(),
            logger: self.logger.clone(),
            environment: self.environment.clone(),
        }
    }
}

/// Wraps an [`Applicator`] with metrics and structured-row logging.
pub struct ObservableApplicator<L, E: Environment> {
    inner: Rc<dyn Applicator>,
    applicator_kind: &'static str,
    observability: Observability<L, E>,
}

impl<L, E: Environment> ObservableApplicator<L, E> {
    pub fn new(
        inner: Rc<dyn Applicator>,
        applicator_kind: &'static str,
        observability: Observability<L, E>,
    ) -> Self {
        Self {
            inner,
            applicator_kind,
            observability,
        }
    }
}

/// Stable code for each [`ApplyError`] kind, logged as `error_code`.
fn error_code(err: &ApplyError) -> &'static str {
    match err {
        ApplyError::StalePosition { .. } => "StalePosition",
        ApplyError::MissingHeader => "MissingHeader",
        ApplyError::InvalidEngineState { .. } => "InvalidEngineState",
        ApplyError::MalformedPolicy { .. } => "MalformedPolicy",
        ApplyError::DeprecatedOperation { .. } => "DeprecatedOperation",
        ApplyError::Storage(error) => match error {
            StorageError::TransactionConflict(_) => "StorageTransactionConflict",
            StorageError::Timeout(_) => "StorageTimeout",
            StorageError::BackendUnavailable(_) => "StorageBackendUnavailable",
            StorageError::InternalError(_) => "StorageInternalError",
        },
        ApplyError::Concurrency(error) => match error {
            ConcurrencyError::Engine { .. } => "ConcurrencyEngine",
            ConcurrencyError::Voter { .. } => "ConcurrencyVoter",
            ConcurrencyError::Decider { .. } => "ConcurrencyDecider",
        },
        ApplyError::Bus(error) => match error {
            AgentBusError::InvalidArgument(_) => "BusInvalidArgument",
            AgentBusError::Timeout(_) => "BusTimeout",
            AgentBusError::Unavailable(_) => "BusUnavailable",
            AgentBusError::Internal(_) => "BusInternal",
        },
        ApplyError::Backend(_) => "Backend",
    }
}

#[async_trait::async_trait(?Send)]
impl<L, E> Applicator for ObservableApplicator<L, E>
where
    L: AgentbusLogger,
    E: Environment,
{
    async fn apply(&self, bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
        let tags = [("applicator_kind", self.applicator_kind)];
        self.observability
            .metrics
            .record_counter("apply.applicator.num_calls", 1, &tags);

        // Capture entry metadata up front — the entry is only borrowed for the
        // call, and the header is absent for malformed entries.
        let payload_type = parse_entry(entry).entry_type;
        let log_position = entry.header.as_ref().map(|h| h.log_position);

        let start = self
            .observability
            .environment
            .with_clock(|c| c.monotonic_time());
        let result = self.inner.apply(bus_id, entry).await;
        let elapsed = self
            .observability
            .environment
            .with_clock(|c| c.monotonic_time())
            - start;

        self.observability.metrics.record_histogram(
            "apply.applicator.latency_us",
            elapsed.as_micros() as i64,
            &tags,
        );
        if let Err(err) = &result {
            let error_tags = [
                ("applicator_kind", self.applicator_kind),
                ("error_code", error_code(err)),
            ];
            self.observability.metrics.record_counter(
                "apply.applicator.num_errors",
                1,
                &error_tags,
            );
        }

        let mut row = LogRow::default()
            .api_call_name("apply")
            .latency_ms(elapsed.as_millis() as u64)
            .add_string_field("component", self.applicator_kind)
            .add_string_field("resource_id", bus_id)
            .add_string_field("payload_type", payload_type);
        if let Some(position) = log_position {
            row = row.add_i64_field("log_position", position);
        }
        if let Err(ApplyError::MalformedPolicy { kind, .. }) = &result {
            row = row.add_string_field("malformed_policy_kind", kind.as_ref());
        }
        if let Err(ApplyError::DeprecatedOperation { operation }) = &result {
            row = row.add_string_field("deprecated_operation_kind", operation.as_ref());
        }
        let row = match &result {
            Ok(_) => row.add_i64_field("success", 1),
            Err(err) => row
                .add_i64_field("success", 0)
                .error_code(error_code(err))
                .error_message(err.to_string()),
        };
        self.observability.logger.log_row(row);

        result
    }
}

/// A [`DeciderFactory`] that wraps the default decider in an
/// [`ObservableApplicator`] (kind `"decider"`).
pub struct ObservedDeciderFactory<L, E, S = InMemoryStorage> {
    inner: DeciderFactoryImpl<S>,
    observability: Observability<L, E>,
}

impl<L, E, S> ObservedDeciderFactory<L, E, S> {
    pub fn new(storage: Rc<S>, observability: Observability<L, E>) -> Self {
        Self {
            inner: DeciderFactoryImpl::new(storage),
            observability,
        }
    }
}

impl<L, E, S> Clone for ObservedDeciderFactory<L, E, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            observability: self.observability.clone(),
        }
    }
}

impl<S, L, E> DeciderFactory for ObservedDeciderFactory<L, E, S>
where
    S: Storage + 'static,
    L: AgentbusLogger + 'static,
    E: Environment + 'static,
{
    fn create_decider(
        &self,
        spec: StateMachineSpec<Option<i64>, DeciderPolicy>,
    ) -> Rc<dyn Applicator> {
        let decider = self.inner.create_decider(spec);
        Rc::new(ObservableApplicator::new(
            decider,
            "decider",
            self.observability.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::InMemoryLogger;
    use agentbus_api::InMemoryMetrics;
    use agentbus_api::LogRow;
    use agentbus_api::RealEnvironment;
    use futures::executor::block_on;

    use super::*;
    use crate::ConcurrencyError;
    use crate::MalformedPolicyBatchKind;

    struct RecordingMetrics {
        inner: InMemoryMetrics,
        records: RefCell<Vec<(String, Vec<(String, String)>)>>,
    }

    impl RecordingMetrics {
        fn new() -> Self {
            Self {
                inner: InMemoryMetrics::new(),
                records: RefCell::new(Vec::new()),
            }
        }

        fn counter(&self, key: &str) -> i64 {
            self.inner.counter(key)
        }

        fn histogram(&self, key: &str) -> Vec<i64> {
            self.inner.histogram(key)
        }

        fn tags_for(&self, key: &str) -> Vec<Vec<(String, String)>> {
            self.records
                .borrow()
                .iter()
                .filter(|(recorded_key, _)| recorded_key == key)
                .map(|(_, tags)| tags.clone())
                .collect()
        }
    }

    impl AgentBusMetrics for RecordingMetrics {
        fn record_counter(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
            self.records.borrow_mut().push((
                key.to_string(),
                tags.iter()
                    .map(|(tag_key, tag_value)| (tag_key.to_string(), tag_value.to_string()))
                    .collect(),
            ));
            self.inner.record_counter(key, value, tags);
        }

        fn record_histogram(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
            self.records.borrow_mut().push((
                key.to_string(),
                tags.iter()
                    .map(|(tag_key, tag_value)| (tag_key.to_string(), tag_value.to_string()))
                    .collect(),
            ));
            self.inner.record_histogram(key, value, tags);
        }
    }

    fn fixed_applicator_kind_tag() -> Vec<(String, String)> {
        vec![(
            "applicator_kind".to_string(),
            "fixed-applicator".to_string(),
        )]
    }

    fn fixed_applicator_error_tags(code: &str) -> Vec<(String, String)> {
        vec![
            (
                "applicator_kind".to_string(),
                "fixed-applicator".to_string(),
            ),
            ("error_code".to_string(), code.to_string()),
        ]
    }

    /// An applicator that returns one configured result, for exercising the wrapper.
    struct FixedApplicator(RefCell<Option<Result<Option<Payload>, ApplyError>>>);

    impl FixedApplicator {
        fn new(result: Result<Option<Payload>, ApplyError>) -> Self {
            Self(RefCell::new(Some(result)))
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Applicator for FixedApplicator {
        async fn apply(
            &self,
            _bus_id: &str,
            _entry: &BusEntry,
        ) -> Result<Option<Payload>, ApplyError> {
            self.0
                .borrow_mut()
                .take()
                .expect("test should call the applicator once")
        }
    }

    fn intention_entry(position: i64) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention("do X".to_string())),
                    ..Default::default()
                })),
            }),
        }
    }

    fn field(row: &LogRow, key: &str) -> Option<String> {
        row.additional_fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.to_string())
    }

    fn observe(
        inner: FixedApplicator,
    ) -> (
        ObservableApplicator<InMemoryLogger, RealEnvironment>,
        Rc<RecordingMetrics>,
        InMemoryLogger,
    ) {
        let metrics = Rc::new(RecordingMetrics::new());
        let logger = InMemoryLogger::new();
        let wrapper = ObservableApplicator::new(
            Rc::new(inner),
            "fixed-applicator",
            Observability {
                metrics: metrics.clone(),
                logger: Rc::new(logger.clone()),
                environment: Rc::new(RealEnvironment::new()),
            },
        );
        (wrapper, metrics, logger)
    }

    #[test]
    fn success_records_metrics_and_row() {
        let payload = Payload { payload: None };
        let (wrapper, metrics, logger) = observe(FixedApplicator::new(Ok(Some(payload.clone()))));

        let out = block_on(wrapper.apply("bus-1", &intention_entry(7))).unwrap();
        assert_eq!(out, Some(payload), "the inner result must pass through");

        assert_eq!(metrics.counter("apply.applicator.num_calls"), 1);
        assert_eq!(
            metrics.counter("apply.applicator.num_errors"),
            0,
            "no error on success"
        );
        assert_eq!(
            metrics.histogram("apply.applicator.latency_us").len(),
            1,
            "one latency sample recorded"
        );
        assert_eq!(
            metrics.tags_for("apply.applicator.num_calls"),
            vec![fixed_applicator_kind_tag()],
            "throughput metric is tagged only by applicator kind"
        );
        assert_eq!(
            metrics.tags_for("apply.applicator.latency_us"),
            vec![fixed_applicator_kind_tag()],
            "latency metric is tagged only by applicator kind"
        );
        let rows = logger.rows();
        assert_eq!(rows.len(), 1, "one row per apply");
        let row = &rows[0];
        assert_eq!(row.api_call_name, "apply");
        assert_eq!(
            field(row, "component"),
            Some("fixed-applicator".to_string()),
            "component"
        );
        assert_eq!(
            field(row, "resource_id"),
            Some("bus-1".to_string()),
            "bus resource"
        );
        assert_eq!(
            field(row, "payload_type"),
            Some("intention".to_string()),
            "payload type"
        );
        assert_eq!(
            field(row, "log_position"),
            Some("7".to_string()),
            "log position"
        );
        assert_eq!(field(row, "request_context"), None);
        assert_eq!(field(row, "response_context"), None);
        assert_eq!(field(row, "success"), Some("1".to_string()));
        assert_eq!(row.error_code, None, "no error code on success");
        assert_eq!(row.error_message, None, "no error message on success");
    }

    #[test]
    fn error_records_error_metric_and_error_fields() {
        let (wrapper, metrics, logger) =
            observe(FixedApplicator::new(Err(ApplyError::StalePosition {
                requested: 5,
                last: 11,
            })));

        let err = block_on(wrapper.apply("bus-1", &intention_entry(5)))
            .expect_err("the inner error must pass through");
        assert!(
            matches!(
                err,
                ApplyError::StalePosition {
                    requested: 5,
                    last: 11
                }
            ),
            "the original error is preserved, got {err:?}"
        );

        assert_eq!(metrics.counter("apply.applicator.num_calls"), 1);
        assert_eq!(
            metrics.counter("apply.applicator.num_errors"),
            1,
            "one error counted"
        );
        assert_eq!(metrics.histogram("apply.applicator.latency_us").len(), 1);
        assert_eq!(
            metrics.tags_for("apply.applicator.num_errors"),
            vec![fixed_applicator_error_tags("StalePosition")],
            "error metric is tagged by applicator kind and error code"
        );
        let rows = logger.rows();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(field(row, "success"), Some("0".to_string()));
        assert_eq!(field(row, "request_context"), None);
        assert_eq!(field(row, "response_context"), None);
        assert_eq!(
            row.error_code.as_deref(),
            Some("StalePosition"),
            "ApplyError variant name"
        );
        assert!(
            row.error_message
                .as_deref()
                .is_some_and(|m| m.contains("stale apply for position 5")),
            "error message carries the error string, got {:?}",
            row.error_message
        );
    }

    #[test]
    fn malformed_policy_has_a_distinct_error_code() {
        let (wrapper, metrics, logger) =
            observe(FixedApplicator::new(Err(ApplyError::MalformedPolicy {
                kind: MalformedPolicyBatchKind::PolicyBatch,
                message: "invalid policy".to_string(),
            })));

        block_on(wrapper.apply("bus-1", &intention_entry(5)))
            .expect_err("the malformed entry must pass through");

        let rows = logger.rows();
        assert_eq!(rows.len(), 1, "one row per apply");
        assert_eq!(
            metrics.tags_for("apply.applicator.num_errors"),
            vec![fixed_applicator_error_tags("MalformedPolicy")],
            "malformed policy errors have stable metric tags"
        );
        assert_eq!(
            field(&rows[0], "resource_id"),
            Some("bus-1".to_string()),
            "bus resource"
        );
        assert_eq!(
            field(&rows[0], "log_position"),
            Some("5".to_string()),
            "log position"
        );
        assert_eq!(
            field(&rows[0], "malformed_policy_kind"),
            Some("PolicyBatch".to_string()),
            "malformed payload category"
        );
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("MalformedPolicy"),
            "malformed playback should be independently observable"
        );
        assert!(
            rows[0]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("invalid policy")),
            "diagnostic message should be preserved, got {:?}",
            rows[0].error_message
        );
    }

    #[test]
    fn storage_errors_keep_specific_error_codes() {
        for (error, expected) in [
            (
                StorageError::TransactionConflict(anyhow::anyhow!("conflict")),
                "StorageTransactionConflict",
            ),
            (
                StorageError::Timeout(anyhow::anyhow!("timeout")),
                "StorageTimeout",
            ),
            (
                StorageError::BackendUnavailable(anyhow::anyhow!("unavailable")),
                "StorageBackendUnavailable",
            ),
            (
                StorageError::InternalError(anyhow::anyhow!("internal")),
                "StorageInternalError",
            ),
        ] {
            assert_eq!(error_code(&ApplyError::Storage(error)), expected);
        }
        for (error, expected) in [
            (
                ConcurrencyError::Voter {
                    bus_id: "bus".to_string(),
                    position: 7,
                    source: None,
                },
                "ConcurrencyVoter",
            ),
            (
                ConcurrencyError::Decider {
                    bus_id: "bus".to_string(),
                    position: 7,
                    source: Some(StorageError::TransactionConflict(anyhow::anyhow!(
                        "conflict"
                    ))),
                },
                "ConcurrencyDecider",
            ),
            (
                ConcurrencyError::Engine {
                    bus_id: "bus".to_string(),
                    position: 7,
                    source: None,
                },
                "ConcurrencyEngine",
            ),
        ] {
            assert_eq!(error_code(&ApplyError::Concurrency(error)), expected);
        }
    }

    #[test]
    fn bus_errors_keep_specific_error_codes() {
        for (error, expected) in [
            (
                AgentBusError::InvalidArgument(anyhow::anyhow!("invalid argument")),
                "BusInvalidArgument",
            ),
            (
                AgentBusError::Timeout(anyhow::anyhow!("timeout")),
                "BusTimeout",
            ),
            (
                AgentBusError::Unavailable(anyhow::anyhow!("unavailable")),
                "BusUnavailable",
            ),
            (
                AgentBusError::Internal(anyhow::anyhow!("internal")),
                "BusInternal",
            ),
        ] {
            assert_eq!(error_code(&ApplyError::Bus(error)), expected);
        }
    }
}
