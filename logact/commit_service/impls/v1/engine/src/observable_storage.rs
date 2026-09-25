/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Observability wrapper for the [`Storage`] API.
//!
//! The wrapper records low-cardinality metrics and one structured row per
//! operation. A completed CAS rejection remains `Ok(false)` and is represented
//! as `success=1` with `applied:0` in the response context; backend failures
//! retain their [`StorageError`] category as the row's error code.

use std::time::Duration;

use agentbus_api::AgentBusMetrics;
use agentbus_api::AgentbusLogger;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::LogRow;
use agentbus_api::MonotonicInstant;
use bytes::Bytes;

use crate::Observability;
use crate::Storage;
use crate::StorageError;
use crate::StorageResult;

const NO_TAGS: &[(&str, &str)] = &[];

/// Decorates a [`Storage`] with operation metrics and structured logging.
pub struct ObservableStorage<S, L, E: Environment> {
    inner: S,
    observability: Observability<L, E>,
}

impl<S, L, E: Environment> ObservableStorage<S, L, E> {
    /// Wrap storage with the shared engine observability sinks.
    pub fn new(inner: S, observability: Observability<L, E>) -> Self {
        Self {
            inner,
            observability,
        }
    }

    fn monotonic_time(&self) -> MonotonicInstant {
        self.observability
            .environment
            .with_clock(|clock| clock.monotonic_time())
    }

    fn elapsed(&self, start: MonotonicInstant) -> Duration {
        self.monotonic_time().duration_since(start)
    }
}

fn record_latency(metrics: &dyn AgentBusMetrics, key: &str, elapsed: Duration) {
    metrics.record_histogram(key, elapsed.as_micros() as i64, NO_TAGS);
}

fn detail(key: &str, value: impl std::fmt::Display) -> String {
    format!("{key}:{value}")
}

fn base_row(method: &str, key: &str, elapsed: Duration) -> LogRow {
    LogRow::default()
        .api_call_name(method)
        .latency_ms(elapsed.as_millis() as u64)
        .add_string_field("component", "storage")
        .add_string_field("resource_id", key)
}

fn storage_error_code(error: &StorageError) -> &'static str {
    match error {
        StorageError::TransactionConflict(_) => "TransactionConflict",
        StorageError::Timeout(_) => "Timeout",
        StorageError::BackendUnavailable(_) => "BackendUnavailable",
        StorageError::InternalError(_) => "InternalError",
    }
}

fn storage_error_message(error: &StorageError) -> String {
    let source = match error {
        StorageError::TransactionConflict(source)
        | StorageError::Timeout(source)
        | StorageError::BackendUnavailable(source)
        | StorageError::InternalError(source) => source,
    };
    format!("{error}: {source:#}")
}

fn add_get_result(row: LogRow, result: &StorageResult<Option<(Bytes, i64)>>) -> LogRow {
    match result {
        Ok(Some((value, version))) => row.add_i64_field("success", 1).add_string_vector_field(
            "response_context",
            vec![
                detail("found", 1),
                detail("version", version),
                detail("value_bytes", value.len()),
            ],
        ),
        Ok(None) => row
            .add_i64_field("success", 1)
            .add_string_vector_field("response_context", vec![detail("found", 0)]),
        Err(error) => row
            .add_i64_field("success", 0)
            .error_code(storage_error_code(error))
            .error_message(storage_error_message(error)),
    }
}

fn add_put_result(row: LogRow, result: &StorageResult<bool>) -> LogRow {
    match result {
        Ok(applied) => row.add_i64_field("success", 1).add_string_vector_field(
            "response_context",
            vec![detail("applied", i64::from(*applied))],
        ),
        Err(error) => row
            .add_i64_field("success", 0)
            .error_code(storage_error_code(error))
            .error_message(storage_error_message(error)),
    }
}

#[async_trait::async_trait(?Send)]
impl<S, L, E> Storage for ObservableStorage<S, L, E>
where
    S: Storage,
    L: AgentbusLogger,
    E: Environment,
{
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        self.observability
            .metrics
            .record_counter("get.storage.num_calls", 1, NO_TAGS);
        let start = self.monotonic_time();

        let result = self.inner.get(key).await;

        let elapsed = self.elapsed(start);
        record_latency(
            &*self.observability.metrics,
            "get.storage.latency_us",
            elapsed,
        );
        match &result {
            Ok(Some(_)) => {}
            Ok(None) => {
                self.observability
                    .metrics
                    .record_counter("get.storage.num_misses", 1, NO_TAGS)
            }
            Err(_) => {
                self.observability
                    .metrics
                    .record_counter("get.storage.num_errors", 1, NO_TAGS)
            }
        }

        self.observability
            .logger
            .log_row(add_get_result(base_row("get", key, elapsed), &result));

        result
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        version: i64,
    ) -> StorageResult<bool> {
        let value_bytes = value.len();
        self.observability
            .metrics
            .record_counter("put.storage.num_calls", 1, NO_TAGS);
        self.observability.metrics.record_histogram(
            "put.storage.value_bytes",
            value_bytes as i64,
            NO_TAGS,
        );
        let start = self.monotonic_time();

        let result = self.inner.put(key, value, expected, version).await;

        let elapsed = self.elapsed(start);
        record_latency(
            &*self.observability.metrics,
            "put.storage.latency_us",
            elapsed,
        );
        match &result {
            Ok(true) => {}
            Ok(false) => self.observability.metrics.record_counter(
                "put.storage.num_cas_rejections",
                1,
                NO_TAGS,
            ),
            Err(_) => {
                self.observability
                    .metrics
                    .record_counter("put.storage.num_errors", 1, NO_TAGS)
            }
        }

        let expected = expected.map_or_else(|| "absent".to_string(), |version| version.to_string());
        let request_context = vec![
            detail("version", version),
            detail("value_bytes", value_bytes),
            detail("expected", expected),
        ];
        let row = base_row("put", key, elapsed)
            .add_string_vector_field("request_context", request_context);
        self.observability
            .logger
            .log_row(add_put_result(row, &result));

        result
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use agentbus_api::InMemoryLogger;
    use agentbus_api::InMemoryMetrics;
    use agentbus_api::LogFieldValue;
    use agentbus_api::RealEnvironment;
    use anyhow::anyhow;
    use futures::executor::block_on;

    use super::*;
    use crate::InMemoryStorage;

    fn field(row: &LogRow, key: &str) -> Option<String> {
        row.additional_fields
            .iter()
            .find(|(field_key, _)| field_key == key)
            .map(|(_, value)| value.to_string())
    }

    fn string_vector_field(row: &LogRow, key: &str) -> Option<Vec<String>> {
        row.additional_fields
            .iter()
            .find(|(field_key, _)| field_key == key)
            .and_then(|(_, value)| match value {
                LogFieldValue::StringVector(values) => Some(values.clone()),
                _ => None,
            })
    }

    fn observability() -> (
        Observability<InMemoryLogger, RealEnvironment>,
        Rc<InMemoryMetrics>,
        InMemoryLogger,
    ) {
        let metrics = Rc::new(InMemoryMetrics::new());
        let logger = InMemoryLogger::new();
        (
            Observability {
                metrics: metrics.clone(),
                logger: Rc::new(logger.clone()),
                environment: Rc::new(RealEnvironment::new()),
            },
            metrics,
            logger,
        )
    }

    struct FailingStorage;

    #[async_trait::async_trait(?Send)]
    impl Storage for FailingStorage {
        async fn get(&self, _key: &str) -> StorageResult<Option<(Bytes, i64)>> {
            Err(StorageError::Timeout(anyhow!("get source")))
        }

        async fn put(
            &self,
            _key: &str,
            _value: Bytes,
            _expected: Option<i64>,
            _new_position: i64,
        ) -> StorageResult<bool> {
            Err(StorageError::TransactionConflict(anyhow!("put source")))
        }
    }

    #[test]
    fn storage_error_codes_and_messages_preserve_categories() {
        for (error, code, message) in [
            (
                StorageError::TransactionConflict(anyhow!("conflict source")),
                "TransactionConflict",
                "transaction conflict: conflict source",
            ),
            (
                StorageError::Timeout(anyhow!("timeout source")),
                "Timeout",
                "timeout: timeout source",
            ),
            (
                StorageError::BackendUnavailable(anyhow!("unavailable source")),
                "BackendUnavailable",
                "backend unavailable: unavailable source",
            ),
            (
                StorageError::InternalError(anyhow!("internal source")),
                "InternalError",
                "internal error: internal source",
            ),
        ] {
            assert_eq!(storage_error_code(&error), code);
            assert_eq!(storage_error_message(&error), message);
        }
    }

    #[test]
    fn records_success_miss_and_cas_rejection() {
        let (observability, metrics, logger) = observability();
        let storage = ObservableStorage::new(InMemoryStorage::new(), observability);
        let key = "engine:state:bus-1";

        assert!(
            block_on(storage.get(key))
                .expect("miss should not be an error")
                .is_none(),
            "key should initially be absent"
        );
        assert!(
            block_on(storage.put(key, Bytes::from_static(b"value"), None, 1))
                .expect("initial put should succeed"),
            "initial put should be applied"
        );
        assert!(
            !block_on(storage.put(key, Bytes::from_static(b"other"), None, 2))
                .expect("CAS rejection should not be an error"),
            "second absent-key CAS should be rejected"
        );
        let found_value = block_on(storage.get(key))
            .expect("get should succeed")
            .expect("stored key should be present");
        assert_eq!(found_value, (Bytes::from_static(b"value"), 1));
        assert!(
            block_on(storage.put(key, Bytes::from_static(b"updated"), Some(1), 2))
                .expect("version-based CAS should succeed"),
            "matching expected version should be applied"
        );

        assert_eq!(metrics.counter("get.storage.num_calls"), 2);
        assert_eq!(metrics.counter("get.storage.num_misses"), 1);
        assert_eq!(metrics.counter("get.storage.num_errors"), 0);
        assert_eq!(metrics.histogram("get.storage.latency_us").len(), 2);
        assert_eq!(metrics.counter("put.storage.num_calls"), 3);
        assert_eq!(metrics.counter("put.storage.num_cas_rejections"), 1);
        assert_eq!(metrics.counter("put.storage.num_errors"), 0);
        assert_eq!(metrics.histogram("put.storage.value_bytes"), vec![5, 5, 7]);

        let rows = logger.rows();
        assert_eq!(rows.len(), 5, "one row per storage operation");

        let miss = &rows[0];
        assert_eq!(miss.api_call_name, "get");
        assert_eq!(field(miss, "component"), Some("storage".to_string()));
        assert_eq!(field(miss, "resource_id"), Some(key.to_string()));
        assert_eq!(field(miss, "success"), Some("1".to_string()));
        assert_eq!(
            string_vector_field(miss, "response_context"),
            Some(vec!["found:0".to_string()])
        );

        let applied = &rows[1];
        assert_eq!(applied.api_call_name, "put");
        assert_eq!(field(applied, "success"), Some("1".to_string()));
        assert_eq!(
            string_vector_field(applied, "request_context"),
            Some(vec![
                "version:1".to_string(),
                "value_bytes:5".to_string(),
                "expected:absent".to_string(),
            ])
        );
        assert_eq!(
            string_vector_field(applied, "response_context"),
            Some(vec!["applied:1".to_string()])
        );

        let rejected = &rows[2];
        assert_eq!(field(rejected, "success"), Some("1".to_string()));
        assert_eq!(
            string_vector_field(rejected, "response_context"),
            Some(vec!["applied:0".to_string()])
        );
        assert_eq!(rejected.error_code, None, "CAS rejection is not an error");
        assert_eq!(
            rejected.error_message, None,
            "CAS rejection has no error message"
        );

        let found = &rows[3];
        assert_eq!(
            string_vector_field(found, "response_context"),
            Some(vec![
                "found:1".to_string(),
                "version:1".to_string(),
                "value_bytes:5".to_string(),
            ])
        );

        let conditional = &rows[4];
        assert_eq!(
            string_vector_field(conditional, "request_context"),
            Some(vec![
                "version:2".to_string(),
                "value_bytes:7".to_string(),
                "expected:1".to_string(),
            ])
        );
        assert_eq!(
            string_vector_field(conditional, "response_context"),
            Some(vec!["applied:1".to_string()])
        );
    }

    #[test]
    fn records_typed_storage_errors_without_changing_them() {
        let (observability, metrics, logger) = observability();
        let storage = ObservableStorage::new(FailingStorage, observability);

        let get_error = block_on(storage.get("state")).expect_err("get should fail");
        assert!(
            matches!(get_error, StorageError::Timeout(_)),
            "get error category should pass through, got {get_error:?}"
        );
        let put_error =
            block_on(storage.put("state", Bytes::new(), None, 1)).expect_err("put should fail");
        assert!(
            matches!(put_error, StorageError::TransactionConflict(_)),
            "put error category should pass through, got {put_error:?}"
        );

        assert_eq!(metrics.counter("get.storage.num_errors"), 1);
        assert_eq!(metrics.counter("put.storage.num_errors"), 1);
        assert_eq!(
            metrics.counter("put.storage.num_cas_rejections"),
            0,
            "transaction abort is not a completed CAS rejection"
        );

        let rows = logger.rows();
        assert_eq!(rows.len(), 2, "one row per failed operation");
        assert_eq!(field(&rows[0], "success"), Some("0".to_string()));
        assert_eq!(rows[0].error_code.as_deref(), Some("Timeout"));
        assert!(
            rows[0]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("get source")),
            "get source should be preserved, got {:?}",
            rows[0].error_message
        );
        assert_eq!(field(&rows[1], "success"), Some("0".to_string()));
        assert_eq!(rows[1].error_code.as_deref(), Some("TransactionConflict"));
        assert!(
            rows[1]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("put source")),
            "put source should be preserved, got {:?}",
            rows[1].error_message
        );
    }
}
