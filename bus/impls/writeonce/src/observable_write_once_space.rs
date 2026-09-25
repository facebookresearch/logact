/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ObservableWriteOnceSpace: metrics instrumentation for WriteOnceSpace backends.

use std::rc::Rc;
use std::time::Duration;

use agentbus_api::AgentBusMetrics;
use agentbus_api::AgentbusLogger;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::LogRow;
use agentbus_api::MonotonicInstant;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use bytes::Bytes;

use crate::write_once_error_message;

const NO_TAGS: &[(&str, &str)] = &[];
const COMPONENT: &str = "write_once_space";

/// Decorates a [`WriteOnceSpace`] with low-cardinality operation metrics.
pub struct ObservableWriteOnceSpace<W, E: Environment> {
    inner: W,
    metrics: Rc<dyn AgentBusMetrics>,
    logger: Rc<dyn AgentbusLogger>,
    environment: Rc<E>,
}

impl<W: Clone, E: Environment> Clone for ObservableWriteOnceSpace<W, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
            logger: self.logger.clone(),
            environment: self.environment.clone(),
        }
    }
}

impl<W, E: Environment> ObservableWriteOnceSpace<W, E> {
    pub fn new(
        inner: W,
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
}

fn record_latency(metrics: &dyn AgentBusMetrics, key: &str, elapsed: Duration) {
    metrics.record_histogram(key, elapsed.as_micros() as i64, NO_TAGS);
}

fn base_row(row: LogRow, method: &str, space_id: &str, elapsed: Duration) -> LogRow {
    row.api_call_name(method)
        .latency_ms(elapsed.as_millis() as u64)
        .add_string_field("component", COMPONENT)
        .add_string_field("resource_id", space_id)
}

fn tail_error_code(err: &TailError) -> &'static str {
    match err {
        TailError::BackendUnavailable(_) => "BackendUnavailable",
    }
}

fn write_once_error_code(err: &WriteOnceError) -> &'static str {
    match err {
        WriteOnceError::AddressAlreadyExists(_) => "AddressAlreadyExists",
        WriteOnceError::TransactionConflict(_) => "TransactionConflict",
        WriteOnceError::Timeout(_) => "Timeout",
        WriteOnceError::BackendUnavailable(_) => "BackendUnavailable",
        WriteOnceError::InternalError(_) => "InternalError",
    }
}

fn add_tail_result(row: LogRow, result: &TailResult<u64>) -> LogRow {
    match result {
        Ok(tail_position) => row
            .add_i64_field("success", 1)
            .add_i64_field("tail_position", *tail_position as i64),
        Err(err) => row
            .add_i64_field("success", 0)
            .error_code(tail_error_code(err))
            .error_message(err.to_string()),
    }
}

fn add_write_result(row: LogRow, result: &WriteOnceResult<()>) -> LogRow {
    match result {
        Ok(()) => row.add_i64_field("success", 1),
        Err(err) => row
            .add_i64_field("success", 0)
            .error_code(write_once_error_code(err))
            .error_message(write_once_error_message(err)),
    }
}

impl<W: TailableSpace, E: Environment> TailableSpace for ObservableWriteOnceSpace<W, E> {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let logger = self.logger.clone();
        self.metrics
            .record_counter("tail.write_once_space.num_calls", 1, NO_TAGS);
        let start = self.monotonic_time();

        let result = self.inner.tail(space_id, window_size).await;

        let elapsed = self.elapsed(start);
        record_latency(&*self.metrics, "tail.write_once_space.latency_us", elapsed);
        if result.is_err() {
            self.metrics
                .record_counter("tail.write_once_space.num_errors", 1, NO_TAGS);
        }

        let row = LogRow::default().add_i64_field("window_size", window_size as i64);
        let row = add_tail_result(base_row(row, "tail", space_id, elapsed), &result);
        logger.log_row(row);

        result
    }
}

impl<W: WriteOnceSpace, E: Environment> WriteOnceSpace for ObservableWriteOnceSpace<W, E> {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let logger = self.logger.clone();
        let value_bytes = value.len();
        self.metrics
            .record_counter("write.write_once_space.num_calls", 1, NO_TAGS);
        self.metrics.record_histogram(
            "write.write_once_space.value_bytes",
            value_bytes as i64,
            NO_TAGS,
        );
        let start = self.monotonic_time();

        let result = self.inner.write(space_id, address, value).await;

        let elapsed = self.elapsed(start);
        record_latency(&*self.metrics, "write.write_once_space.latency_us", elapsed);
        match &result {
            Ok(()) => {}
            Err(WriteOnceError::AddressAlreadyExists(_)) => {
                self.metrics
                    .record_counter("write.write_once_space.num_conflicts", 1, NO_TAGS);
            }
            Err(
                WriteOnceError::TransactionConflict(_)
                | WriteOnceError::Timeout(_)
                | WriteOnceError::BackendUnavailable(_)
                | WriteOnceError::InternalError(_),
            ) => {
                self.metrics
                    .record_counter("write.write_once_space.num_errors", 1, NO_TAGS);
            }
        }

        let row = LogRow::default()
            .add_i64_field("position", address as i64)
            .add_i64_field("value_bytes", value_bytes as i64);
        let row = add_write_result(base_row(row, "write", space_id, elapsed), &result);
        logger.log_row(row);

        result
    }

    async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        let logger = self.logger.clone();
        self.metrics
            .record_counter("read.write_once_space.num_calls", 1, NO_TAGS);
        let start = self.monotonic_time();

        let result = self.inner.read(space_id, address).await;

        let elapsed = self.elapsed(start);
        record_latency(&*self.metrics, "read.write_once_space.latency_us", elapsed);
        if result.is_none() {
            self.metrics
                .record_counter("read.write_once_space.num_misses", 1, NO_TAGS);
        }

        let mut row = base_row(
            LogRow::default()
                .add_i64_field("position", address as i64)
                .add_i64_field("success", 1)
                .add_i64_field("hit", if result.is_some() { 1 } else { 0 }),
            "read",
            space_id,
            elapsed,
        );
        if let Some(value) = &result {
            row = row.add_i64_field("value_bytes", value.len() as i64);
        }
        logger.log_row(row);

        result
    }
}

#[cfg(test)]
mod tests {
    use agentbus_api::InMemoryLogger;
    use agentbus_api::InMemoryMetrics;
    use agentbus_api::LogRow;
    use agentbus_api::RealEnvironment;
    use anyhow::anyhow;

    use super::*;
    use crate::InMemoryWriteOnceSpace;

    fn field(row: &LogRow, key: &str) -> Option<String> {
        row.additional_fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.to_string())
    }

    #[test]
    fn write_once_error_codes_preserve_categories() {
        for (error, code, message) in [
            (
                WriteOnceError::AddressAlreadyExists(1),
                "AddressAlreadyExists",
                "address already exists: 1",
            ),
            (
                WriteOnceError::TransactionConflict(anyhow!("conflict source")),
                "TransactionConflict",
                "transaction conflict: conflict source",
            ),
            (
                WriteOnceError::Timeout(anyhow!("timeout source")),
                "Timeout",
                "timeout: timeout source",
            ),
            (
                WriteOnceError::BackendUnavailable(anyhow!("unavailable")),
                "BackendUnavailable",
                "backend unavailable: unavailable",
            ),
            (
                WriteOnceError::InternalError(anyhow!("internal")),
                "InternalError",
                "internal error: internal",
            ),
        ] {
            assert_eq!(write_once_error_code(&error), code);
            assert_eq!(write_once_error_message(&error), message);
        }
    }

    #[test]
    fn test_observable_write_once_space_records_metrics() {
        futures::executor::block_on(async {
            let metrics = Rc::new(InMemoryMetrics::new());
            let logger = Rc::new(InMemoryLogger::new());
            let environment = Rc::new(RealEnvironment::new());
            let mut space = ObservableWriteOnceSpace::new(
                InMemoryWriteOnceSpace::new(),
                metrics.clone(),
                logger.clone(),
                environment,
            );

            space
                .write("space", 0, Bytes::from_static(b"value"))
                .await
                .expect("initial write should succeed");
            assert_eq!(metrics.counter("write.write_once_space.num_calls"), 1);
            assert_eq!(
                metrics.histogram("write.write_once_space.latency_us").len(),
                1
            );
            assert_eq!(
                metrics.histogram("write.write_once_space.value_bytes"),
                vec![5]
            );

            let conflict = space
                .write("space", 0, Bytes::from_static(b"other"))
                .await
                .expect_err("duplicate write should conflict");
            assert!(matches!(conflict, WriteOnceError::AddressAlreadyExists(0)));
            assert_eq!(metrics.counter("write.write_once_space.num_conflicts"), 1);

            assert_eq!(
                space.read("space", 0).await,
                Some(Bytes::from_static(b"value"))
            );
            assert_eq!(space.read("space", 1).await, None);
            assert_eq!(metrics.counter("read.write_once_space.num_calls"), 2);
            assert_eq!(metrics.counter("read.write_once_space.num_misses"), 1);

            assert_eq!(space.tail("space", 10).await.unwrap(), 1);
            assert_eq!(metrics.counter("tail.write_once_space.num_calls"), 1);
            assert_eq!(
                metrics.histogram("tail.write_once_space.latency_us").len(),
                1
            );

            let rows = logger.rows();
            assert_eq!(rows.len(), 5, "one row per write-once operation");

            let write = &rows[0];
            assert_eq!(write.api_call_name, "write");
            assert_eq!(
                field(write, "component"),
                Some("write_once_space".to_string())
            );
            assert_eq!(field(write, "resource_id"), Some("space".to_string()));
            assert_eq!(field(write, "position"), Some("0".to_string()));
            assert_eq!(field(write, "value_bytes"), Some("5".to_string()));
            assert_eq!(field(write, "success"), Some("1".to_string()));

            let conflict = &rows[1];
            assert_eq!(conflict.api_call_name, "write");
            assert_eq!(field(conflict, "resource_id"), Some("space".to_string()));
            assert_eq!(field(conflict, "position"), Some("0".to_string()));
            assert_eq!(field(conflict, "success"), Some("0".to_string()));
            assert_eq!(conflict.error_code.as_deref(), Some("AddressAlreadyExists"));

            let read_hit = &rows[2];
            assert_eq!(read_hit.api_call_name, "read");
            assert_eq!(field(read_hit, "position"), Some("0".to_string()));
            assert_eq!(field(read_hit, "success"), Some("1".to_string()));
            assert_eq!(field(read_hit, "hit"), Some("1".to_string()));
            assert_eq!(field(read_hit, "value_bytes"), Some("5".to_string()));

            let read_miss = &rows[3];
            assert_eq!(read_miss.api_call_name, "read");
            assert_eq!(field(read_miss, "position"), Some("1".to_string()));
            assert_eq!(field(read_miss, "success"), Some("1".to_string()));
            assert_eq!(field(read_miss, "hit"), Some("0".to_string()));
            assert_eq!(field(read_miss, "value_bytes"), None);

            let tail = &rows[4];
            assert_eq!(tail.api_call_name, "tail");
            assert_eq!(field(tail, "resource_id"), Some("space".to_string()));
            assert_eq!(field(tail, "window_size"), Some("10".to_string()));
            assert_eq!(field(tail, "success"), Some("1".to_string()));
            assert_eq!(field(tail, "tail_position"), Some("1".to_string()));
        });
    }
}
