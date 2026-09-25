/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Structured row-logging interface for AgentBus.
//!
//! [`AgentbusLogger`] is the sink an observable wrapper writes a row to on every
//! call. Wrappers populate the common [`LogRow`] representation, and real
//! backends can implement the trait; [`NoopLogger`] and [`InMemoryLogger`] are
//! provided here for defaults and tests.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

/// Typed value for an extra structured log column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogFieldValue {
    String(String),
    StringVector(Vec<String>),
    I64(i64),
}

impl fmt::Display for LogFieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => value.fmt(f),
            Self::StringVector(values) => write!(f, "[{}]", values.join(", ")),
            Self::I64(value) => value.fmt(f),
        }
    }
}

/// A finalized structured row. The error fields are optional — they stay
/// `None` on a successful call; `latency_ms` and `api_call_name` are always set.
/// Extra columns land in `additional_fields` in insertion order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogRow {
    pub latency_ms: u64,
    pub api_call_name: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub additional_fields: Vec<(String, LogFieldValue)>,
}

impl LogRow {
    /// Latency the API call / event took to complete, in milliseconds.
    pub fn latency_ms(mut self, value: u64) -> Self {
        self.latency_ms = value;
        self
    }

    /// Name of the API call being logged.
    pub fn api_call_name(mut self, value: impl Into<String>) -> Self {
        self.api_call_name = value.into();
        self
    }

    /// Code classifying a failure (omitted on success).
    pub fn error_code(mut self, value: impl Into<String>) -> Self {
        self.error_code = Some(value.into());
        self
    }

    /// Human-readable error message (omitted on success).
    pub fn error_message(mut self, value: impl Into<String>) -> Self {
        self.error_message = Some(value.into());
        self
    }

    /// Add an arbitrary string column.
    pub fn add_string_field(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.additional_fields
            .push((field.into(), LogFieldValue::String(value.into())));
        self
    }

    /// Add an arbitrary string-vector column.
    pub fn add_string_vector_field(mut self, field: impl Into<String>, value: Vec<String>) -> Self {
        self.additional_fields
            .push((field.into(), LogFieldValue::StringVector(value)));
        self
    }

    /// Add an arbitrary integer column.
    pub fn add_i64_field(mut self, field: impl Into<String>, value: i64) -> Self {
        self.additional_fields
            .push((field.into(), LogFieldValue::I64(value)));
        self
    }
}

/// A sink for structured rows.
pub trait AgentbusLogger: 'static {
    /// Publish a finalized row.
    fn log_row(&self, row: LogRow);
}

/// Lets a single logger be shared (by `Rc`) across several observable wrappers
/// — e.g. one engine logger handed to the decider and every voter — by
/// delegating through the shared reference.
impl<L: AgentbusLogger + ?Sized> AgentbusLogger for Rc<L> {
    fn log_row(&self, row: LogRow) {
        (**self).log_row(row);
    }
}

/// Records every published [`LogRow`] in memory for test assertions. Cloning
/// shares the same backing buffer, so an observable wrapper can take a clone by
/// value while the test keeps a handle to read the rows back.
#[derive(Clone, Default)]
pub struct InMemoryLogger {
    rows: Rc<RefCell<Vec<LogRow>>>,
}

impl InMemoryLogger {
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of the rows published so far.
    pub fn rows(&self) -> Vec<LogRow> {
        self.rows.borrow().clone()
    }
}

impl AgentbusLogger for InMemoryLogger {
    fn log_row(&self, row: LogRow) {
        self.rows.borrow_mut().push(row);
    }
}

/// No-op logger that discards all rows.
#[derive(Clone)]
pub struct NoopLogger;

impl AgentbusLogger for NoopLogger {
    fn log_row(&self, _row: LogRow) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial subject under observation: it just echoes its input.
    struct Echo;

    impl Echo {
        fn call(&self, input: &str) -> String {
            format!("echo:{input}")
        }
    }

    /// An observable wrapper around [`Echo`] that logs one row per call,
    /// mirroring how the real observable wrappers in this codebase emit a row
    /// around the inner call. Generic over the logger so tests inject a fake.
    struct ObservableEcho<L: AgentbusLogger> {
        inner: Echo,
        logger: L,
    }

    impl<L: AgentbusLogger> ObservableEcho<L> {
        fn call(&self, input: &str) -> String {
            let result = self.inner.call(input);
            let row = LogRow::default()
                .api_call_name("call")
                .latency_ms(42)
                .add_string_field("input", input)
                .add_i64_field("success", 1);
            self.logger.log_row(row);
            result
        }
    }

    #[test]
    fn builder_populates_all_fields() {
        let row = LogRow::default()
            .api_call_name("apply")
            .latency_ms(7)
            .error_code("StalePosition")
            .error_message("stale apply for position 5: last applied position was 11")
            .add_string_field("bus_id", "bus-1")
            .add_string_vector_field("request", vec!["payload_type:intention".to_string()])
            .add_i64_field("success", 1);

        assert_eq!(
            row,
            LogRow {
                latency_ms: 7,
                api_call_name: "apply".to_string(),
                error_code: Some("StalePosition".to_string()),
                error_message: Some(
                    "stale apply for position 5: last applied position was 11".to_string()
                ),
                additional_fields: vec![
                    (
                        "bus_id".to_string(),
                        LogFieldValue::String("bus-1".to_string())
                    ),
                    (
                        "request".to_string(),
                        LogFieldValue::StringVector(vec!["payload_type:intention".to_string()])
                    ),
                    ("success".to_string(), LogFieldValue::I64(1)),
                ],
            },
            "every chained setter should land in its column"
        );
    }

    #[test]
    fn observable_wrapper_logs_correct_row() {
        let logger = InMemoryLogger::new();
        let observable = ObservableEcho {
            inner: Echo,
            logger: logger.clone(),
        };

        let out = observable.call("hi");
        assert_eq!(out, "echo:hi", "wrapper must pass the inner result through");

        let rows = logger.rows();
        assert_eq!(rows.len(), 1, "exactly one row should be logged per call");
        assert_eq!(
            rows[0],
            LogRow {
                latency_ms: 42,
                api_call_name: "call".to_string(),
                error_code: None,
                error_message: None,
                additional_fields: vec![
                    ("input".to_string(), LogFieldValue::String("hi".to_string())),
                    ("success".to_string(), LogFieldValue::I64(1)),
                ],
            },
            "the logged row should carry exactly the fields the wrapper set"
        );
    }
}
