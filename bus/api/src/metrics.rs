/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Common metrics interface for AgentBus.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

pub trait AgentBusMetrics: 'static {
    /// Increment a counter by the given amount.
    fn record_counter(&self, key: &str, value: i64, tags: &[(&str, &str)]);

    /// Record a value in a histogram/distribution.
    fn record_histogram(&self, key: &str, value: i64, tags: &[(&str, &str)]);
}

/// Metrics backend that prefixes metric keys before forwarding to another backend.
pub struct PrefixedMetrics {
    prefix: String,
    inner: Rc<dyn AgentBusMetrics>,
}

impl PrefixedMetrics {
    /// Create a metrics backend that prepends `prefix` to every metric key.
    pub fn new(prefix: impl Into<String>, inner: Rc<dyn AgentBusMetrics>) -> Self {
        Self {
            prefix: prefix.into(),
            inner,
        }
    }

    fn key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            return key.to_string();
        }

        format!("{}.{}", self.prefix, key)
    }
}

impl AgentBusMetrics for PrefixedMetrics {
    fn record_counter(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
        self.inner.record_counter(&self.key(key), value, tags);
    }

    fn record_histogram(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
        self.inner.record_histogram(&self.key(key), value, tags);
    }
}

/// In-memory implementation that records all metrics for testing.
pub struct InMemoryMetrics {
    counters: RefCell<HashMap<String, i64>>,
    histograms: RefCell<HashMap<String, Vec<i64>>>,
}

impl InMemoryMetrics {
    pub fn new() -> Self {
        Self {
            counters: RefCell::new(HashMap::new()),
            histograms: RefCell::new(HashMap::new()),
        }
    }

    pub fn counter(&self, key: &str) -> i64 {
        *self.counters.borrow().get(key).unwrap_or(&0)
    }

    pub fn histogram(&self, key: &str) -> Vec<i64> {
        self.histograms
            .borrow()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }
}

impl AgentBusMetrics for InMemoryMetrics {
    fn record_counter(&self, key: &str, value: i64, _tags: &[(&str, &str)]) {
        *self
            .counters
            .borrow_mut()
            .entry(key.to_string())
            .or_insert(0) += value;
    }

    fn record_histogram(&self, key: &str, value: i64, _tags: &[(&str, &str)]) {
        self.histograms
            .borrow_mut()
            .entry(key.to_string())
            .or_default()
            .push(value);
    }
}

/// No-op implementation that discards all metrics.
pub struct NoopMetrics;

impl AgentBusMetrics for NoopMetrics {
    fn record_counter(&self, _key: &str, _value: i64, _tags: &[(&str, &str)]) {}
    fn record_histogram(&self, _key: &str, _value: i64, _tags: &[(&str, &str)]) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Record {
        key: String,
        value: i64,
        tags: Vec<(String, String)>,
    }

    #[derive(Default)]
    struct RecordingMetrics {
        counters: RefCell<Vec<Record>>,
        histograms: RefCell<Vec<Record>>,
    }

    impl RecordingMetrics {
        fn counter_records(&self) -> Vec<Record> {
            self.counters.borrow().clone()
        }

        fn histogram_records(&self) -> Vec<Record> {
            self.histograms.borrow().clone()
        }
    }

    impl AgentBusMetrics for RecordingMetrics {
        fn record_counter(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
            self.counters.borrow_mut().push(record(key, value, tags));
        }

        fn record_histogram(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
            self.histograms.borrow_mut().push(record(key, value, tags));
        }
    }

    fn record(key: &str, value: i64, tags: &[(&str, &str)]) -> Record {
        Record {
            key: key.to_string(),
            value,
            tags: tags
                .iter()
                .map(|(tag_key, tag_value)| (tag_key.to_string(), tag_value.to_string()))
                .collect(),
        }
    }

    #[test]
    fn prefixed_metrics_prefixes_counters_and_preserves_tags() {
        let inner = Rc::new(RecordingMetrics::default());
        let wrapped_inner: Rc<dyn AgentBusMetrics> = inner.clone();
        let metrics = PrefixedMetrics::new("commit_service", wrapped_inner);

        metrics.record_counter(
            "apply.applicator.num_calls",
            1,
            &[("applicator_kind", "decider")],
        );

        assert_eq!(
            inner.counter_records(),
            vec![record(
                "commit_service.apply.applicator.num_calls",
                1,
                &[("applicator_kind", "decider")]
            )]
        );
    }

    #[test]
    fn prefixed_metrics_prefixes_histograms() {
        let inner = Rc::new(RecordingMetrics::default());
        let wrapped_inner: Rc<dyn AgentBusMetrics> = inner.clone();
        let metrics = PrefixedMetrics::new("commit_service", wrapped_inner);

        metrics.record_histogram("write.write_once_space.latency_us", 42, &[]);

        assert_eq!(
            inner.histogram_records(),
            vec![record(
                "commit_service.write.write_once_space.latency_us",
                42,
                &[]
            )]
        );
    }
}
