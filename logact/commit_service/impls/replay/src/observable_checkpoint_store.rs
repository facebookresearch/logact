/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Metrics and structured logging for checkpoint loads and completed saves.

use std::time::Duration;

use agentbus_api::AgentbusLogger;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::LogRow;
use agentbus_api::MonotonicInstant;
use logact_commit_service_engine::Observability;
use logact_commit_service_engine::StorageError;

use crate::Checkpoint;
use crate::CheckpointError;
use crate::CheckpointResult;
use crate::CheckpointStore;

/// Decorates a [`CheckpointStore`] with operation metrics and structured rows.
/// Place inside [`crate::BackgroundCheckpoints`] to observe completed saves.
/// A successful save may leave an already newer stored checkpoint unchanged.
pub struct ObservableCheckpointStore<C, L, E: Environment> {
    inner: C,
    component: &'static str,
    observability: Observability<L, E>,
}

impl<C, L, E: Environment> ObservableCheckpointStore<C, L, E> {
    /// Wrap a checkpoint store with shared metrics, logging, and timing sinks.
    pub fn new(inner: C, component: &'static str, observability: Observability<L, E>) -> Self {
        Self {
            inner,
            component,
            observability,
        }
    }

    fn monotonic_time(&self) -> MonotonicInstant {
        self.observability
            .environment
            .with_clock(|clock| clock.monotonic_time())
    }
}

impl<C: Clone, L, E: Environment> Clone for ObservableCheckpointStore<C, L, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            component: self.component,
            observability: self.observability.clone(),
        }
    }
}

fn base_row(component: &str, method: &str, bus_id: &str, elapsed: Duration) -> LogRow {
    LogRow::default()
        .api_call_name(method)
        .latency_ms(elapsed.as_millis() as u64)
        .add_string_field("component", component)
        .add_string_field("resource_id", bus_id)
}

fn checkpoint_error_code(error: &CheckpointError) -> &'static str {
    match error {
        CheckpointError::Storage(error) => match error {
            StorageError::InvalidPosition(_) => "StorageInvalidPosition",
            StorageError::TransactionConflict(_) => "StorageTransactionConflict",
            StorageError::Timeout(_) => "StorageTimeout",
            StorageError::BackendUnavailable(_) => "StorageBackendUnavailable",
            StorageError::InternalError(_) => "StorageInternalError",
        },
        CheckpointError::Unavailable(_) => "Unavailable",
        CheckpointError::Internal(_) => "Internal",
    }
}

fn checkpoint_error_message(error: &CheckpointError) -> String {
    match error {
        CheckpointError::Storage(error) => {
            let source = match error {
                StorageError::InvalidPosition(_) => return error.to_string(),
                StorageError::TransactionConflict(source)
                | StorageError::Timeout(source)
                | StorageError::BackendUnavailable(source)
                | StorageError::InternalError(source) => source,
            };
            format!("{error}: {source:#}")
        }
        CheckpointError::Unavailable(source) | CheckpointError::Internal(source) => {
            format!("{source:#}")
        }
    }
}

fn add_result<T>(row: LogRow, result: &CheckpointResult<T>) -> LogRow {
    match result {
        Ok(_) => row.add_i64_field("success", 1),
        Err(error) => row
            .add_i64_field("success", 0)
            .error_code(checkpoint_error_code(error))
            .error_message(checkpoint_error_message(error)),
    }
}

impl<C, L, E> CheckpointStore for ObservableCheckpointStore<C, L, E>
where
    C: CheckpointStore,
    L: AgentbusLogger,
    E: Environment,
{
    async fn load(&self, bus_id: &str) -> CheckpointResult<Option<Checkpoint>> {
        let metrics = &self.observability.metrics;
        let tags = [("component", self.component)];
        metrics.record_counter("load.checkpoint_store.num_calls", 1, &tags);
        let start = self.monotonic_time();

        let result = self.inner.load(bus_id).await;

        let elapsed = self.monotonic_time().duration_since(start);
        metrics.record_histogram(
            "load.checkpoint_store.latency_us",
            elapsed.as_micros() as i64,
            &tags,
        );
        let row = add_result(base_row(self.component, "load", bus_id, elapsed), &result);
        let row = match &result {
            Ok(Some(checkpoint)) => row.add_i64_field("log_position", checkpoint.position),
            Ok(None) => {
                metrics.record_counter("load.checkpoint_store.num_misses", 1, &tags);
                row
            }
            Err(_) => {
                metrics.record_counter("load.checkpoint_store.num_errors", 1, &tags);
                row
            }
        };
        self.observability.logger.log_row(row);
        result
    }

    async fn save(&self, bus_id: &str, checkpoint: &Checkpoint) -> CheckpointResult<()> {
        let metrics = &self.observability.metrics;
        let tags = [("component", self.component)];
        metrics.record_counter("save.checkpoint_store.num_calls", 1, &tags);
        let start = self.monotonic_time();

        let result = self.inner.save(bus_id, checkpoint).await;

        let elapsed = self.monotonic_time().duration_since(start);
        metrics.record_histogram(
            "save.checkpoint_store.latency_us",
            elapsed.as_micros() as i64,
            &tags,
        );
        if result.is_err() {
            metrics.record_counter("save.checkpoint_store.num_errors", 1, &tags);
        }
        let row = base_row(self.component, "save", bus_id, elapsed)
            .add_i64_field("log_position", checkpoint.position);
        self.observability.logger.log_row(add_result(row, &result));
        result
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agentbus_api::AgentBusMetrics;
    use agentbus_api::InMemoryLogger;
    use agentbus_api::InMemoryMetrics;
    use agentbus_api::LogFieldValue;
    use agentbus_api::RealEnvironment;
    use anyhow::anyhow;
    use futures::channel::oneshot;
    use futures::executor::block_on;
    use logact_commit_service_engine::InMemoryStorage;
    use logact_commit_service_engine::PerBusEngineState;
    use logact_commit_service_engine::VersionedDecider;

    use super::*;
    use crate::BackgroundCheckpoints;
    use crate::ReplayState;
    use crate::StorageCheckpoints;
    use crate::storage::encode_engine_record;

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

    fn checkpoint(position: i64) -> Checkpoint {
        Checkpoint {
            position,
            state: ReplayState::from_engine_state(PerBusEngineState {
                decider: Some(VersionedDecider {
                    policy: DeciderPolicy::OnByDefault as i32,
                    id: 0,
                }),
                applied_policy_version: Some(position),
                ..Default::default()
            })
            .expect("valid engine state"),
        }
    }

    fn field<'a>(row: &'a LogRow, key: &str) -> Option<&'a LogFieldValue> {
        row.additional_fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }

    #[test]
    fn records_hits_misses_and_saves_without_regressing_checkpoints() {
        let (sinks, metrics, logger) = observability();
        let store = ObservableCheckpointStore::new(
            StorageCheckpoints::new(Rc::new(InMemoryStorage::new())),
            "test_checkpoints",
            sinks,
        );
        let newest = checkpoint(9);
        block_on(async {
            assert!(store.load("bus").await.unwrap().is_none());
            store.save("bus", &newest).await.unwrap();
            store.clone().save("bus", &checkpoint(7)).await.unwrap();
            let loaded = store.load("bus").await.unwrap().unwrap();
            assert_eq!(loaded.position, newest.position);
            assert_eq!(
                encode_engine_record(&loaded.state),
                encode_engine_record(&newest.state)
            );
        });

        assert_eq!(metrics.counter("load.checkpoint_store.num_calls"), 2);
        assert_eq!(metrics.counter("load.checkpoint_store.num_misses"), 1);
        assert_eq!(metrics.counter("load.checkpoint_store.num_errors"), 0);
        assert_eq!(
            metrics.histogram("load.checkpoint_store.latency_us").len(),
            2
        );
        assert_eq!(metrics.counter("save.checkpoint_store.num_calls"), 2);
        assert_eq!(metrics.counter("save.checkpoint_store.num_errors"), 0);
        assert_eq!(
            metrics.histogram("save.checkpoint_store.latency_us").len(),
            2
        );

        let rows = logger.rows();
        assert_eq!(rows.len(), 4);
        for row in &rows {
            assert_eq!(
                field(row, "component"),
                Some(&LogFieldValue::String("test_checkpoints".to_owned()))
            );
            assert_eq!(
                field(row, "resource_id"),
                Some(&LogFieldValue::String("bus".to_owned()))
            );
            assert_eq!(field(row, "success"), Some(&LogFieldValue::I64(1)));
            assert_eq!(row.error_code, None);
            assert_eq!(row.error_message, None);
        }
        assert_eq!(rows[0].api_call_name, "load");
        assert_eq!(field(&rows[0], "log_position"), None);
        for (row, position) in [(&rows[1], 9), (&rows[2], 7)] {
            assert_eq!(row.api_call_name, "save");
            assert_eq!(
                field(row, "log_position"),
                Some(&LogFieldValue::I64(position))
            );
        }
        assert_eq!(rows[3].api_call_name, "load");
        assert_eq!(
            field(&rows[3], "log_position"),
            Some(&LogFieldValue::I64(9))
        );
    }

    struct FailingStore(fn(anyhow::Error) -> CheckpointError);

    #[test]
    fn preserves_invalid_storage_position() {
        let (sinks, metrics, logger) = observability();
        let store = ObservableCheckpointStore::new(
            StorageCheckpoints::new(Rc::new(InMemoryStorage::new())),
            "storage_checkpoints",
            sinks,
        );
        assert!(matches!(
            block_on(store.save("bus", &checkpoint(-1))),
            Err(CheckpointError::Storage(StorageError::InvalidPosition(-1)))
        ));
        assert_eq!(metrics.counter("save.checkpoint_store.num_errors"), 1);
        let rows = logger.rows();
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("StorageInvalidPosition")
        );
        assert_eq!(
            rows[0].error_message.as_deref(),
            Some("logical position must be nonnegative: -1")
        );
    }

    fn failure() -> anyhow::Error {
        anyhow!(std::io::Error::other("backend source")).context("checkpoint failure")
    }

    impl CheckpointStore for FailingStore {
        async fn load(&self, _bus_id: &str) -> CheckpointResult<Option<Checkpoint>> {
            Err((self.0)(failure()))
        }

        async fn save(&self, _bus_id: &str, _checkpoint: &Checkpoint) -> CheckpointResult<()> {
            Err((self.0)(failure()))
        }
    }

    #[test]
    fn preserves_all_error_categories_and_source_chains() {
        let cases: [(fn(anyhow::Error) -> CheckpointError, &str); 6] = [
            (
                |source| StorageError::TransactionConflict(source).into(),
                "StorageTransactionConflict",
            ),
            (
                |source| StorageError::Timeout(source).into(),
                "StorageTimeout",
            ),
            (
                |source| StorageError::BackendUnavailable(source).into(),
                "StorageBackendUnavailable",
            ),
            (
                |source| StorageError::InternalError(source).into(),
                "StorageInternalError",
            ),
            (CheckpointError::Unavailable, "Unavailable"),
            (CheckpointError::Internal, "Internal"),
        ];
        for (make_error, code) in cases {
            let (sinks, metrics, logger) = observability();
            let store = ObservableCheckpointStore::new(
                FailingStore(make_error),
                "failing_checkpoints",
                sinks,
            );
            for error in [
                block_on(store.load("bus")).unwrap_err(),
                block_on(store.save("bus", &checkpoint(9))).unwrap_err(),
            ] {
                assert_eq!(checkpoint_error_code(&error), code);
                let source = match error {
                    CheckpointError::Storage(StorageError::TransactionConflict(source))
                    | CheckpointError::Storage(StorageError::Timeout(source))
                    | CheckpointError::Storage(StorageError::BackendUnavailable(source))
                    | CheckpointError::Storage(StorageError::InternalError(source))
                    | CheckpointError::Unavailable(source)
                    | CheckpointError::Internal(source) => source,
                    CheckpointError::Storage(StorageError::InvalidPosition(_)) => {
                        panic!("unexpected invalid position")
                    }
                };
                assert!(source.downcast_ref::<std::io::Error>().is_some());
                assert_eq!(format!("{source:#}"), "checkpoint failure: backend source");
            }
            assert_eq!(metrics.counter("load.checkpoint_store.num_errors"), 1);
            assert_eq!(metrics.counter("load.checkpoint_store.num_misses"), 0);
            assert_eq!(metrics.counter("save.checkpoint_store.num_errors"), 1);
            let rows = logger.rows();
            assert_eq!(rows.len(), 2);
            for row in &rows {
                assert_eq!(field(row, "success"), Some(&LogFieldValue::I64(0)));
                assert_eq!(row.error_code.as_deref(), Some(code));
                assert!(
                    row.error_message
                        .as_deref()
                        .unwrap()
                        .ends_with("checkpoint failure: backend source")
                );
            }
            assert_eq!(
                field(&rows[1], "log_position"),
                Some(&LogFieldValue::I64(9))
            );
        }
    }

    #[derive(Clone)]
    struct PendingSave(Rc<RefCell<Option<oneshot::Receiver<()>>>>);

    impl CheckpointStore for PendingSave {
        async fn load(&self, _bus_id: &str) -> CheckpointResult<Option<Checkpoint>> {
            Ok(None)
        }

        async fn save(&self, _bus_id: &str, _checkpoint: &Checkpoint) -> CheckpointResult<()> {
            let ready = self.0.borrow_mut().take().unwrap();
            ready.await.unwrap();
            Err(StorageError::BackendUnavailable(failure()).into())
        }
    }

    struct ComponentMetrics {
        inner: Rc<InMemoryMetrics>,
        counters: RefCell<Vec<(String, String, i64)>>,
    }

    impl ComponentMetrics {
        fn counter(&self, key: &str, component: &str) -> i64 {
            self.counters
                .borrow()
                .iter()
                .filter(|(name, label, _)| name == key && label == component)
                .map(|(_, _, value)| value)
                .sum()
        }
    }

    impl AgentBusMetrics for ComponentMetrics {
        fn record_counter(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
            let component = tags
                .iter()
                .find(|(name, _)| *name == "component")
                .expect("checkpoint counters should have a component tag")
                .1;
            self.counters
                .borrow_mut()
                .push((key.to_owned(), component.to_owned(), value));
            self.inner.record_counter(key, value, tags);
        }

        fn record_histogram(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
            assert_eq!(tags.len(), 1);
            assert_eq!(tags[0].0, "component");
            self.inner.record_histogram(key, value, tags);
        }
    }

    #[test]
    fn observes_background_save_failure_after_completion() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async {
            let (mut sinks, metrics, logger) = observability();
            let components = Rc::new(ComponentMetrics {
                inner: metrics.clone(),
                counters: RefCell::new(Vec::new()),
            });
            sinks.metrics = components.clone();
            let environment = sinks.environment.clone();
            let (ready, pending) = oneshot::channel();
            let store = ObservableCheckpointStore::new(
                BackgroundCheckpoints::new(
                    ObservableCheckpointStore::new(
                        PendingSave(Rc::new(RefCell::new(Some(pending)))),
                        "storage_checkpoints",
                        sinks.clone(),
                    ),
                    environment,
                ),
                "background_checkpoints",
                sinks,
            );

            store.save("bus", &checkpoint(9)).await.unwrap();
            assert_eq!(logger.rows().len(), 1, "background scheduling completed");
            assert_eq!(
                components.counter("save.checkpoint_store.num_calls", "background_checkpoints"),
                1
            );
            assert_eq!(
                components.counter("save.checkpoint_store.num_calls", "storage_checkpoints"),
                0
            );
            tokio::task::yield_now().await;
            assert_eq!(
                components.counter("save.checkpoint_store.num_calls", "storage_checkpoints"),
                1
            );
            assert_eq!(
                metrics.histogram("save.checkpoint_store.latency_us").len(),
                1
            );
            assert_eq!(logger.rows().len(), 1, "underlying save is still pending");

            ready.send(()).unwrap();
            tokio::task::yield_now().await;
            assert_eq!(metrics.counter("save.checkpoint_store.num_errors"), 1);
            assert_eq!(
                components.counter("save.checkpoint_store.num_errors", "background_checkpoints"),
                0
            );
            assert_eq!(
                components.counter("save.checkpoint_store.num_errors", "storage_checkpoints"),
                1
            );
            assert_eq!(
                metrics.histogram("save.checkpoint_store.latency_us").len(),
                2
            );
            let rows = logger.rows();
            assert_eq!(rows.len(), 2);
            for row in &rows {
                assert_eq!(row.api_call_name, "save");
                assert_eq!(field(row, "log_position"), Some(&LogFieldValue::I64(9)));
            }
            assert_eq!(
                field(&rows[0], "component"),
                Some(&LogFieldValue::String("background_checkpoints".to_owned()))
            );
            assert_eq!(field(&rows[0], "success"), Some(&LogFieldValue::I64(1)));
            assert_eq!(rows[0].error_code, None);
            assert_eq!(
                field(&rows[1], "component"),
                Some(&LogFieldValue::String("storage_checkpoints".to_owned()))
            );
            assert_eq!(field(&rows[1], "success"), Some(&LogFieldValue::I64(0)));
            assert_eq!(
                rows[1].error_code.as_deref(),
                Some("StorageBackendUnavailable")
            );
        });
    }
}
