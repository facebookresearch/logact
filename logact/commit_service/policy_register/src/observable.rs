/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use agentbus_api::AgentbusLogger;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::LogRow;
use agentbus_api::MonotonicInstant;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;
use logact_commit_service_engine::Observability;
use logact_commit_service_engine::storage_error_code;
use logact_commit_service_engine::storage_error_message;

use crate::PolicyRegister;
use crate::PolicyRegisterError;
use crate::PolicyRegisterResult;

const NO_TAGS: &[(&str, &str)] = &[];
const COMPONENT: &str = "policy_register";

/// Decorates a [`PolicyRegister`] with metrics and structured-row logging.
pub struct ObservablePolicyRegister<T, L, E: Environment> {
    inner: T,
    observability: Observability<L, E>,
}

impl<T, L, E: Environment> ObservablePolicyRegister<T, L, E> {
    /// Wrap `inner` with the shared engine observability sinks.
    pub fn new(inner: T, observability: Observability<L, E>) -> Self {
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

impl<T: Clone, L, E: Environment> Clone for ObservablePolicyRegister<T, L, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            observability: self.observability.clone(),
        }
    }
}

fn error_code(error: &PolicyRegisterError) -> &'static str {
    match error {
        PolicyRegisterError::Uninitialized { .. } => "Uninitialized",
        PolicyRegisterError::Storage(error) => storage_error_code(error),
        PolicyRegisterError::MalformedPolicy { .. } => "MalformedPolicy",
        PolicyRegisterError::GenerationOverflow { .. } => "GenerationOverflow",
        PolicyRegisterError::ConcurrentUpdate { .. } => "ConcurrentUpdate",
    }
}

fn add_result_status<T>(row: LogRow, result: &PolicyRegisterResult<T>) -> LogRow {
    match result {
        Ok(_) => row.add_i64_field("success", 1),
        Err(error) => {
            let message = match error {
                PolicyRegisterError::Storage(storage) => {
                    format!("{error}: {}", storage_error_message(storage))
                }
                _ => error.to_string(),
            };
            row.add_i64_field("success", 0)
                .error_code(error_code(error))
                .error_message(message)
        }
    }
}

impl<T: PolicyRegister, L, E: Environment> ObservablePolicyRegister<T, L, E> {
    fn base_row(&self, method: &str, elapsed: Duration) -> LogRow {
        LogRow::default()
            .api_call_name(method)
            .latency_ms(elapsed.as_millis() as u64)
            .add_string_field("component", COMPONENT)
            .add_string_field("resource_id", self.inner.key())
    }
}

impl<T, L, E> PolicyRegister for ObservablePolicyRegister<T, L, E>
where
    T: PolicyRegister,
    L: AgentbusLogger,
    E: Environment,
{
    fn key(&self) -> &str {
        self.inner.key()
    }

    async fn read_validated(&self) -> PolicyRegisterResult<VersionedPolicyState> {
        self.observability.metrics.record_counter(
            "read_validated.policy_register.num_calls",
            1,
            NO_TAGS,
        );
        let start = self.monotonic_time();

        let result = self.inner.read_validated().await;

        let elapsed = self.elapsed(start);
        self.observability.metrics.record_histogram(
            "read_validated.policy_register.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );
        if result.is_err() {
            self.observability.metrics.record_counter(
                "read_validated.policy_register.num_errors",
                1,
                NO_TAGS,
            );
        }
        let mut row = add_result_status(self.base_row("read_validated", elapsed), &result);
        if let Ok(policy) = &result {
            row = row.add_string_vector_field(
                "response_context",
                vec![format!("version:{}", policy.version)],
            );
        }
        self.observability.logger.log_row(row);

        result
    }

    async fn set_policy(
        &self,
        state: &PolicyState,
        expected_version: Option<i64>,
    ) -> PolicyRegisterResult<i64> {
        self.observability.metrics.record_counter(
            "set_policy.policy_register.num_calls",
            1,
            NO_TAGS,
        );
        let start = self.monotonic_time();

        let result = self.inner.set_policy(state, expected_version).await;

        let elapsed = self.elapsed(start);
        self.observability.metrics.record_histogram(
            "set_policy.policy_register.latency_us",
            elapsed.as_micros() as i64,
            NO_TAGS,
        );
        if result.is_err() {
            self.observability.metrics.record_counter(
                "set_policy.policy_register.num_errors",
                1,
                NO_TAGS,
            );
        }
        let expected =
            expected_version.map_or_else(|| "absent".to_string(), |version| version.to_string());
        let mut row = add_result_status(
            self.base_row("set_policy", elapsed)
                .add_string_vector_field("request_context", vec![format!("expected:{expected}")]),
            &result,
        );
        if let Ok(version) = &result {
            row =
                row.add_string_vector_field("response_context", vec![format!("version:{version}")]);
        }
        self.observability.logger.log_row(row);

        result
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use agentbus_api::InMemoryLogger;
    use agentbus_api::InMemoryMetrics;
    use agentbus_api::LogFieldValue;
    use agentbus_api::RealEnvironment;
    use anyhow::anyhow;
    use futures::executor::block_on;
    use logact_commit_service_engine::StorageError;

    use super::*;

    #[derive(Clone)]
    struct FakeRegister {
        requests: Rc<RefCell<Vec<(PolicyState, Option<i64>)>>>,
        error: Rc<RefCell<Option<PolicyRegisterError>>>,
    }

    impl PolicyRegister for FakeRegister {
        fn key(&self) -> &str {
            "policy-1"
        }

        async fn read_validated(&self) -> PolicyRegisterResult<VersionedPolicyState> {
            if let Some(error) = self.error.borrow_mut().take() {
                return Err(error);
            }
            Ok(VersionedPolicyState {
                state: PolicyState::default(),
                version: 7,
            })
        }

        async fn set_policy(
            &self,
            state: &PolicyState,
            expected_version: Option<i64>,
        ) -> PolicyRegisterResult<i64> {
            self.requests
                .borrow_mut()
                .push((state.clone(), expected_version));
            if let Some(error) = self.error.borrow_mut().take() {
                return Err(error);
            }
            Ok(7)
        }
    }

    fn observable() -> (
        ObservablePolicyRegister<FakeRegister, InMemoryLogger, RealEnvironment>,
        Rc<InMemoryMetrics>,
        InMemoryLogger,
    ) {
        let metrics = Rc::new(InMemoryMetrics::new());
        let logger = InMemoryLogger::new();
        let wrapper = ObservablePolicyRegister::new(
            FakeRegister {
                requests: Rc::new(RefCell::new(Vec::new())),
                error: Rc::new(RefCell::new(None)),
            },
            Observability {
                metrics: metrics.clone(),
                logger: Rc::new(logger.clone()),
                environment: Rc::new(RealEnvironment::new()),
            },
        );
        (wrapper, metrics, logger)
    }

    fn field(row: &LogRow, key: &str) -> Option<LogFieldValue> {
        row.additional_fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    }

    #[test]
    fn forwards_success_and_records_observability() {
        let (wrapper, metrics, logger) = observable();
        let state = PolicyState {
            decider_policy: Some(99),
            ..Default::default()
        };
        for expected in [None, Some(6)] {
            assert_eq!(
                block_on(wrapper.clone().set_policy(&state, expected)).unwrap(),
                7
            );
        }
        assert_eq!(
            wrapper.inner.requests.borrow().as_slice(),
            &[(state.clone(), None), (state, Some(6))]
        );
        assert_eq!(wrapper.key(), "policy-1");
        assert_eq!(
            block_on(wrapper.read_validated()).unwrap(),
            VersionedPolicyState {
                state: PolicyState::default(),
                version: 7,
            }
        );
        for (method, calls) in [("set_policy", 2), ("read_validated", 1)] {
            assert_eq!(
                metrics.counter(&format!("{method}.policy_register.num_calls")),
                calls
            );
            assert_eq!(
                metrics.counter(&format!("{method}.policy_register.num_errors")),
                0
            );
            assert_eq!(
                metrics
                    .histogram(&format!("{method}.policy_register.latency_us"))
                    .len(),
                calls as usize
            );
        }
        let rows = logger.rows();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(
                field(row, "component"),
                Some(LogFieldValue::String("policy_register".into()))
            );
            assert_eq!(
                field(row, "resource_id"),
                Some(LogFieldValue::String("policy-1".into()))
            );
            assert_eq!(field(row, "success"), Some(LogFieldValue::I64(1)));
            assert_eq!(row.error_code, None);
            assert_eq!(row.error_message, None);
            assert_eq!(
                field(row, "response_context"),
                Some(LogFieldValue::StringVector(vec!["version:7".into()]))
            );
        }
        assert_eq!(rows[0].api_call_name, "set_policy");
        assert_eq!(rows[2].api_call_name, "read_validated");
        assert_eq!(
            field(&rows[0], "request_context"),
            Some(LogFieldValue::StringVector(vec!["expected:absent".into()]))
        );
        assert_eq!(
            field(&rows[1], "request_context"),
            Some(LogFieldValue::StringVector(vec!["expected:6".into()]))
        );
    }

    fn errors() -> Vec<(PolicyRegisterError, &'static str)> {
        vec![
            (
                PolicyRegisterError::Uninitialized {
                    key: "policy-1".into(),
                },
                "Uninitialized",
            ),
            (
                PolicyRegisterError::MalformedPolicy {
                    message: "bad policy".into(),
                },
                "MalformedPolicy",
            ),
            (
                PolicyRegisterError::GenerationOverflow {
                    generation: i64::MAX,
                },
                "GenerationOverflow",
            ),
            (
                PolicyRegisterError::ConcurrentUpdate {
                    key: "policy-1".into(),
                },
                "ConcurrentUpdate",
            ),
            (
                PolicyRegisterError::Storage(StorageError::InvalidPosition(-1)),
                "InvalidPosition",
            ),
            (
                PolicyRegisterError::Storage(StorageError::TransactionConflict(anyhow!(
                    "conflict source"
                ))),
                "TransactionConflict",
            ),
            (
                PolicyRegisterError::Storage(StorageError::Timeout(anyhow!("timeout source"))),
                "Timeout",
            ),
            (
                PolicyRegisterError::Storage(StorageError::BackendUnavailable(anyhow!(
                    "unavailable source"
                ))),
                "BackendUnavailable",
            ),
            (
                PolicyRegisterError::Storage(StorageError::InternalError(anyhow!(
                    "internal source"
                ))),
                "InternalError",
            ),
        ]
    }

    #[test]
    fn preserves_errors_and_records_their_categories() {
        for method in ["read_validated", "set_policy"] {
            for (error, code) in errors() {
                let (wrapper, metrics, logger) = observable();
                let original = format!("{error:?}");
                *wrapper.inner.error.borrow_mut() = Some(error);
                let returned = if method == "read_validated" {
                    block_on(wrapper.read_validated()).unwrap_err()
                } else {
                    block_on(wrapper.set_policy(&PolicyState::default(), Some(6))).unwrap_err()
                };
                assert_eq!(format!("{returned:?}"), original);
                assert_eq!(
                    metrics.counter(&format!("{method}.policy_register.num_calls")),
                    1
                );
                assert_eq!(
                    metrics.counter(&format!("{method}.policy_register.num_errors")),
                    1
                );
                assert_eq!(
                    metrics
                        .histogram(&format!("{method}.policy_register.latency_us"))
                        .len(),
                    1
                );
                let rows = logger.rows();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].api_call_name, method);
                assert_eq!(field(&rows[0], "success"), Some(LogFieldValue::I64(0)));
                assert_eq!(rows[0].error_code.as_deref(), Some(code));
                assert_eq!(field(&rows[0], "response_context"), None);
                let message = rows[0].error_message.as_deref().unwrap();
                assert!(message.contains(&returned.to_string()));
                if let PolicyRegisterError::Storage(source) = returned {
                    assert!(message.contains(&source.to_string()));
                    match source {
                        StorageError::InvalidPosition(_) => {}
                        StorageError::TransactionConflict(source)
                        | StorageError::Timeout(source)
                        | StorageError::BackendUnavailable(source)
                        | StorageError::InternalError(source) => {
                            assert!(message.contains(&source.to_string()));
                        }
                    }
                }
            }
        }
    }
}
