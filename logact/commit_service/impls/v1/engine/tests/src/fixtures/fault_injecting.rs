/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::environment::Environment;
use agentbus_simulator::Simulator;
use anyhow::anyhow;
use bytes::Bytes;
use conformance::ConformanceFixture;
use conformance::SimulatorFixture;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::StorageError;
use logact_commit_service_engine::StorageResult;
use rand::RngExt as _;

/// Faults and delays injected by [`FaultInjectingStorage`].
#[derive(Clone, Debug, Default)]
pub struct StorageFaultConfig {
    /// Probability that a get returns `BackendUnavailable`.
    pub get_failure_rate: f64,
    /// Probability that a put returns `BackendUnavailable`.
    pub put_failure_rate: f64,
    /// Delay before a get reaches the wrapped storage.
    pub get_delay: Duration,
    /// Delay before an accepted put reaches the wrapped storage.
    pub put_delay: Duration,
    /// Whether same-key puts overlapping during `put_delay` conflict.
    pub conflict_on_overlapping_put: bool,
}

/// Counts of faults injected by a [`FaultInjectingStorage`] and its clones.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageFaultCounts {
    /// Injected get failures.
    pub get_failures: u64,
    /// Injected put failures.
    pub put_failures: u64,
    /// Injected conflicts with an overlapping same-key put.
    pub transaction_conflicts: u64,
}

#[derive(Default)]
struct FaultInjectionState {
    in_flight_puts: RefCell<HashSet<String>>,
    get_failures: Cell<u64>,
    put_failures: Cell<u64>,
    transaction_conflicts: Cell<u64>,
}

struct InFlightPutGuard {
    state: Rc<FaultInjectionState>,
    key: String,
}

impl Drop for InFlightPutGuard {
    fn drop(&mut self) {
        self.state.in_flight_puts.borrow_mut().remove(&self.key);
    }
}

/// Wraps any `Storage` and injects configured failures, write behavior, and
/// operation latency.
pub struct FaultInjectingStorage<S, E> {
    inner: S,
    env: Rc<E>,
    config: StorageFaultConfig,
    state: Rc<FaultInjectionState>,
}

impl<S: Clone, E> Clone for FaultInjectingStorage<S, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            env: self.env.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
        }
    }
}

impl<S: Storage, E: Environment> FaultInjectingStorage<S, E> {
    /// Wraps `inner` with the requested fault profile.
    pub fn new(inner: S, env: Rc<E>, config: StorageFaultConfig) -> Self {
        Self {
            inner,
            env,
            config,
            state: Rc::default(),
        }
    }

    /// Returns counts shared by this wrapper and all of its clones.
    pub fn fault_counts(&self) -> StorageFaultCounts {
        StorageFaultCounts {
            get_failures: self.state.get_failures.get(),
            put_failures: self.state.put_failures.get(),
            transaction_conflicts: self.state.transaction_conflicts.get(),
        }
    }

    fn should_fail(&self, failure_rate: f64) -> bool {
        if failure_rate <= 0.0 {
            return false;
        }
        self.env.with_rng(|rng| rng.random::<f64>() < failure_rate)
    }

    fn acquire_in_flight_put(&self, key: &str) -> StorageResult<Option<InFlightPutGuard>> {
        if !self.config.conflict_on_overlapping_put {
            return Ok(None);
        }

        if !self
            .state
            .in_flight_puts
            .borrow_mut()
            .insert(key.to_string())
        {
            self.state
                .transaction_conflicts
                .set(self.state.transaction_conflicts.get().saturating_add(1));
            return Err(StorageError::TransactionConflict(anyhow!(
                "injected conflict with in-flight put for key '{key}'"
            )));
        }

        Ok(Some(InFlightPutGuard {
            state: self.state.clone(),
            key: key.to_string(),
        }))
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage, E: Environment> Storage for FaultInjectingStorage<S, E> {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        if self.should_fail(self.config.get_failure_rate) {
            self.state
                .get_failures
                .set(self.state.get_failures.get().saturating_add(1));
            return Err(StorageError::BackendUnavailable(anyhow!(
                "injected fault on get"
            )));
        }
        if !self.config.get_delay.is_zero() {
            self.env.sleep(self.config.get_delay).await;
        }
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        if self.should_fail(self.config.put_failure_rate) {
            self.state
                .put_failures
                .set(self.state.put_failures.get().saturating_add(1));
            return Err(StorageError::BackendUnavailable(anyhow!(
                "injected fault on put"
            )));
        }

        let _in_flight_put = self.acquire_in_flight_put(key)?;
        if !self.config.put_delay.is_zero() {
            self.env.sleep(self.config.put_delay).await;
        }

        self.inner.put(key, value, expected, new_position).await
    }
}

/// Pass-through fault-injection fixture for running the ordinary storage suite.
pub struct FaultInjectingStorageFixture<F> {
    inner: F,
}

impl<F> ConformanceFixture for FaultInjectingStorageFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: Storage> + SimulatorFixture,
{
    type Env = Simulator;
    type Impl = FaultInjectingStorage<F::Impl, Simulator>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        FaultInjectingStorage::new(
            self.inner.create_impl(),
            self.inner.get_env(),
            StorageFaultConfig::default(),
        )
    }
}

impl<F> SimulatorFixture for FaultInjectingStorageFixture<F>
where
    F: ConformanceFixture<Env = Simulator, Impl: Storage> + SimulatorFixture,
{
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
        }
    }
}
