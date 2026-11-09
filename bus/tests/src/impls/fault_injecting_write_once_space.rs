/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! FaultInjectingWriteOnceSpace: A wrapper that injects faults into any WriteOnceSpace implementation

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use agentbus_api::environment::Environment;
use anyhow::anyhow;
use bytes::Bytes;
use rand::distr::Distribution;
use rand::distr::Uniform;

use crate::common::fault_config::Fate;
use crate::common::fault_config::FaultConfig;
use crate::simulator::Simulator;

/// Injects one ambiguous write, optionally applying the attempted write first.
#[derive(Clone)]
pub struct AmbiguousOnceWriteOnceSpace<W> {
    inner: W,
    ambiguous_next_write: Rc<Cell<bool>>,
    apply_ambiguous_write: bool,
}

impl<W> AmbiguousOnceWriteOnceSpace<W> {
    pub fn new(inner: W, apply_ambiguous_write: bool) -> Self {
        Self {
            inner,
            ambiguous_next_write: Rc::new(Cell::new(true)),
            apply_ambiguous_write,
        }
    }
}

impl<W: WriteOnceSpace> TailableSpace for AmbiguousOnceWriteOnceSpace<W> {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        self.inner.tail(space_id, window_size).await
    }
}

impl<W: WriteOnceSpace> WriteOnceSpace for AmbiguousOnceWriteOnceSpace<W> {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        if self.ambiguous_next_write.replace(false) {
            if self.apply_ambiguous_write {
                self.inner.write(space_id, address, value.clone()).await?;
            }
            return Err(WriteOnceError::AmbiguousWrite(address));
        }

        self.inner.write(space_id, address, value).await
    }

    async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
        self.inner.read(space_id, address).await
    }
}

/// FaultInjectingWriteOnceSpace - wraps any WriteOnceSpace with fault injection and optional latency
pub struct FaultInjectingWriteOnceSpace<
    W: WriteOnceSpace + Clone,
    LatencyMs: Distribution<u64> = Uniform<u64>,
> {
    inner: W,
    fault_config: FaultConfig,
    write_latency_ms: LatencyMs,
    read_latency_ms: LatencyMs,
    env: Rc<Simulator>,
}

impl<W: WriteOnceSpace + Clone, LatencyMs: Distribution<u64> + Clone> Clone
    for FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fault_config: self.fault_config.clone(),
            write_latency_ms: self.write_latency_ms.clone(),
            read_latency_ms: self.read_latency_ms.clone(),
            env: self.env.clone(),
        }
    }
}

impl<W: WriteOnceSpace + Clone, LatencyMs: Distribution<u64>>
    FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    pub fn new(
        inner: W,
        fault_config: FaultConfig,
        write_latency_ms: LatencyMs,
        read_latency_ms: LatencyMs,
        env: Rc<Simulator>,
    ) -> Result<Self, String> {
        fault_config.validate()?;
        Ok(Self {
            inner,
            fault_config,
            write_latency_ms,
            read_latency_ms,
            env,
        })
    }

    fn determine_fate(&self) -> Fate {
        self.env
            .with_rng(|rng| self.fault_config.determine_fate(rng))
    }
}

impl<W: WriteOnceSpace + Clone + 'static, LatencyMs: Distribution<u64> + Clone + 'static>
    TailableSpace for FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let ms = self.env.with_rng(|rng| self.read_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        self.inner.tail(space_id, window_size).await
    }
}

impl<W: WriteOnceSpace + Clone + 'static, LatencyMs: Distribution<u64> + Clone + 'static>
    WriteOnceSpace for FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let ms = self.env.with_rng(|rng| self.write_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        match self.determine_fate() {
            Fate::Success => self.inner.write(space_id, address, value).await,
            Fate::Lost => Err(WriteOnceError::BackendUnavailable(anyhow!("Lost"))),
            Fate::CommitThenError => {
                let _ = self.inner.write(space_id, address, value).await;
                Err(WriteOnceError::BackendUnavailable(anyhow!(
                    "CommitThenError"
                )))
            }
            Fate::ErrorThenCommit => {
                let mut inner_clone = self.inner.clone();
                let space_id = space_id.to_string();
                self.env.spawn(async move {
                    let _ = inner_clone.write(&space_id, address, value).await;
                });
                Err(WriteOnceError::BackendUnavailable(anyhow!(
                    "ErrorThenCommit"
                )))
            }
        }
    }

    async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
        let ms = self.env.with_rng(|rng| self.read_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        self.inner.read(space_id, address).await
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentbus_api::AgentBus;
    use agentbus_api::AgentBusError;
    use agentbus_api::AppendRequest;
    use agentbus_api::BusId;
    use agentbus_api::ConditionalWriteError;
    use agentbus_api::ConditionalWriteResult;
    use agentbus_api::ConditionalWriteSpace;
    use agentbus_api::PollRequest;
    use agentbus_api::Version;
    use agentbus_api::VersionedValue;
    use agentbus_conditional_write_space::InMemoryConditionalWriteSpace;
    use agentbus_conditional_write_space::WriteOnceSpaceAdapter;
    use agentbus_writeonce::WriteOnceAgentBus;

    use super::*;
    use crate::common::helpers::variant_name_to_payload;

    struct PendingWrite {
        space_id: String,
        address: u64,
        expected_version: Option<Version>,
        value: Bytes,
    }

    #[derive(Clone)]
    struct ConflictingSpace {
        inner: InMemoryConditionalWriteSpace,
        conflict_next_write: Rc<Cell<bool>>,
        defer_commit: bool,
        pending_write: Rc<RefCell<Option<PendingWrite>>>,
    }

    impl ConflictingSpace {
        async fn complete_pending_write(&mut self) {
            let pending = self
                .pending_write
                .borrow_mut()
                .take()
                .expect("a write should be pending");
            let version = self
                .inner
                .write(
                    &pending.space_id,
                    pending.address,
                    pending.expected_version,
                    pending.value,
                )
                .await
                .expect("the pending write should succeed");
            assert!(version.is_some(), "the pending write should commit");
        }
    }

    impl TailableSpace for ConflictingSpace {
        async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
            self.inner.tail(space_id, window_size).await
        }
    }

    impl ConditionalWriteSpace for ConflictingSpace {
        async fn write(
            &mut self,
            space_id: &str,
            address: u64,
            expected_version: Option<Version>,
            value: Bytes,
        ) -> ConditionalWriteResult<Option<Version>> {
            let inject_conflict = self.conflict_next_write.replace(false);
            if inject_conflict && self.defer_commit {
                *self.pending_write.borrow_mut() = Some(PendingWrite {
                    space_id: space_id.to_owned(),
                    address,
                    expected_version,
                    value,
                });
                return Err(ConditionalWriteError::TransactionConflict(anyhow!(
                    "injected conflict while an earlier attempt is pending"
                )));
            }
            let result = self
                .inner
                .write(space_id, address, expected_version, value)
                .await?;
            if inject_conflict {
                assert!(result.is_some(), "the injected attempt should commit");
                // A backend client's retry can conflict after an earlier attempt committed.
                return Err(ConditionalWriteError::TransactionConflict(anyhow!(
                    "injected conflict after commit"
                )));
            }
            Ok(result)
        }

        async fn read(
            &self,
            space_id: &str,
            address: u64,
        ) -> ConditionalWriteResult<Option<VersionedValue>> {
            self.inner.read(space_id, address).await
        }
    }

    #[test]
    fn transaction_conflict_after_commit_does_not_duplicate_append() {
        futures::executor::block_on(async {
            let env = Rc::new(Simulator::new(0));
            let space = ConflictingSpace {
                inner: InMemoryConditionalWriteSpace::new(env.clone()),
                conflict_next_write: Rc::new(Cell::new(true)),
                defer_commit: false,
                pending_write: Rc::new(RefCell::new(None)),
            };
            let bus = WriteOnceAgentBus::new(WriteOnceSpaceAdapter::new(space), env, None);
            let bus_id = BusId {
                agent_bus_id: "conflict-after-commit".to_owned(),
            };
            let payload = variant_name_to_payload("intention");
            let result = bus
                .append(AppendRequest {
                    bus_id: Some(bus_id.clone()),
                    payload: Some(payload.clone()),
                })
                .await;
            let entries = bus
                .poll(PollRequest {
                    bus_id: Some(bus_id),
                    start_log_position: 0,
                    max_entries: 10,
                    filter: None,
                })
                .await
                .expect("reading the committed entry should succeed")
                .entries;

            assert_eq!(
                entries.len(),
                1,
                "one append must not duplicate its payload after a conflict; append returned {result:?}"
            );
            assert_eq!(entries[0].payload.as_ref(), Some(&payload));
            assert_eq!(
                entries[0]
                    .header
                    .as_ref()
                    .expect("the committed entry should have a header")
                    .log_position,
                0
            );
            assert!(matches!(result, Err(AgentBusError::Timeout(_))));
        });
    }

    #[test]
    fn transaction_conflict_before_commit_leaves_append_ambiguous() {
        futures::executor::block_on(async {
            let env = Rc::new(Simulator::new(0));
            let mut space = ConflictingSpace {
                inner: InMemoryConditionalWriteSpace::new(env.clone()),
                conflict_next_write: Rc::new(Cell::new(true)),
                defer_commit: true,
                pending_write: Rc::new(RefCell::new(None)),
            };
            let bus = WriteOnceAgentBus::new(WriteOnceSpaceAdapter::new(space.clone()), env, None);
            let bus_id = BusId {
                agent_bus_id: "conflict-before-commit".to_owned(),
            };
            let payload = variant_name_to_payload("intention");
            let result = bus
                .append(AppendRequest {
                    bus_id: Some(bus_id.clone()),
                    payload: Some(payload.clone()),
                })
                .await;
            let poll_request = PollRequest {
                bus_id: Some(bus_id),
                start_log_position: 0,
                max_entries: 10,
                filter: None,
            };
            let entries = bus
                .poll(poll_request.clone())
                .await
                .expect("reading before commit should succeed")
                .entries;
            assert!(
                entries.is_empty(),
                "the pending write must not have landed; append returned {result:?}"
            );
            assert!(matches!(result, Err(AgentBusError::Timeout(_))));

            space.complete_pending_write().await;

            let entries = bus
                .poll(poll_request)
                .await
                .expect("reading after commit should succeed")
                .entries;
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].payload.as_ref(), Some(&payload));
            assert_eq!(
                entries[0]
                    .header
                    .as_ref()
                    .expect("the committed entry should have a header")
                    .log_position,
                0
            );
        });
    }
}
