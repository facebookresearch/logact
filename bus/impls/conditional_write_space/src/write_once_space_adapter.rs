/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agentbus_api::ConditionalWriteError;
use agentbus_api::ConditionalWriteSpace;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use bytes::Bytes;

/// Adapts any [`ConditionalWriteSpace`] to implement [`WriteOnceSpace`].
///
/// Write-once semantics are achieved by writing with `expected_version=None`,
/// which the underlying CAS succeeds only if the address is empty. A version
/// mismatch maps to [`WriteOnceError::AddressAlreadyExists`].
#[derive(Clone)]
pub struct WriteOnceSpaceAdapter<C> {
    inner: C,
}

impl<C> WriteOnceSpaceAdapter<C> {
    pub fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: ConditionalWriteSpace> TailableSpace for WriteOnceSpaceAdapter<C> {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        self.inner.tail(space_id, window_size).await
    }
}

fn map_conditional_read_error(error: ConditionalWriteError) -> WriteOnceError {
    match error {
        ConditionalWriteError::TransactionConflict(source) => {
            WriteOnceError::BackendUnavailable(source)
        }
        ConditionalWriteError::Timeout(source) => WriteOnceError::Timeout(source),
        ConditionalWriteError::BackendUnavailable(source) => {
            WriteOnceError::BackendUnavailable(source)
        }
        ConditionalWriteError::InternalError(source) => WriteOnceError::InternalError(source),
    }
}

fn map_conditional_write_error(error: ConditionalWriteError, address: u64) -> WriteOnceError {
    match error {
        // Transaction conflicts do not necessarily mean the write failed:
        // an earlier attempt retried by the backend client may have committed.
        ConditionalWriteError::TransactionConflict(_) => WriteOnceError::AmbiguousWrite(address),
        ConditionalWriteError::Timeout(source) => WriteOnceError::Timeout(source),
        ConditionalWriteError::BackendUnavailable(source) => {
            WriteOnceError::BackendUnavailable(source)
        }
        ConditionalWriteError::InternalError(source) => WriteOnceError::InternalError(source),
    }
}

impl<C: ConditionalWriteSpace> WriteOnceSpace for WriteOnceSpaceAdapter<C> {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let wrote = self
            .inner
            .write(space_id, address, None, value)
            .await
            .map_err(|error| map_conditional_write_error(error, address))?;

        if wrote.is_some() {
            Ok(())
        } else {
            Err(WriteOnceError::AddressAlreadyExists(address))
        }
    }

    async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
        self.inner
            .read(space_id, address)
            .await
            .map(|value| value.map(|v| v.value))
            .map_err(map_conditional_read_error)
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use agentbus_api::ConditionalWriteResult;
    use agentbus_api::RealEnvironment;
    use agentbus_api::Version;
    use agentbus_api::VersionedValue;

    use super::*;
    use crate::InMemoryConditionalWriteSpace;

    struct ConflictingSpace {
        inner: InMemoryConditionalWriteSpace,
        apply_write: bool,
        fail_reads: bool,
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
            if self.apply_write {
                self.inner
                    .write(space_id, address, expected_version, value)
                    .await?;
            }
            Err(ConditionalWriteError::TransactionConflict(
                std::io::Error::other("injected conflict").into(),
            ))
        }

        async fn read(
            &self,
            space_id: &str,
            address: u64,
        ) -> ConditionalWriteResult<Option<VersionedValue>> {
            if self.fail_reads {
                return Err(ConditionalWriteError::TransactionConflict(
                    std::io::Error::other("injected read conflict").into(),
                ));
            }
            self.inner.read(space_id, address).await
        }
    }

    #[test]
    fn transaction_conflicts_have_ambiguous_write_outcomes() {
        futures::executor::block_on(async {
            for apply_write in [false, true] {
                let mut space = WriteOnceSpaceAdapter::new(ConflictingSpace {
                    inner: InMemoryConditionalWriteSpace::new(Rc::new(RealEnvironment::new())),
                    apply_write,
                    fail_reads: false,
                });
                let value = Bytes::from_static(b"value");
                assert!(matches!(
                    space.write("s", 7, value.clone()).await,
                    Err(WriteOnceError::AmbiguousWrite(7))
                ));
                assert_eq!(
                    space.read("s", 7).await.expect("read should succeed"),
                    apply_write.then_some(value),
                    "a conflict does not establish whether the write applied"
                );
            }
        });
    }

    #[test]
    fn read_conflicts_are_backend_unavailable() {
        futures::executor::block_on(async {
            let space = WriteOnceSpaceAdapter::new(ConflictingSpace {
                inner: InMemoryConditionalWriteSpace::new(Rc::new(RealEnvironment::new())),
                apply_write: false,
                fail_reads: true,
            });
            let error = space.read("s", 7).await.expect_err("read should fail");
            assert!(matches!(error, WriteOnceError::BackendUnavailable(_)));
            assert!(error.to_string().contains("backend unavailable"));
        });
    }
}
