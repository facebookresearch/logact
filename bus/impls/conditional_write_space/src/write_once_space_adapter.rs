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
/// which the underlying CAS succeeds only if the address is empty. A `false`
/// result maps to [`WriteOnceError::AddressAlreadyExists`].
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

fn map_conditional_write_error(error: ConditionalWriteError) -> WriteOnceError {
    match error {
        ConditionalWriteError::TransactionConflict(source) => {
            WriteOnceError::TransactionConflict(source)
        }
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
            .map_err(map_conditional_write_error)?;

        if wrote {
            Ok(())
        } else {
            Err(WriteOnceError::AddressAlreadyExists(address))
        }
    }

    async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        self.inner
            .read(space_id, address)
            .await
            .ok()
            .flatten()
            .map(|v| v.value)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;

    #[test]
    fn preserves_conditional_write_error_categories() {
        for (conditional_error, write_once_error, source_message) in [
            (
                ConditionalWriteError::TransactionConflict(anyhow!("conflict")),
                WriteOnceError::TransactionConflict(anyhow!("expected")),
                "conflict",
            ),
            (
                ConditionalWriteError::Timeout(anyhow!("timeout")),
                WriteOnceError::Timeout(anyhow!("expected")),
                "timeout",
            ),
            (
                ConditionalWriteError::BackendUnavailable(anyhow!("unavailable")),
                WriteOnceError::BackendUnavailable(anyhow!("expected")),
                "unavailable",
            ),
            (
                ConditionalWriteError::InternalError(anyhow!("internal")),
                WriteOnceError::InternalError(anyhow!("expected")),
                "internal",
            ),
        ] {
            let mapped = map_conditional_write_error(conditional_error);
            assert_eq!(
                std::mem::discriminant(&mapped),
                std::mem::discriminant(&write_once_error),
            );
            assert_eq!(
                std::error::Error::source(&mapped).unwrap().to_string(),
                source_message,
            );
        }
    }
}
