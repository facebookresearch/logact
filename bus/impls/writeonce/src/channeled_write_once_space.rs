/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ChanneledWriteOnceSpace: A channel-based wrapper around any WriteOnceSpace.
//!
//! This allows multiple frontends to share a single WriteOnceSpace backend,
//! enabling testing of contention and retry-on-conflict scenarios.

use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use anyhow::Error as AnyhowError;
use anyhow::anyhow;
use bytes::Bytes;
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;
use futures::channel::mpsc::unbounded;
use futures::channel::oneshot;
use futures::stream::StreamExt;

/// Messages sent to the WriteOnceSpace backend
enum Message {
    Write {
        space_id: String,
        address: u64,
        value: Bytes,
        response_tx: oneshot::Sender<WriteOnceResult<()>>,
    },
    Read {
        space_id: String,
        address: u64,
        response_tx: oneshot::Sender<Option<Bytes>>,
    },
    Tail {
        space_id: String,
        window_size: u64,
        response_tx: oneshot::Sender<TailResult<u64>>,
    },
}

/// ChanneledWriteOnceSpace - frontend handle for accessing a shared WriteOnceSpace.
///
/// This is a lightweight handle that can be cloned. All actual storage
/// is owned by the `ChanneledWriteOnceSpaceBackend` which runs as an async task.
#[derive(Clone)]
pub struct ChanneledWriteOnceSpace {
    tx: UnboundedSender<Message>,
}

/// ChanneledWriteOnceSpaceBackend - background worker that owns the actual WriteOnceSpace.
///
/// Processes write/read requests from multiple frontends.
pub struct ChanneledWriteOnceSpaceBackend<W: WriteOnceSpace> {
    rx: UnboundedReceiver<Message>,
    space: W,
}

impl ChanneledWriteOnceSpace {
    /// Create a new ChanneledWriteOnceSpace frontend/backend pair.
    ///
    /// Returns a tuple of (frontend, backend). The backend must be spawned
    /// as an async task (e.g., `env.spawn(backend.run())`).
    pub fn new<W: WriteOnceSpace>(space: W) -> (Self, ChanneledWriteOnceSpaceBackend<W>) {
        let (tx, rx) = unbounded();
        let frontend = Self { tx };
        let backend = ChanneledWriteOnceSpaceBackend { rx, space };
        (frontend, backend)
    }

    /// Helper for write calls.
    async fn call_write(&self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .unbounded_send(Message::Write {
                space_id: space_id.to_string(),
                address,
                value,
                response_tx,
            })
            .map_err(|e| WriteOnceError::BackendUnavailable(AnyhowError::new(e)))?;

        response_rx
            .await
            .unwrap_or(Err(WriteOnceError::BackendUnavailable(anyhow!(
                "channel closed"
            ))))
    }

    /// Helper for read calls.
    async fn call_read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .tx
            .unbounded_send(Message::Read {
                space_id: space_id.to_string(),
                address,
                response_tx,
            })
            .is_err()
        {
            return None;
        }

        response_rx.await.unwrap_or(None)
    }

    /// Helper for tail calls.
    async fn call_tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .unbounded_send(Message::Tail {
                space_id: space_id.to_string(),
                window_size,
                response_tx,
            })
            .map_err(|e| TailError::BackendUnavailable(e.to_string()))?;

        response_rx
            .await
            .unwrap_or(Err(TailError::BackendUnavailable(
                "channel closed".to_string(),
            )))
    }
}

impl TailableSpace for ChanneledWriteOnceSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        self.call_tail(space_id, window_size).await
    }
}

impl WriteOnceSpace for ChanneledWriteOnceSpace {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        self.call_write(space_id, address, value).await
    }

    async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        self.call_read(space_id, address).await
    }
}

impl<W: WriteOnceSpace> ChanneledWriteOnceSpaceBackend<W> {
    /// Run the background worker loop.
    ///
    /// Processes incoming requests and delegates to the underlying WriteOnceSpace.
    /// This method consumes self and runs until the channel is closed.
    pub async fn run(mut self) {
        while let Some(message) = self.rx.next().await {
            match message {
                Message::Write {
                    space_id,
                    address,
                    value,
                    response_tx,
                } => {
                    let result = self.space.write(&space_id, address, value).await;
                    let _ = response_tx.send(result);
                }
                Message::Read {
                    space_id,
                    address,
                    response_tx,
                } => {
                    let result = self.space.read(&space_id, address).await;
                    let _ = response_tx.send(result);
                }
                Message::Tail {
                    space_id,
                    window_size,
                    response_tx,
                } => {
                    let result = self.space.tail(&space_id, window_size).await;
                    let _ = response_tx.send(result);
                }
            }
        }
        tracing::debug!("ChanneledWriteOnceSpaceBackend: channel closed, shutting down");
    }
}
