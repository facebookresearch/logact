/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::Rc;

use futures_intrusive::sync::LocalMutex;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;
use lru::LruCache;

const MAX_CACHED_BUSES: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

/// Best-effort FIFO serialization of `commit_intention` calls per bus.
///
/// Clones share the service and a bounded LRU cache of bus mutexes.
/// `create_bus` and the AgentBus surface bypass the queues. Dropping a queued or
/// active call releases its place. Eviction can allow overlapping commits on a bus
/// while calls still hold its old mutex.
pub struct BufferingCommitService<S> {
    inner: Rc<S>,
    queue: Rc<CommitQueue>,
}

struct CommitQueue {
    buses: RefCell<LruCache<String, Rc<LocalMutex<()>>>>,
}

impl Default for CommitQueue {
    fn default() -> Self {
        Self {
            buses: RefCell::new(LruCache::new(MAX_CACHED_BUSES)),
        }
    }
}

impl<S> Clone for BufferingCommitService<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            queue: self.queue.clone(),
        }
    }
}

impl<S> BufferingCommitService<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Rc::new(inner),
            queue: Rc::default(),
        }
    }
}

impl CommitQueue {
    async fn run<F: Future>(&self, bus_id: String, operation: F) -> F::Output {
        let mutex = self
            .buses
            .borrow_mut()
            .get_or_insert(bus_id, || Rc::new(LocalMutex::new((), true)))
            .clone();
        let _guard = mutex.lock().await;
        operation.await
    }
}

impl<S: CommitSvc> CommitSvc for BufferingCommitService<S> {
    type Bus = S::Bus;

    fn agent_bus(&self) -> &Self::Bus {
        self.inner.agent_bus()
    }

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.inner.create_bus(request).await
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.queue
            .run(
                request.bus_id.agent_bus_id.clone(),
                self.inner.commit_intention(request),
            )
            .await
    }
}
