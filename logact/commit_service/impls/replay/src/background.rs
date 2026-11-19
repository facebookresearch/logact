/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

use agentbus_api::Environment;
use lru::LruCache;

use crate::Checkpoint;
use crate::CheckpointResult;
use crate::CheckpointStore;

const MAX_UNFLUSHED_BUSES: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

/// A checkpoint store over `C` whose saves run in the background on `E`, so a
/// commit returns without waiting for its checkpoint to be saved. `C` keeps the
/// furthest checkpoint, as every [`CheckpointStore`] does, so saves that land
/// late or out of order, from this server or another, can't replace a further
/// one. A load returns the further of `C`'s checkpoint and the last one this
/// store saved whose save hasn't completed.
pub struct BackgroundCheckpoints<C, E> {
    inner: C,
    environment: Rc<E>,
    /// Per bus, the furthest checkpoint whose save hasn't completed. Evicting
    /// one only means a load falls back to storage.
    unflushed: Rc<RefCell<LruCache<String, Checkpoint>>>,
}

impl<C, E> BackgroundCheckpoints<C, E> {
    pub fn new(inner: C, environment: Rc<E>) -> Self {
        Self {
            inner,
            environment,
            unflushed: Rc::new(RefCell::new(LruCache::new(MAX_UNFLUSHED_BUSES))),
        }
    }
}

impl<C: Clone, E> Clone for BackgroundCheckpoints<C, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            environment: self.environment.clone(),
            unflushed: self.unflushed.clone(),
        }
    }
}

impl<C, E> CheckpointStore for BackgroundCheckpoints<C, E>
where
    C: CheckpointStore + Clone + 'static,
    E: Environment,
{
    async fn load(&self, bus_id: &str) -> CheckpointResult<Option<Checkpoint>> {
        let stored = self.inner.load(bus_id).await?;
        let unflushed = self.unflushed.borrow().peek(bus_id).cloned();
        Ok(stored
            .into_iter()
            .chain(unflushed)
            .max_by_key(|checkpoint| checkpoint.position))
    }

    async fn save(&self, bus_id: &str, checkpoint: &Checkpoint) -> CheckpointResult<()> {
        let position = checkpoint.position;
        {
            let mut unflushed = self.unflushed.borrow_mut();
            if unflushed
                .peek(bus_id)
                .is_some_and(|pending| pending.position >= position)
            {
                return Ok(());
            }
            unflushed.put(bus_id.to_owned(), checkpoint.clone());
        }
        let inner = self.inner.clone();
        let unflushed = self.unflushed.clone();
        let bus_id = bus_id.to_owned();
        let checkpoint = checkpoint.clone();
        self.environment.spawn_local(async move {
            if let Err(error) = inner.save(&bus_id, &checkpoint).await {
                tracing::warn!(bus_id, position, error = %error, "saving checkpoint failed");
            }
            let mut unflushed = unflushed.borrow_mut();
            if unflushed
                .peek(&bus_id)
                .is_some_and(|pending| pending.position == position)
            {
                unflushed.pop(&bus_id);
            }
        });
        Ok(())
    }
}
