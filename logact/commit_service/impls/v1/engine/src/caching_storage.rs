/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::Environment;
use agentbus_api::environment::Clock;
use agentbus_api::environment::MonotonicInstant;
use anyhow::anyhow;
use bytes::Bytes;
use lru::LruCache;

use crate::Storage;
use crate::StorageError;
use crate::StorageResult;

const MAX_ENTRY_VALUE_BYTES: usize = 64 * 1024;

/// TTL and size limits for local checkpoint caching.
pub struct CachingStorageConfig {
    /// Freshness bound measured from the start of a write or backing read.
    pub ttl: Duration,
    /// Maximum cached entries. Zero disables caching.
    pub max_entries: usize,
    /// Maximum cached value bytes, excluding keys and metadata. Zero disables caching.
    pub max_value_bytes: usize,
}

struct CachedValue {
    value: Bytes,
    position: i64,
    expires_at: MonotonicInstant,
}

struct Cache {
    config: CachingStorageConfig,
    values: LruCache<String, CachedValue>,
    value_bytes: usize,
}

impl Cache {
    fn pop(&mut self, key: &str) -> Option<CachedValue> {
        let removed = self.values.pop(key)?;
        self.value_bytes -= removed.value.len();
        Some(removed)
    }

    fn insert_if_newer(&mut self, key: &str, value: CachedValue) {
        if self
            .values
            .peek(key)
            .is_some_and(|cached| cached.position >= value.position)
        {
            return;
        }
        self.pop(key);
        if self.config.max_entries == 0
            || self.config.max_value_bytes == 0
            || value.value.len() > self.config.max_value_bytes
            || value.value.len() > MAX_ENTRY_VALUE_BYTES
        {
            return;
        }
        while value.value.len() > self.config.max_value_bytes.saturating_sub(self.value_bytes) {
            let Some((_, removed)) = self.values.pop_lru() else {
                return;
            };
            self.value_bytes -= removed.value.len();
        }
        self.value_bytes += value.value.len();
        if let Some((_, removed)) = self.values.push(key.to_owned(), value) {
            self.value_bytes -= removed.value.len();
        }
    }
}

/// Caches checkpoints submitted through `put_if_newer`, before awaiting storage,
/// and checkpoints returned by backing reads. Cache hits retain values without
/// renewing their TTL. `put` is unsupported; CAS callers must use uncached storage.
///
/// Write rejection, failure or cancellation leaves the submitted checkpoint
/// cached until expiry or eviction. Callers must tolerate stale and unpersisted
/// checkpoints and retain the log history needed to replay forward from them.
pub struct CachingStorage<S, E> {
    inner: Rc<S>,
    environment: Rc<E>,
    cache: RefCell<Cache>,
}

impl<S, E> CachingStorage<S, E> {
    /// Wrap storage with the supplied TTL and LRU limits. Either size limit being
    /// zero disables caching; values above 64 KiB bypass the cache.
    pub fn new(inner: Rc<S>, environment: Rc<E>, config: CachingStorageConfig) -> Self {
        let values =
            LruCache::new(NonZeroUsize::new(config.max_entries).unwrap_or(NonZeroUsize::MIN));
        Self {
            inner,
            environment,
            cache: RefCell::new(Cache {
                config,
                values,
                value_bytes: 0,
            }),
        }
    }

    fn is_expired(&self, cached: &CachedValue) -> bool
    where
        E: Environment,
    {
        self.environment.with_clock(Clock::monotonic_time) >= cached.expires_at
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage, E: Environment> Storage for CachingStorage<S, E> {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        {
            let mut cache = self.cache.borrow_mut();
            if let Some(cached) = cache.values.get(key) {
                if !self.is_expired(cached) {
                    return Ok(Some((cached.value.clone(), cached.position)));
                }
                cache.pop(key);
            }
        }
        let expires_at =
            self.environment.with_clock(Clock::monotonic_time) + self.cache.borrow().config.ttl;
        let stored = self.inner.get(key).await?;
        let mut cache = self.cache.borrow_mut();
        if let Some((value, position)) = &stored {
            cache.insert_if_newer(
                key,
                CachedValue {
                    value: value.clone(),
                    position: *position,
                    expires_at,
                },
            );
        }
        if let Some(cached) = cache.values.get(key)
            && !self.is_expired(cached)
        {
            return Ok(Some((cached.value.clone(), cached.position)));
        }
        Ok(stored)
    }

    async fn put(
        &self,
        _key: &str,
        _value: Bytes,
        _expected: Option<i64>,
        _new_position: i64,
    ) -> StorageResult<bool> {
        Err(StorageError::InternalError(anyhow!(
            "CachingStorage::put is unsupported; use put_if_newer"
        )))
    }

    async fn put_if_newer(
        &self,
        key: &str,
        value: Bytes,
        new_position: i64,
    ) -> StorageResult<bool> {
        if new_position < 0 {
            return Err(StorageError::InvalidPosition(new_position));
        }
        let expires_at =
            self.environment.with_clock(Clock::monotonic_time) + self.cache.borrow().config.ttl;
        self.cache.borrow_mut().insert_if_newer(
            key,
            CachedValue {
                value: value.clone(),
                position: new_position,
                expires_at,
            },
        );
        self.inner.put_if_newer(key, value, new_position).await
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use agentbus_simulator::Simulator;
    use futures::channel::oneshot;
    use futures::executor::block_on;

    use super::*;
    use crate::InMemoryStorage;

    const CACHE_TTL: Duration = Duration::from_secs(60);
    const CACHE_CONFIG: CachingStorageConfig = CachingStorageConfig {
        ttl: CACHE_TTL,
        max_entries: 128,
        max_value_bytes: 8 * 1024 * 1024,
    };

    #[test]
    fn rejected_put_if_newer_retains_submitted_checkpoint() {
        block_on(async {
            let inner = Rc::new(InMemoryStorage::new());
            let storage =
                CachingStorage::new(inner.clone(), Rc::new(Simulator::new(0)), CACHE_CONFIG);
            assert!(
                storage
                    .put_if_newer("key", Bytes::from("checkpoint10"), 10)
                    .await
                    .unwrap()
            );
            assert!(
                inner
                    .put_if_newer("key", Bytes::from("checkpoint20"), 20)
                    .await
                    .unwrap()
            );
            assert!(
                !storage
                    .put_if_newer("key", Bytes::from("checkpoint15"), 15)
                    .await
                    .unwrap()
            );
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint15"), 15))
            );
            assert_eq!(
                inner.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint20"), 20))
            );
            assert!(
                !storage
                    .put_if_newer("key", Bytes::from("checkpoint20"), 20)
                    .await
                    .unwrap()
            );
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint20"), 20))
            );
            assert!(
                !storage
                    .put_if_newer("key", Bytes::from("duplicate"), 20)
                    .await
                    .unwrap()
            );
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint20"), 20))
            );
        });
    }

    #[test]
    fn put_is_unsupported_without_cache_or_storage_mutation() {
        block_on(async {
            let inner = Rc::new(InMemoryStorage::new());
            let storage =
                CachingStorage::new(inner.clone(), Rc::new(Simulator::new(0)), CACHE_CONFIG);
            assert!(
                storage
                    .put_if_newer("key", Bytes::from("checkpoint"), 10)
                    .await
                    .unwrap()
            );
            for key in ["key", "absent"] {
                assert!(matches!(
                    storage.put(key, Bytes::from("cas"), None, 20).await,
                    Err(StorageError::InternalError(_))
                ));
            }
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint"), 10))
            );
            assert_eq!(
                inner.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint"), 10))
            );
            assert_eq!(inner.get("absent").await.unwrap(), None);
        });
    }

    #[test]
    fn negative_position_does_not_poison_the_cache() {
        block_on(async {
            let storage = CachingStorage::new(
                Rc::new(InMemoryStorage::new()),
                Rc::new(Simulator::new(0)),
                CACHE_CONFIG,
            );
            assert!(matches!(
                storage
                    .put_if_newer("absent", Bytes::from("invalid"), -1)
                    .await,
                Err(StorageError::InvalidPosition(-1))
            ));
            assert_eq!(storage.get("absent").await.unwrap(), None);
            assert!(storage.cache.borrow().values.is_empty());
        });
    }

    struct DelayedBeforeWriteStorage {
        inner: InMemoryStorage,
        release: RefCell<Option<oneshot::Receiver<()>>>,
        failure: RefCell<Option<StorageError>>,
    }

    #[async_trait::async_trait(?Send)]
    impl Storage for DelayedBeforeWriteStorage {
        async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
            self.inner.get(key).await
        }

        async fn put(
            &self,
            key: &str,
            value: Bytes,
            expected: Option<i64>,
            new_position: i64,
        ) -> StorageResult<bool> {
            self.inner.put(key, value, expected, new_position).await
        }

        async fn put_if_newer(
            &self,
            key: &str,
            value: Bytes,
            new_position: i64,
        ) -> StorageResult<bool> {
            let release = self.release.borrow_mut().take();
            let failure = self.failure.borrow_mut().take();
            if let Some(release) = release {
                release.await.expect("test should release the write");
            }
            if let Some(failure) = failure {
                return Err(failure);
            }
            self.inner.put_if_newer(key, value, new_position).await
        }
    }

    #[test]
    fn cancelled_before_storage_write_retains_submitted_checkpoint_until_expiry() {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            let (_release, wait) = oneshot::channel();
            let inner = Rc::new(DelayedBeforeWriteStorage {
                inner: InMemoryStorage::new(),
                release: RefCell::new(Some(wait)),
                failure: RefCell::new(None),
            });
            let storage =
                CachingStorage::new(inner.clone(), task_environment.clone(), CACHE_CONFIG);
            {
                let write = storage.put_if_newer("key", Bytes::from("checkpoint"), 10);
                futures::pin_mut!(write);
                assert!(matches!(futures::poll!(write.as_mut()), Poll::Pending));
                assert_eq!(inner.get("key").await.unwrap(), None);
                assert_eq!(
                    storage.get("key").await.unwrap(),
                    Some((Bytes::from("checkpoint"), 10))
                );
            }
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint"), 10))
            );
            task_environment.sleep(CACHE_TTL).await;
            assert_eq!(storage.get("key").await.unwrap(), None);
            assert_eq!(storage.cache.borrow().value_bytes, 0);
        });
        environment.run();
        block_on(handle).expect("cancelled-write simulation should complete");
    }

    #[test]
    fn older_put_if_newer_preserves_newer_cached_checkpoint() {
        block_on(async {
            let inner = Rc::new(DelayedBeforeWriteStorage {
                inner: InMemoryStorage::new(),
                release: RefCell::new(None),
                failure: RefCell::new(None),
            });
            let storage =
                CachingStorage::new(inner.clone(), Rc::new(Simulator::new(0)), CACHE_CONFIG);
            assert!(
                storage
                    .put_if_newer("key", Bytes::from("newer"), 20)
                    .await
                    .unwrap()
            );
            let (release, wait) = oneshot::channel();
            inner.release.replace(Some(wait));
            let older = storage.put_if_newer("key", Bytes::from("older"), 10);
            futures::pin_mut!(older);
            assert!(matches!(futures::poll!(older.as_mut()), Poll::Pending));
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("newer"), 20)),
                "an older submission must not regress the cached checkpoint while awaiting storage"
            );
            release.send(()).unwrap();
            assert!(!older.await.unwrap());
            assert_eq!(
                storage.cache.borrow().values.peek("key").unwrap().position,
                20,
                "rejecting the older write must preserve the newer cache entry"
            );
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("newer"), 20))
            );
        });
    }

    #[test]
    fn rejected_or_failed_old_write_does_not_invalidate_newer_checkpoint() {
        block_on(async {
            for failure in [
                None,
                Some(StorageError::Timeout(anyhow::anyhow!("injected timeout"))),
            ] {
                let (release, wait) = oneshot::channel();
                let storage = CachingStorage::new(
                    Rc::new(DelayedBeforeWriteStorage {
                        inner: InMemoryStorage::new(),
                        release: RefCell::new(Some(wait)),
                        failure: RefCell::new(failure),
                    }),
                    Rc::new(Simulator::new(0)),
                    CACHE_CONFIG,
                );
                let old = storage.put_if_newer("key", Bytes::from("old"), 10);
                futures::pin_mut!(old);
                assert!(matches!(futures::poll!(old.as_mut()), Poll::Pending));
                assert!(
                    storage
                        .put_if_newer("key", Bytes::from("new"), 20)
                        .await
                        .unwrap()
                );
                release.send(()).unwrap();
                assert!(!matches!(old.await, Ok(true)));
                assert_eq!(
                    storage.get("key").await.unwrap(),
                    Some((Bytes::from("new"), 20))
                );
                assert_eq!(
                    storage.cache.borrow().values.peek("key").unwrap().position,
                    20
                );
            }
        });
    }

    #[test]
    fn failed_write_retains_submitted_checkpoint() {
        block_on(async {
            let storage = CachingStorage::new(
                Rc::new(DelayedBeforeWriteStorage {
                    inner: InMemoryStorage::new(),
                    release: RefCell::new(None),
                    failure: RefCell::new(Some(StorageError::BackendUnavailable(anyhow::anyhow!(
                        "injected failure"
                    )))),
                }),
                Rc::new(Simulator::new(0)),
                CACHE_CONFIG,
            );
            assert!(matches!(
                storage
                    .put_if_newer("key", Bytes::from("checkpoint"), 10)
                    .await,
                Err(StorageError::BackendUnavailable(_))
            ));
            assert_eq!(storage.inner.get("key").await.unwrap(), None);
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("checkpoint"), 10))
            );
            assert_eq!(
                storage.cache.borrow().values.peek("key").unwrap().position,
                10
            );
            assert_eq!(storage.cache.borrow().value_bytes, "checkpoint".len());
        });
    }

    #[test]
    fn backing_reads_populate_without_hit_renewal() {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            let inner = Rc::new(InMemoryStorage::new());
            assert!(
                inner
                    .put_if_newer("key", Bytes::from("old"), 10)
                    .await
                    .unwrap()
            );
            let storage =
                CachingStorage::new(inner.clone(), task_environment.clone(), CACHE_CONFIG);
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("old"), 10))
            );
            assert!(
                inner
                    .put_if_newer("key", Bytes::from("new"), 20)
                    .await
                    .unwrap()
            );
            task_environment
                .sleep(CACHE_TTL - Duration::from_secs(1))
                .await;
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("old"), 10))
            );
            task_environment.sleep(Duration::from_secs(1)).await;
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20))
            );
            assert_eq!(
                storage.cache.borrow().values.peek("key").unwrap().position,
                20
            );
        });
        environment.run();
        block_on(handle).expect("read-through expiry simulation should complete");
    }

    struct DelayedReadStorage {
        inner: Rc<InMemoryStorage>,
        release: RefCell<Option<oneshot::Receiver<()>>>,
    }

    #[async_trait::async_trait(?Send)]
    impl Storage for DelayedReadStorage {
        async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
            let stored = self.inner.get(key).await;
            let release = self.release.borrow_mut().take();
            if let Some(release) = release {
                release.await.expect("test should release the read");
            }
            stored
        }
        async fn put(
            &self,
            key: &str,
            value: Bytes,
            expected: Option<i64>,
            new_position: i64,
        ) -> StorageResult<bool> {
            self.inner.put(key, value, expected, new_position).await
        }
        async fn put_if_newer(
            &self,
            key: &str,
            value: Bytes,
            new_position: i64,
        ) -> StorageResult<bool> {
            self.inner.put_if_newer(key, value, new_position).await
        }
    }

    #[test]
    fn late_backing_read_preserves_and_returns_newer_local_checkpoint() {
        block_on(async {
            for exists in [false, true] {
                let inner = Rc::new(InMemoryStorage::new());
                if exists {
                    assert!(
                        inner
                            .put_if_newer("key", Bytes::from("old"), 10)
                            .await
                            .unwrap()
                    );
                }
                let (release, wait) = oneshot::channel();
                let storage = CachingStorage::new(
                    Rc::new(DelayedReadStorage {
                        inner,
                        release: RefCell::new(Some(wait)),
                    }),
                    Rc::new(Simulator::new(0)),
                    CACHE_CONFIG,
                );
                let read = storage.get("key");
                futures::pin_mut!(read);
                assert!(matches!(futures::poll!(read.as_mut()), Poll::Pending));
                assert!(
                    storage
                        .put_if_newer("key", Bytes::from("new"), 20)
                        .await
                        .unwrap()
                );
                release.send(()).unwrap();
                assert_eq!(
                    read.await.unwrap(),
                    Some((Bytes::from("new"), 20)),
                    "a late older or missing snapshot must not hide a newer local checkpoint"
                );
                assert_eq!(
                    storage.cache.borrow().values.peek("key").unwrap().position,
                    20
                );
            }
        });
    }

    #[test]
    fn delayed_read_response_does_not_restart_expiry() {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            let inner = Rc::new(InMemoryStorage::new());
            assert!(
                inner
                    .put_if_newer("key", Bytes::from("old"), 10)
                    .await
                    .unwrap()
            );
            let (release, wait) = oneshot::channel();
            let storage = CachingStorage::new(
                Rc::new(DelayedReadStorage {
                    inner: inner.clone(),
                    release: RefCell::new(Some(wait)),
                }),
                task_environment.clone(),
                CACHE_CONFIG,
            );
            let read = storage.get("key");
            let delay = async {
                task_environment.sleep(CACHE_TTL).await;
                assert!(
                    inner
                        .put_if_newer("key", Bytes::from("new"), 20)
                        .await
                        .unwrap()
                );
                release.send(()).unwrap();
            };
            let (read, ()) = futures::join!(read, delay);
            assert_eq!(read.unwrap(), Some((Bytes::from("old"), 10)));
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20))
            );
        });
        environment.run();
        block_on(handle).expect("delayed read simulation should complete");
    }

    struct DelayedWriteStorage {
        inner: InMemoryStorage,
        release: RefCell<Option<oneshot::Receiver<()>>>,
    }

    #[async_trait::async_trait(?Send)]
    impl Storage for DelayedWriteStorage {
        async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
            self.inner.get(key).await
        }

        async fn put(
            &self,
            key: &str,
            value: Bytes,
            expected: Option<i64>,
            new_position: i64,
        ) -> StorageResult<bool> {
            let result = self.inner.put(key, value, expected, new_position).await;
            let release = self.release.borrow_mut().take();
            if let Some(release) = release {
                release
                    .await
                    .expect("test should release the write response");
            }
            result
        }

        async fn put_if_newer(
            &self,
            key: &str,
            value: Bytes,
            new_position: i64,
        ) -> StorageResult<bool> {
            let result = self.inner.put_if_newer(key, value, new_position).await;
            let release = self.release.borrow_mut().take();
            if let Some(release) = release {
                release
                    .await
                    .expect("test should release the write response");
            }
            result
        }
    }

    #[test]
    fn delayed_put_if_newer_response_preserves_newer_cached_value() {
        block_on(async {
            let (release, wait) = oneshot::channel();
            let storage = CachingStorage::new(
                Rc::new(DelayedWriteStorage {
                    inner: InMemoryStorage::new(),
                    release: RefCell::new(Some(wait)),
                }),
                Rc::new(Simulator::new(0)),
                CACHE_CONFIG,
            );
            let old = storage.put_if_newer("key", Bytes::from("old"), 10);
            let new = async {
                assert!(
                    storage
                        .put_if_newer("key", Bytes::from("new"), 20)
                        .await
                        .unwrap()
                );
                release.send(()).unwrap();
            };
            let (old, ()) = futures::join!(old, new);
            assert!(old.unwrap());
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20))
            );
        });
    }

    #[test]
    fn in_flight_put_if_newer_exposes_submitted_value() {
        block_on(async {
            let storage = CachingStorage::new(
                Rc::new(DelayedWriteStorage {
                    inner: InMemoryStorage::new(),
                    release: RefCell::new(None),
                }),
                Rc::new(Simulator::new(0)),
                CACHE_CONFIG,
            );
            assert!(
                storage
                    .put_if_newer("key", Bytes::from("old"), 10)
                    .await
                    .unwrap()
            );
            let (release, wait) = oneshot::channel();
            storage.inner.release.replace(Some(wait));
            let write = storage.put_if_newer("key", Bytes::from("new"), 20);
            futures::pin_mut!(write);
            assert!(matches!(futures::poll!(write.as_mut()), Poll::Pending));
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20)),
                "an in-flight save should expose the submitted checkpoint"
            );
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20)),
                "another read should retain the submitted checkpoint"
            );
            release.send(()).unwrap();
            assert!(write.await.unwrap());
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20))
            );
        });
    }

    #[test]
    fn cancelled_put_if_newer_retains_submitted_value_until_expiry() {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            let inner = Rc::new(DelayedWriteStorage {
                inner: InMemoryStorage::new(),
                release: RefCell::new(None),
            });
            let storage =
                CachingStorage::new(inner.clone(), task_environment.clone(), CACHE_CONFIG);
            assert!(
                storage
                    .put_if_newer("key", Bytes::from("old"), 10)
                    .await
                    .unwrap()
            );
            let (_release, wait) = oneshot::channel();
            inner.release.replace(Some(wait));
            {
                let write = storage.put_if_newer("key", Bytes::from("new"), 20);
                futures::pin_mut!(write);
                assert!(matches!(futures::poll!(write.as_mut()), Poll::Pending));
            }
            assert_eq!(
                inner.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20))
            );
            for _ in 0..2 {
                assert_eq!(
                    storage.get("key").await.unwrap(),
                    Some((Bytes::from("new"), 20))
                );
            }
            task_environment.sleep(CACHE_TTL).await;
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from("new"), 20))
            );
            assert_eq!(storage.cache.borrow().value_bytes, "new".len());
        });
        environment.run();
        block_on(handle).expect("cancelled-save simulation should complete");
    }

    #[test]
    fn cache_evicts_and_bypasses_large_values() {
        block_on(async {
            let storage = CachingStorage::new(
                Rc::new(InMemoryStorage::new()),
                Rc::new(Simulator::new(0)),
                CachingStorageConfig {
                    max_entries: 2,
                    ..CACHE_CONFIG
                },
            );
            let value = Bytes::from_static(b"a");
            assert!(storage.put_if_newer("a", value.clone(), 0).await.unwrap());
            assert!(storage.put_if_newer("b", value.clone(), 0).await.unwrap());
            assert_eq!(storage.get("a").await.unwrap(), Some((value.clone(), 0)));
            assert!(storage.put_if_newer("c", value, 0).await.unwrap());
            assert!(storage.cache.borrow().values.peek("b").is_none());
            assert!(storage.cache.borrow().values.peek("a").is_some());
            assert_eq!(storage.cache.borrow().values.len(), 2);
            assert_eq!(storage.cache.borrow().value_bytes, 2);

            let large = Bytes::from(vec![0; MAX_ENTRY_VALUE_BYTES + 1]);
            assert!(storage.put_if_newer("a", large.clone(), 1).await.unwrap());
            assert!(storage.cache.borrow().values.peek("a").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 1);
            assert_eq!(storage.get("a").await.unwrap(), Some((large, 1)));
        });
    }

    #[test]
    fn total_byte_limit_evicts_and_tracks_replacements() {
        block_on(async {
            let storage = CachingStorage::new(
                Rc::new(InMemoryStorage::new()),
                Rc::new(Simulator::new(0)),
                CachingStorageConfig {
                    max_entries: 10,
                    max_value_bytes: 5,
                    ..CACHE_CONFIG
                },
            );
            assert!(
                storage
                    .put_if_newer("a", Bytes::from_static(b"aa"), 0)
                    .await
                    .unwrap()
            );
            assert!(
                storage
                    .put_if_newer("b", Bytes::from_static(b"bb"), 0)
                    .await
                    .unwrap()
            );
            assert!(
                storage
                    .put_if_newer("c", Bytes::from_static(b"ccc"), 0)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.peek("a").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 5);
            assert!(
                storage
                    .put_if_newer("c", Bytes::from_static(b"cccc"), 1)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.peek("b").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 4);

            let oversized = Bytes::from_static(b"123456");
            assert!(
                storage
                    .put_if_newer("large", oversized.clone(), 0)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.peek("large").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 4);
            assert!(
                storage
                    .put_if_newer("c", oversized.clone(), 2)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.is_empty());
            assert_eq!(storage.cache.borrow().value_bytes, 0);
            assert_eq!(storage.get("c").await.unwrap(), Some((oversized, 2)));

            assert!(
                storage
                    .put_if_newer("c", Bytes::from_static(b"c"), 3)
                    .await
                    .unwrap()
            );
            assert_eq!(storage.cache.borrow().value_bytes, 1);
            assert!(!storage.put_if_newer("c", Bytes::new(), 3).await.unwrap());
            assert_eq!(storage.cache.borrow().value_bytes, 1);
        });
    }

    #[test]
    fn either_zero_limit_disables_caching() {
        block_on(async {
            for (max_entries, max_value_bytes) in [(0, 5), (2, 0)] {
                let inner = Rc::new(InMemoryStorage::new());
                let storage = CachingStorage::new(
                    inner.clone(),
                    Rc::new(Simulator::new(0)),
                    CachingStorageConfig {
                        max_entries,
                        max_value_bytes,
                        ..CACHE_CONFIG
                    },
                );
                assert!(storage.put_if_newer("key", Bytes::new(), 0).await.unwrap());
                assert!(storage.cache.borrow().values.is_empty());
                assert_eq!(storage.cache.borrow().value_bytes, 0);
                assert!(inner.put("key", Bytes::new(), Some(0), 1).await.unwrap());
                assert_eq!(storage.get("key").await.unwrap().unwrap().1, 1);
            }
        });
    }

    #[test]
    fn reads_retain_values_after_equal_save_rejection() {
        block_on(async {
            let inner = Rc::new(InMemoryStorage::new());
            let storage =
                CachingStorage::new(inner.clone(), Rc::new(Simulator::new(0)), CACHE_CONFIG);
            let value = Bytes::from_static(b"cached");
            assert!(storage.put_if_newer("key", value.clone(), 0).await.unwrap());
            assert!(inner.put("key", Bytes::new(), Some(0), 1).await.unwrap());
            for _ in 0..3 {
                assert_eq!(storage.get("key").await.unwrap(), Some((value.clone(), 0)));
                assert_eq!(storage.cache.borrow().value_bytes, value.len());
            }
            assert!(!storage.put_if_newer("key", Bytes::new(), 1).await.unwrap());
            assert_eq!(storage.cache.borrow().value_bytes, 0);
            assert_eq!(storage.get("key").await.unwrap(), Some((Bytes::new(), 1)));
            assert!(inner.put("key", Bytes::new(), Some(1), 2).await.unwrap());
            assert_eq!(storage.get("key").await.unwrap(), Some((Bytes::new(), 1)));
        });
    }

    #[test]
    fn entries_expire_without_hit_renewal_and_refresh_from_reads() {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            let inner = Rc::new(InMemoryStorage::new());
            let ttl = Duration::from_secs(7);
            let storage = CachingStorage::new(
                inner.clone(),
                task_environment.clone(),
                CachingStorageConfig {
                    ttl,
                    ..CACHE_CONFIG
                },
            );
            assert!(
                storage
                    .put_if_newer("key", Bytes::from_static(b"cached"), 0)
                    .await
                    .unwrap()
            );
            assert!(inner.put("key", Bytes::new(), Some(0), 1).await.unwrap());

            task_environment.sleep(ttl).await;
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 1);
            assert_eq!(
                storage.cache.borrow().values.peek("key").unwrap().position,
                1
            );
            assert_eq!(storage.cache.borrow().value_bytes, 0);

            assert!(
                storage
                    .put_if_newer("key", Bytes::from_static(b"cached"), 2)
                    .await
                    .unwrap()
            );
            assert!(inner.put("key", Bytes::new(), Some(2), 3).await.unwrap());
            task_environment.sleep(ttl - Duration::from_secs(1)).await;
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 2);
            task_environment.sleep(Duration::from_secs(1)).await;
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 3);
        });
        environment.run();
        block_on(handle).expect("expiry simulation should complete");
    }

    #[test]
    fn delayed_write_response_does_not_restart_expiry() {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            let (release, wait) = oneshot::channel();
            let inner = Rc::new(DelayedWriteStorage {
                inner: InMemoryStorage::new(),
                release: RefCell::new(Some(wait)),
            });
            let storage =
                CachingStorage::new(inner.clone(), task_environment.clone(), CACHE_CONFIG);
            let write = storage.put_if_newer("key", Bytes::new(), 0);
            let delay = async {
                task_environment.sleep(CACHE_TTL).await;
                assert!(
                    inner
                        .inner
                        .put("key", Bytes::new(), Some(0), 1)
                        .await
                        .unwrap()
                );
                release.send(()).unwrap();
            };
            let (write, ()) = futures::join!(write, delay);
            assert!(write.unwrap());
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 1);
            assert_eq!(
                storage.cache.borrow().values.peek("key").unwrap().position,
                1
            );
        });
        environment.run();
        block_on(handle).expect("delayed write simulation should complete");
    }
}
