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
use bytes::Bytes;
use lru::LruCache;

use crate::Storage;
use crate::StorageResult;

const MAX_ENTRY_VALUE_BYTES: usize = 64 * 1024;

/// TTL and size limits for acknowledged-write caching.
pub struct CachingStorageConfig {
    /// Freshness bound measured from the start of a write.
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
    fn get(&mut self, key: &str) -> Option<CachedValue> {
        let removed = self.values.pop(key)?;
        self.value_bytes -= removed.value.len();
        Some(removed)
    }

    fn insert(&mut self, key: &str, value: CachedValue) {
        if self
            .values
            .peek(key)
            .is_some_and(|cached| cached.position >= value.position)
        {
            return;
        }
        self.get(key);
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

/// Caches acknowledged writes for one optimistic engine read. Reads consume
/// cached values and do not populate the cache. Writes always reach storage and
/// invalidate unconfirmed values.
/// Callers must tolerate stale reads until expiry; engine replay requires available
/// log history after the cached checkpoint.
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
        let cached = self.cache.borrow_mut().get(key);
        if let Some(cached) = cached
            && !self.is_expired(&cached)
        {
            return Ok(Some((cached.value, cached.position)));
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
        self.cache.borrow_mut().get(key);
        let expires_at =
            self.environment.with_clock(Clock::monotonic_time) + self.cache.borrow().config.ttl;
        let result = self
            .inner
            .put(key, value.clone(), expected, new_position)
            .await;
        let mut cache = self.cache.borrow_mut();
        match &result {
            Ok(true) => cache.insert(
                key,
                CachedValue {
                    value,
                    position: new_position,
                    expires_at,
                },
            ),
            _ => {
                cache.get(key);
            }
        }
        result
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
    }

    #[test]
    fn delayed_write_response_does_not_replace_newer_value() {
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
            let old = storage.put("key", Bytes::from_static(b"old"), None, 10);
            let new = async {
                assert!(
                    storage
                        .put("key", Bytes::from_static(b"new"), Some(10), 11)
                        .await
                        .unwrap()
                );
                release.send(()).unwrap();
            };
            let (old, ()) = futures::join!(old, new);
            assert!(old.unwrap());
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from_static(b"new"), 11))
            );
        });
    }

    #[test]
    fn cancelled_write_leaves_a_cache_miss() {
        block_on(async {
            let storage = CachingStorage::new(
                Rc::new(DelayedWriteStorage {
                    inner: InMemoryStorage::new(),
                    release: RefCell::new(None),
                }),
                Rc::new(Simulator::new(0)),
                CACHE_CONFIG,
            );
            assert!(storage.put("key", Bytes::new(), None, 0).await.unwrap());
            let (_release, wait) = oneshot::channel();
            storage.inner.release.replace(Some(wait));
            {
                let write = storage.put("key", Bytes::new(), Some(0), 1);
                futures::pin_mut!(write);
                assert!(matches!(futures::poll!(write.as_mut()), Poll::Pending));
            }
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 1);
        });
    }

    #[test]
    fn rejected_write_invalidates_and_reads_do_not_populate() {
        block_on(async {
            let inner = Rc::new(InMemoryStorage::new());
            let storage =
                CachingStorage::new(inner.clone(), Rc::new(Simulator::new(0)), CACHE_CONFIG);
            assert!(storage.put("key", Bytes::new(), None, 0).await.unwrap());
            assert!(inner.put("key", Bytes::new(), Some(0), 1).await.unwrap());
            assert!(!storage.put("key", Bytes::new(), Some(0), 2).await.unwrap());
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 1);
            assert!(inner.put("key", Bytes::new(), Some(1), 2).await.unwrap());
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 2);
        });
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
            assert!(storage.put("a", value.clone(), None, 0).await.unwrap());
            assert!(storage.put("b", value.clone(), None, 0).await.unwrap());
            assert!(storage.put("c", value, None, 0).await.unwrap());
            assert!(storage.cache.borrow().values.peek("a").is_none());
            assert!(storage.cache.borrow().values.peek("b").is_some());
            assert_eq!(storage.cache.borrow().values.len(), 2);
            assert_eq!(storage.cache.borrow().value_bytes, 2);

            let large = Bytes::from(vec![0; MAX_ENTRY_VALUE_BYTES + 1]);
            assert!(storage.put("b", large.clone(), Some(0), 1).await.unwrap());
            assert!(storage.cache.borrow().values.peek("b").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 1);
            assert_eq!(storage.get("b").await.unwrap(), Some((large, 1)));
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
                    .put("a", Bytes::from_static(b"aa"), None, 0)
                    .await
                    .unwrap()
            );
            assert!(
                storage
                    .put("b", Bytes::from_static(b"bb"), None, 0)
                    .await
                    .unwrap()
            );
            assert!(
                storage
                    .put("c", Bytes::from_static(b"ccc"), None, 0)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.peek("a").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 5);
            assert!(
                storage
                    .put("c", Bytes::from_static(b"cccc"), Some(0), 1)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.peek("b").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 4);

            let oversized = Bytes::from_static(b"123456");
            assert!(
                storage
                    .put("large", oversized.clone(), None, 0)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.peek("large").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 4);
            assert!(
                storage
                    .put("c", oversized.clone(), Some(1), 2)
                    .await
                    .unwrap()
            );
            assert!(storage.cache.borrow().values.is_empty());
            assert_eq!(storage.cache.borrow().value_bytes, 0);
            assert_eq!(storage.get("c").await.unwrap(), Some((oversized, 2)));

            assert!(
                storage
                    .put("c", Bytes::from_static(b"c"), Some(2), 3)
                    .await
                    .unwrap()
            );
            assert_eq!(storage.cache.borrow().value_bytes, 1);
            assert!(!storage.put("c", Bytes::new(), Some(2), 4).await.unwrap());
            assert_eq!(storage.cache.borrow().value_bytes, 0);
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
                assert!(storage.put("key", Bytes::new(), None, 0).await.unwrap());
                assert!(storage.cache.borrow().values.is_empty());
                assert_eq!(storage.cache.borrow().value_bytes, 0);
                assert!(inner.put("key", Bytes::new(), Some(0), 1).await.unwrap());
                assert_eq!(storage.get("key").await.unwrap().unwrap().1, 1);
            }
        });
    }

    #[test]
    fn cached_value_is_returned_only_once() {
        block_on(async {
            let inner = Rc::new(InMemoryStorage::new());
            let storage =
                CachingStorage::new(inner.clone(), Rc::new(Simulator::new(0)), CACHE_CONFIG);
            let value = Bytes::from_static(b"cached");
            assert!(storage.put("key", value.clone(), None, 0).await.unwrap());
            assert!(inner.put("key", Bytes::new(), Some(0), 1).await.unwrap());
            assert_eq!(storage.get("key").await.unwrap(), Some((value, 0)));
            assert_eq!(storage.cache.borrow().value_bytes, 0);
            assert_eq!(storage.get("key").await.unwrap(), Some((Bytes::new(), 1)));
            assert!(inner.put("key", Bytes::new(), Some(1), 2).await.unwrap());
            assert_eq!(storage.get("key").await.unwrap(), Some((Bytes::new(), 2)));
        });
    }

    #[test]
    fn entries_expire_and_reads_do_not_populate() {
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
                    .put("key", Bytes::from_static(b"cached"), None, 0)
                    .await
                    .unwrap()
            );
            assert!(inner.put("key", Bytes::new(), Some(0), 1).await.unwrap());

            task_environment.sleep(ttl).await;
            assert_eq!(storage.get("key").await.unwrap().unwrap().1, 1);
            assert!(storage.cache.borrow().values.peek("key").is_none());
            assert_eq!(storage.cache.borrow().value_bytes, 0);

            assert!(
                storage
                    .put("key", Bytes::from_static(b"cached"), Some(1), 2)
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
            let write = storage.put("key", Bytes::new(), None, 0);
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
            assert!(storage.cache.borrow().values.peek("key").is_none());
        });
        environment.run();
        block_on(handle).expect("delayed write simulation should complete");
    }
}
