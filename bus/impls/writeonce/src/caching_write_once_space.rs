/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use bytes::Bytes;
use lru::LruCache;

/// Caches confirmed immutable values from a backing write-once space.
///
/// Clones share a bounded LRU cache and clone the backing space, whose clones
/// must address the same storage. Space IDs and addresses must not be reused or
/// deleted while cached values may remain reachable.
#[derive(Clone)]
pub struct CachingWriteOnceSpace<W> {
    inner: W,
    cache: Rc<RefCell<Cache>>,
}

const DEFAULT_MAX_ENTRY_VALUE_BYTES: usize = 64 * 1024;

struct Cache {
    values: LruCache<(String, u64), Bytes>,
    value_bytes: usize,
    max_entries: usize,
    max_value_bytes: usize,
    max_entry_value_bytes: usize,
}

impl Cache {
    fn get(&mut self, space_id: &str, address: u64) -> Option<Bytes> {
        self.values.get(&(space_id.to_owned(), address)).cloned()
    }

    fn insert(&mut self, space_id: &str, address: u64, value: Bytes) {
        let key = (space_id.to_owned(), address);
        if self.max_entries == 0
            || self.max_value_bytes == 0
            || self.max_entry_value_bytes == 0
            || value.len() > self.max_value_bytes
            || value.len() > self.max_entry_value_bytes
            || self.values.contains(&key)
        {
            return;
        }

        while value.len() > self.max_value_bytes.saturating_sub(self.value_bytes) {
            let Some((_, removed)) = self.values.pop_lru() else {
                return;
            };
            self.value_bytes -= removed.len();
        }
        self.value_bytes += value.len();
        if let Some((_, removed)) = self.values.push(key, value) {
            self.value_bytes -= removed.len();
        }
    }
}

impl<W> CachingWriteOnceSpace<W> {
    /// Wrap `inner` with entry-count and total-value-byte limits, and a 64 KiB
    /// per-entry limit. Either supplied limit being zero disables caching.
    /// Cache hits refresh LRU eviction order.
    pub fn new(inner: W, max_entries: usize, max_value_bytes: usize) -> Self {
        Self::new_with_entry_limit(
            inner,
            max_entries,
            max_value_bytes,
            DEFAULT_MAX_ENTRY_VALUE_BYTES,
        )
    }

    /// Wrap `inner` with explicit entry-count, total-value-byte, and per-entry
    /// value-byte limits. Any zero limit disables caching. Oversized values
    /// bypass the cache without evicting existing entries.
    pub fn new_with_entry_limit(
        inner: W,
        max_entries: usize,
        max_value_bytes: usize,
        max_entry_value_bytes: usize,
    ) -> Self {
        Self {
            inner,
            cache: Rc::new(RefCell::new(Cache {
                values: LruCache::new(NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::MIN)),
                value_bytes: 0,
                max_entries,
                max_value_bytes,
                max_entry_value_bytes,
            })),
        }
    }
}

impl<W: TailableSpace> TailableSpace for CachingWriteOnceSpace<W> {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        self.inner.tail(space_id, window_size).await
    }
}

impl<W: WriteOnceSpace> WriteOnceSpace for CachingWriteOnceSpace<W> {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        self.inner.write(space_id, address, value.clone()).await?;
        self.cache.borrow_mut().insert(space_id, address, value);
        Ok(())
    }

    async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        if let Some(value) = self.cache.borrow_mut().get(space_id, address) {
            return Some(value);
        }
        let value = self.inner.read(space_id, address).await?;
        self.cache
            .borrow_mut()
            .insert(space_id, address, value.clone());
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use agentbus_api::TailError;
    use agentbus_api::WriteOnceError;
    use futures::executor::block_on;

    use super::*;
    use crate::InMemoryWriteOnceSpace;

    #[derive(Clone, Default)]
    struct CountingSpace {
        inner: InMemoryWriteOnceSpace,
        reads: Rc<Cell<usize>>,
        writes: Rc<Cell<usize>>,
        tails: Rc<Cell<usize>>,
        fail_next_write: Rc<Cell<Option<bool>>>,
    }

    impl TailableSpace for CountingSpace {
        async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
            self.tails.set(self.tails.get() + 1);
            self.inner.tail(space_id, window_size).await
        }
    }

    impl WriteOnceSpace for CountingSpace {
        async fn write(
            &mut self,
            space_id: &str,
            address: u64,
            value: Bytes,
        ) -> WriteOnceResult<()> {
            self.writes.set(self.writes.get() + 1);
            match self.fail_next_write.take() {
                Some(commit) => {
                    if commit {
                        self.inner.write(space_id, address, value).await?;
                    }
                    Err(WriteOnceError::Timeout(anyhow::anyhow!("injected timeout")))
                }
                None => self.inner.write(space_id, address, value).await,
            }
        }

        async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
            self.reads.set(self.reads.get() + 1);
            self.inner.read(space_id, address).await
        }
    }

    #[test]
    fn clones_share_cached_values_but_new_wrappers_do_not() {
        block_on(async {
            let mut backing = CountingSpace::default();
            let mut space = CachingWriteOnceSpace::new(backing.clone(), 16, 1024);
            space.write("a", 7, Bytes::from("local")).await.unwrap();
            assert_eq!(space.clone().read("a", 7).await, Some(Bytes::from("local")));
            assert_eq!(
                backing.reads.get(),
                0,
                "a confirmed write avoids its first backing read"
            );

            backing.write("a", 6, Bytes::from("remote")).await.unwrap();
            assert_eq!(space.read("a", 6).await, Some(Bytes::from("remote")));
            assert_eq!(
                space.clone().read("a", 6).await,
                Some(Bytes::from("remote"))
            );
            assert_eq!(
                backing.reads.get(),
                1,
                "a positive backing read populates the shared cache"
            );
            assert_eq!(space.read("a", 7).await, Some(Bytes::from("local")));

            let independent = CachingWriteOnceSpace::new(backing.clone(), 16, 1024);
            assert_eq!(independent.read("a", 7).await, Some(Bytes::from("local")));
            assert_eq!(
                backing.reads.get(),
                2,
                "separately constructed wrappers have separate caches"
            );

            assert_eq!(space.tail("a", 8).await.unwrap(), 0);
            assert!(matches!(
                space.tail("a", 0).await,
                Err(TailError::ZeroWindowSize)
            ));
            assert_eq!(
                backing.tails.get(),
                2,
                "tail discovery and errors are delegated"
            );
        });
    }

    #[test]
    fn empty_reads_and_failed_writes_do_not_populate_the_cache() {
        block_on(async {
            let mut backing = CountingSpace::default();
            let mut space = CachingWriteOnceSpace::new(backing.clone(), 16, 1024);
            assert_eq!(space.read("a", 0).await, None);
            assert_eq!(space.read("a", 0).await, None);
            backing.write("a", 0, Bytes::from("winner")).await.unwrap();
            assert!(matches!(
                space.write("a", 0, Bytes::from("loser")).await,
                Err(WriteOnceError::AddressAlreadyExists(0))
            ));
            assert_eq!(space.read("a", 0).await, Some(Bytes::from("winner")));
            assert_eq!(
                backing.reads.get(),
                3,
                "misses and rejected writes cannot hide a later value"
            );
            assert!(matches!(
                space.write("a", 0, Bytes::from("loser")).await,
                Err(WriteOnceError::AddressAlreadyExists(0))
            ));
            assert_eq!(space.read("a", 0).await, Some(Bytes::from("winner")));
            assert_eq!(backing.reads.get(), 3);

            for commit in [false, true] {
                let address = if commit { 9 } else { 8 };
                backing.fail_next_write.set(Some(commit));
                assert!(matches!(
                    space.write("a", address, Bytes::from("uncertain")).await,
                    Err(WriteOnceError::Timeout(_))
                ));
                let reads = backing.reads.get();
                assert_eq!(
                    space.read("a", address).await,
                    commit.then(|| Bytes::from("uncertain"))
                );
                assert_eq!(
                    backing.reads.get(),
                    reads + 1,
                    "a failed write must be resolved by a backing read"
                );
            }
        });
    }

    #[test]
    fn hits_keep_recent_values_for_entry_and_byte_eviction() {
        block_on(async {
            for (entries, bytes) in [(2, 1024), (16, 4)] {
                let backing = CountingSpace::default();
                let mut space = CachingWriteOnceSpace::new(backing.clone(), entries, bytes);
                space.write("a", 0, Bytes::from("aa")).await.unwrap();
                space.write("a", 1, Bytes::from("bb")).await.unwrap();
                assert_eq!(space.read("a", 0).await, Some(Bytes::from("aa")));
                space.write("a", 2, Bytes::from("cc")).await.unwrap();
                assert_eq!(space.read("a", 0).await, Some(Bytes::from("aa")));
                assert_eq!(space.read("a", 2).await, Some(Bytes::from("cc")));
                assert_eq!(
                    backing.reads.get(),
                    0,
                    "recently used values should remain cached"
                );
                assert_eq!(space.read("a", 1).await, Some(Bytes::from("bb")));
                assert_eq!(
                    backing.reads.get(),
                    1,
                    "the least recently used value should be evicted"
                );
                let cache = space.cache.borrow();
                assert!(cache.values.len() <= entries);
                assert!(cache.value_bytes <= bytes);
            }
        });
    }

    #[test]
    fn per_entry_limit_bypasses_large_values_without_evicting_small_ones() {
        block_on(async {
            let backing = CountingSpace::default();
            let mut space = CachingWriteOnceSpace::new_with_entry_limit(backing.clone(), 16, 32, 4);
            space.write("a", 0, Bytes::from("aa")).await.unwrap();
            space.write("a", 1, Bytes::from("bb")).await.unwrap();
            space.write("a", 2, Bytes::from("large")).await.unwrap();
            assert_eq!(space.read("a", 2).await, Some(Bytes::from("large")));
            assert_eq!(space.read("a", 2).await, Some(Bytes::from("large")));
            assert_eq!(space.read("a", 0).await, Some(Bytes::from("aa")));
            assert_eq!(space.read("a", 1).await, Some(Bytes::from("bb")));
            assert_eq!(backing.reads.get(), 2);
            space.write("a", 3, Bytes::from("four")).await.unwrap();
            assert_eq!(space.read("a", 3).await, Some(Bytes::from("four")));
            assert_eq!(
                backing.reads.get(),
                2,
                "values exactly at the entry limit should be cached"
            );

            let mut disabled =
                CachingWriteOnceSpace::new_with_entry_limit(backing.clone(), 16, 32, 0);
            disabled.write("b", 0, Bytes::new()).await.unwrap();
            assert_eq!(disabled.read("b", 0).await, Some(Bytes::new()));
            assert_eq!(
                backing.reads.get(),
                3,
                "a zero entry limit disables even empty-value caching"
            );
        });
    }

    #[test]
    fn default_entry_limit_is_independent_of_the_total_byte_budget() {
        block_on(async {
            let backing = CountingSpace::default();
            let mut space = CachingWriteOnceSpace::new(backing.clone(), 16, 1024 * 1024);
            let small = Bytes::from(vec![0; DEFAULT_MAX_ENTRY_VALUE_BYTES]);
            let large = Bytes::from(vec![1; DEFAULT_MAX_ENTRY_VALUE_BYTES + 1]);
            space.write("a", 0, small.clone()).await.unwrap();
            space.write("a", 1, large.clone()).await.unwrap();
            assert_eq!(space.read("a", 0).await, Some(small));
            assert_eq!(backing.reads.get(), 0);
            assert_eq!(space.read("a", 1).await, Some(large));
            assert_eq!(backing.reads.get(), 1);
        });
    }

    #[test]
    fn capacity_limits_evict_or_bypass_without_changing_values() {
        block_on(async {
            for (entries, bytes) in [(1, 1024), (16, 3), (0, 1024), (16, 0)] {
                let backing = CountingSpace::default();
                let mut space = CachingWriteOnceSpace::new(backing.clone(), entries, bytes);
                space.write("a", 0, Bytes::from("aa")).await.unwrap();
                space.write("a", 1, Bytes::from("bb")).await.unwrap();
                assert_eq!(space.read("a", 0).await, Some(Bytes::from("aa")));
                assert_eq!(
                    backing.reads.get(),
                    1,
                    "the oldest entry is evicted or caching is disabled"
                );
            }
            let backing = CountingSpace::default();
            let mut space = CachingWriteOnceSpace::new(backing.clone(), 16, 3);
            space.write("a", 0, Bytes::from("aa")).await.unwrap();
            space.write("a", 1, Bytes::from("oversized")).await.unwrap();
            assert_eq!(space.read("a", 1).await, Some(Bytes::from("oversized")));
            assert_eq!(space.read("a", 1).await, Some(Bytes::from("oversized")));
            assert_eq!(space.read("a", 0).await, Some(Bytes::from("aa")));
            assert_eq!(
                backing.reads.get(),
                2,
                "oversized values bypass without evicting cached values"
            );
        });
    }
}
