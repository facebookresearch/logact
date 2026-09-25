/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A `Storage` adapter that namespaces every key under a fixed prefix, so
//! independent owners can share one backing store without colliding.

use bytes::Bytes;

use crate::storage::Storage;
use crate::storage::StorageResult;

/// Wraps another [`Storage`], prepending `prefix` to every key. Used to sandbox a
/// commit-service implementation's state under its own (e.g. version) namespace,
/// so different owners sharing one backend cannot collide.
///
/// The prefix should include a trailing separator (e.g. `"v1/"`): without one,
/// `"v1" + "0:k"` and `"v10" + ":k"` would both map to `"v10:k"`.
pub struct ScopedStorage<S> {
    prefix: String,
    inner: S,
}

impl<S: Storage> ScopedStorage<S> {
    pub fn new(prefix: impl Into<String>, inner: S) -> Self {
        Self {
            prefix: prefix.into(),
            inner,
        }
    }

    fn scoped_key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage> Storage for ScopedStorage<S> {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        self.inner.get(&self.scoped_key(key)).await
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        self.inner
            .put(&self.scoped_key(key), value, expected, new_position)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use futures::executor::block_on;

    use super::*;
    use crate::storage::InMemoryStorage;

    #[test]
    fn distinct_prefixes_are_isolated() {
        // Two scopes over the same backing store (shared via `Rc`).
        let inner = Rc::new(InMemoryStorage::new());
        let a = ScopedStorage::new("a/", inner.clone());
        let b = ScopedStorage::new("b/", inner.clone());

        block_on(a.put("k", Bytes::from_static(b"va"), None, 0)).unwrap();

        // `b` can't see `a`'s key, and can write its own at the same logical key.
        assert!(block_on(b.get("k")).unwrap().is_none());
        block_on(b.put("k", Bytes::from_static(b"vb"), None, 0)).unwrap();

        assert_eq!(
            block_on(a.get("k")).unwrap().unwrap().0,
            Bytes::from_static(b"va")
        );
        assert_eq!(
            block_on(b.get("k")).unwrap().unwrap().0,
            Bytes::from_static(b"vb")
        );

        // The prefix is applied to the underlying store's keys.
        assert!(block_on(inner.get("a/k")).unwrap().is_some());
        assert!(block_on(inner.get("k")).unwrap().is_none());
    }
}
