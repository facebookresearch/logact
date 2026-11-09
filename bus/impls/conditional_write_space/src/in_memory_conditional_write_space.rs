/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::ConditionalWriteResult;
use agentbus_api::ConditionalWriteSpace;
use agentbus_api::Environment;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::TtlCheck;
use agentbus_api::UnsafeWallTime;
use agentbus_api::Version;
use agentbus_api::VersionedValue;
use agentbus_api::WallClockTtl;
use bytes::Bytes;
use rand::RngExt as _;

/// In-memory implementation of a conditional write space.
///
/// All clones share the same underlying state. With a TTL, each successful write
/// expires that long after it is stored, by the environment's unsafe wall clock.
/// An expired entry is treated as absent, so a write expecting an empty address
/// can reclaim it.
///
/// Each successful write gets a random version from the environment, so
/// versions are unordered and callers can rely only on comparing them for
/// equality.
#[derive(Clone)]
pub struct InMemoryConditionalWriteSpace {
    state: Rc<RefCell<HashMap<String, HashMap<u64, Entry>>>>,
    ttl: Option<WallClockTtl>,
    random_version: Rc<dyn Fn() -> Version>,
}

struct Entry {
    value: VersionedValue,
    written_at: Option<UnsafeWallTime>,
}

impl Entry {
    fn is_live(&self, check: Option<TtlCheck>) -> bool {
        match (self.written_at, check) {
            (Some(written_at), Some(check)) => check.is_live(written_at),
            _ => true,
        }
    }
}

impl InMemoryConditionalWriteSpace {
    pub fn new<E: Environment + 'static>(environment: Rc<E>) -> Self {
        Self {
            state: Rc::new(RefCell::new(HashMap::new())),
            ttl: None,
            random_version: Rc::new(move || {
                let version: u64 = environment.with_rng(|rng| rng.random());
                Version(Bytes::copy_from_slice(&version.to_be_bytes()))
            }),
        }
    }

    /// Expire each successful write `ttl` after it is stored.
    pub fn with_ttl<E: Environment + 'static>(environment: Rc<E>, ttl: Duration) -> Self {
        Self {
            ttl: Some(WallClockTtl::new(environment.clone(), ttl)),
            ..Self::new(environment)
        }
    }

    fn check(&self) -> Option<TtlCheck> {
        self.ttl.as_ref().map(WallClockTtl::check)
    }
}

impl TailableSpace for InMemoryConditionalWriteSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        if window_size == 0 {
            return Err(TailError::ZeroWindowSize);
        }

        let check = self.check();
        let state = self.state.borrow();
        let Some(space) = state.get(space_id) else {
            return Ok(0);
        };
        let is_live = |address: &u64| space.get(address).is_some_and(|entry| entry.is_live(check));

        let Some(max_addr) = space.keys().copied().filter(is_live).max() else {
            return Ok(0);
        };
        let end = max_addr + 1;

        let window_start = end.saturating_sub(window_size);
        for addr in window_start..end {
            if !is_live(&addr) {
                return Ok(addr);
            }
        }
        Ok(end)
    }
}

impl ConditionalWriteSpace for InMemoryConditionalWriteSpace {
    async fn write(
        &mut self,
        space_id: &str,
        address: u64,
        expected_version: Option<Version>,
        value: Bytes,
    ) -> ConditionalWriteResult<Option<Version>> {
        let check = self.check();
        let mut state = self.state.borrow_mut();
        let space = state.entry(space_id.to_string()).or_default();
        let existing = space
            .get(&address)
            .filter(|entry| entry.is_live(check))
            .map(|entry| &entry.value);

        match (&expected_version, existing) {
            (None, None) => {}
            (Some(expected), Some(actual)) if *expected == actual.version => {}
            _ => return Ok(None),
        }

        let next_version = (self.random_version)();

        space.insert(
            address,
            Entry {
                value: VersionedValue {
                    version: next_version.clone(),
                    value,
                },
                written_at: check.map(|check| check.now()),
            },
        );
        Ok(Some(next_version))
    }

    async fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> ConditionalWriteResult<Option<VersionedValue>> {
        let check = self.check();
        Ok(self
            .state
            .borrow()
            .get(space_id)
            .and_then(|space| space.get(&address))
            .filter(|entry| entry.is_live(check))
            .map(|entry| entry.value.clone()))
    }
}
