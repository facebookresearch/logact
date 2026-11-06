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

use agentbus_api::Clock;
use agentbus_api::ConditionalWriteResult;
use agentbus_api::ConditionalWriteSpace;
use agentbus_api::Environment;
use agentbus_api::MonotonicInstant;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::Version;
use agentbus_api::VersionedValue;
use bytes::Bytes;

/// In-memory implementation of a conditional write space.
///
/// All clones share the same underlying state. With a TTL, each successful write
/// expires that long after it is stored, measured on the environment's monotonic
/// clock. An expired entry is treated as absent, so a write expecting an empty
/// address can reclaim it.
#[derive(Clone)]
pub struct InMemoryConditionalWriteSpace {
    state: Rc<RefCell<HashMap<String, HashMap<u64, Entry>>>>,
    expiry: Option<Expiry>,
}

struct Entry {
    value: VersionedValue,
    expires_at: Option<MonotonicInstant>,
}

impl Entry {
    fn is_live(&self, now: Option<MonotonicInstant>) -> bool {
        match (self.expires_at, now) {
            (Some(deadline), Some(now)) => now < deadline,
            _ => true,
        }
    }
}

#[derive(Clone)]
struct Expiry {
    ttl: Duration,
    now: Rc<dyn Fn() -> MonotonicInstant>,
}

impl Default for InMemoryConditionalWriteSpace {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryConditionalWriteSpace {
    pub fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(HashMap::new())),
            expiry: None,
        }
    }

    /// Expire each successful write `ttl` after it is stored.
    pub fn with_ttl<E: Environment + 'static>(environment: Rc<E>, ttl: Duration) -> Self {
        Self {
            expiry: Some(Expiry {
                ttl,
                now: Rc::new(move || environment.with_clock(|clock| clock.monotonic_time())),
            }),
            ..Self::new()
        }
    }

    fn now(&self) -> Option<MonotonicInstant> {
        self.expiry.as_ref().map(|expiry| (expiry.now)())
    }
}

fn version_from_u64(n: u64) -> Version {
    Version(Bytes::copy_from_slice(&n.to_be_bytes()))
}

fn version_to_u64(v: &Version) -> u64 {
    let bytes: [u8; 8] = v.0[..]
        .try_into()
        .expect("in-memory version is always 8 bytes");
    u64::from_be_bytes(bytes)
}

impl TailableSpace for InMemoryConditionalWriteSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        if window_size == 0 {
            return Err(TailError::ZeroWindowSize);
        }

        let now = self.now();
        let state = self.state.borrow();
        let Some(space) = state.get(space_id) else {
            return Ok(0);
        };
        let is_live = |address: &u64| space.get(address).is_some_and(|entry| entry.is_live(now));

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
    ) -> ConditionalWriteResult<bool> {
        let now = self.now();
        let mut state = self.state.borrow_mut();
        let space = state.entry(space_id.to_string()).or_default();
        let existing = space
            .get(&address)
            .filter(|entry| entry.is_live(now))
            .map(|entry| &entry.value);

        match (&expected_version, existing) {
            (None, None) => {}
            (Some(expected), Some(actual)) if *expected == actual.version => {}
            _ => return Ok(false),
        }

        let next_version = match expected_version {
            None => 0u64,
            Some(v) => version_to_u64(&v) + 1,
        };

        let expires_at = now
            .zip(self.expiry.as_ref())
            .map(|(now, expiry)| now + expiry.ttl);
        space.insert(
            address,
            Entry {
                value: VersionedValue {
                    version: version_from_u64(next_version),
                    value,
                },
                expires_at,
            },
        );
        Ok(true)
    }

    async fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> ConditionalWriteResult<Option<VersionedValue>> {
        let now = self.now();
        Ok(self
            .state
            .borrow()
            .get(space_id)
            .and_then(|space| space.get(&address))
            .filter(|entry| entry.is_live(now))
            .map(|entry| entry.value.clone()))
    }
}
