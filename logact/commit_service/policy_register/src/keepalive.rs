/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A persistent leased reader that is polled in the background to renew its lease.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::Clock;
use agentbus_api::Environment;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;

use crate::PolicyRegister;
use crate::PolicyRegisterResult;

/// How often the keepalive reads the register, renewing the lease once it expires.
const DEFAULT_INTERVAL: Duration = Duration::from_millis(100);
/// How long the keepalive runs after a request before the next request restarts it.
const DEFAULT_DURATION: Duration = Duration::from_secs(6 * 60 * 60);

/// A read starts a background task, unless one is running, that reads the inner
/// register every `interval` for `duration`. Most background reads hit the
/// inner register's cache; the first one after the lease expires renews it.
/// Requests then wait on storage only if they arrive within an interval of an
/// expiry, or after the task ends and its lease expires before the next
/// request starts another.
pub struct KeepalivePolicyRegister<R, E> {
    state: Rc<State<R, E>>,
}

struct State<R, E> {
    reader: R,
    environment: Rc<E>,
    interval: Duration,
    duration: Duration,
    started: Cell<bool>,
}

impl<R, E> Clone for KeepalivePolicyRegister<R, E> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<R, E> KeepalivePolicyRegister<R, E> {
    pub fn new(reader: R, environment: Rc<E>) -> Self {
        Self::with_timing(reader, environment, DEFAULT_INTERVAL, DEFAULT_DURATION)
    }

    pub fn with_timing(
        reader: R,
        environment: Rc<E>,
        interval: Duration,
        duration: Duration,
    ) -> Self {
        Self {
            state: Rc::new(State {
                reader,
                environment,
                interval,
                duration,
                started: Cell::new(false),
            }),
        }
    }
}

impl<R: PolicyRegister + 'static, E: Environment + 'static> PolicyRegister
    for KeepalivePolicyRegister<R, E>
{
    fn key(&self) -> &str {
        self.state.reader.key()
    }

    async fn read_validated(&self) -> PolicyRegisterResult<VersionedPolicyState> {
        if !self.state.started.replace(true) {
            let state = self.state.clone();
            self.state.environment.spawn_local(async move {
                let now = || state.environment.with_clock(|clock| clock.monotonic_time());
                let deadline = now() + state.duration;
                while now() < deadline {
                    state.environment.sleep(state.interval).await;
                    let _ = state.reader.read_validated().await;
                }
                state.started.set(false);
            });
        }
        self.state.reader.read_validated().await
    }

    async fn set_policy(
        &self,
        state: &PolicyState,
        expected: Option<i64>,
    ) -> PolicyRegisterResult<i64> {
        self.state.reader.set_policy(state, expected).await
    }
}
