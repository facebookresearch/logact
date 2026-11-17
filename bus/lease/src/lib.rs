/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A lease protocol that caches reads of any versioned compare-and-swap cell
//! while keeping them linearizable.
//!
//! Readers cache a stable value for a lease that starts before the read.
//! Writers revoke the lease on the current value, wait until every lease on it
//! has expired, and then publish. Reads during a write go to the backing cell.
//! Each cell stores a payload together with its [`LeaseMetadata`], in
//! whatever format the cell's storage uses. Every writer of a cell must use
//! this protocol.
//!
//! Each value stores its lease duration. Readers lease it for exactly that,
//! and anyone replacing it drains for that plus the drift allowance, so a
//! participant's configured lease duration only sets what it stores when it
//! writes, and can change at any time. Participants must agree on
//! [`LeaseConfig::drift_allowance`]. Durations are measured locally; no clock
//! values are compared across participants.
//!
//! A reservation that stays pending for a drain plus
//! [`LeaseConfig::recovery_grace`] after a participant first observes it is
//! presumed abandoned. A writer expecting
//! the reserved value publishes over it, and a reader republishes the reserved
//! value as stable. Both are safe because leases on the reserved value predate
//! the reservation and none are granted while it is pending.
//!
//! Reservations and recoveries keep the version reported to callers, so callers
//! see a new version only when a write publishes.
//!
//! A value without metadata was written without the protocol, by a writer that
//! does not drain leases, so readers never cache it. The first write through
//! the protocol stores metadata, after which readers lease the value. This lets
//! readers adopt the protocol before every writer has.

mod cas_cell;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::MonotonicInstant;
pub use cas_cell::CasCell;
pub use cas_cell::LeaseMetadata;
pub use cas_cell::StoredValue;

/// Timing bounds of one participant.
#[derive(Clone, Copy, Debug)]
pub struct LeaseConfig {
    /// The lease duration this participant stores with the values it writes.
    pub lease_duration: Duration,
    /// Covers the maximum difference in elapsed time between writer and
    /// readers. Every participant must use the same value.
    pub drift_allowance: Duration,
    /// How long, after leases on a reserved value have drained, this
    /// participant gives the original writer to publish before recovering the
    /// reservation. It only delays recovery, so participants need not agree.
    pub recovery_grace: Duration,
}

impl LeaseConfig {
    /// Time a writer waits after revoking the lease on a value stored with
    /// `stored_lease`, before publishing.
    pub fn drain_duration(self, stored_lease: Duration) -> Duration {
        stored_lease.saturating_add(self.drift_allowance)
    }

    /// Time a reservation stored with `stored_lease` must stay pending after
    /// being observed before this participant recovers it: a drain for leases
    /// on the reserved value to expire, then the grace for the original writer
    /// to publish.
    pub fn recovery_timeout(self, stored_lease: Duration) -> Duration {
        self.drain_duration(stored_lease)
            .saturating_add(self.recovery_grace)
    }
}

/// A payload and the version reported to callers.
#[derive(Clone, Debug, PartialEq)]
pub struct Leased<P, V> {
    pub payload: P,
    pub version: V,
}

/// A stored value, classified by its lease metadata.
enum Stored<P, V> {
    /// Written without the protocol, so never cached. Its stored version is
    /// the version reported to callers.
    Unleased { value: Leased<P, V> },
    /// Leasable for up to `lease_duration`.
    Stable {
        value: Leased<P, V>,
        stored_version: V,
        lease_duration: Duration,
    },
    /// Reserved by a writer that is draining leases of up to `lease_duration`.
    Revoked {
        value: Leased<P, V>,
        stored_version: V,
        lease_duration: Duration,
    },
}

impl<P, V> Stored<P, V> {
    fn value(&self) -> &Leased<P, V> {
        match self {
            Stored::Unleased { value }
            | Stored::Stable { value, .. }
            | Stored::Revoked { value, .. } => value,
        }
    }
}

struct CachedLease<P, V> {
    leased: Leased<P, V>,
    expires_at: MonotonicInstant,
}

struct PendingRevocation<V> {
    stored_version: V,
    since: MonotonicInstant,
}

type Payload<C> = <C as CasCell>::Payload;
type Version<C> = <C as CasCell>::Version;

/// One [`CasCell`] with the lease protocol: reads are served from a lease
/// cached by this instance, and writes drain leases before publishing.
/// Each instance keeps its own lease cache, as a separate process would;
/// share one instance to share the cache.
pub struct LeasedCell<C: CasCell, E> {
    cell: C,
    environment: Rc<E>,
    config: LeaseConfig,
    cached: RefCell<Option<CachedLease<Payload<C>, Version<C>>>>,
    pending: RefCell<Option<PendingRevocation<Version<C>>>>,
}

impl<C: CasCell, E: Environment> LeasedCell<C, E> {
    pub fn new(cell: C, environment: Rc<E>, config: LeaseConfig) -> Self {
        Self {
            cell,
            environment,
            config,
            cached: RefCell::new(None),
            pending: RefCell::new(None),
        }
    }

    /// Serve the cached value while its lease lasts; otherwise read the cell,
    /// recover a reservation abandoned for the recovery timeout, and lease the
    /// value if it is stable. Empty cells and values written without the
    /// protocol are not cached.
    pub async fn read(&self) -> Result<Option<Leased<Payload<C>, Version<C>>>, C::Error> {
        let started = self.now();
        if let Some(cached) = self.cached.borrow().as_ref()
            && started < cached.expires_at
        {
            return Ok(Some(cached.leased.clone()));
        }

        let (value, stored_lease) = match load(&self.cell).await? {
            None => {
                *self.cached.borrow_mut() = None;
                return Ok(None);
            }
            Some(Stored::Unleased { value }) => return Ok(Some(value)),
            Some(Stored::Revoked {
                value,
                stored_version,
                lease_duration,
            }) => {
                self.recover_if_abandoned(&value, stored_version, lease_duration, None)
                    .await?;
                return Ok(Some(value));
            }
            Some(Stored::Stable {
                value,
                lease_duration,
                ..
            }) => (value, lease_duration),
        };

        let expires_at = started + stored_lease;
        *self.pending.borrow_mut() = None;
        let mut cached = self.cached.borrow_mut();
        // Concurrent refreshes may finish out of order; keep the later lease.
        if self.now() < expires_at
            && cached
                .as_ref()
                .is_none_or(|cached| cached.expires_at < expires_at)
        {
            *cached = Some(CachedLease {
                leased: value.clone(),
                expires_at,
            });
        }
        Ok(Some(value))
    }

    /// Publish `payload` if the version reported to callers is `expected`,
    /// after draining leases on the current value, and return the new version,
    /// or `None` on a version mismatch. `expected: None` creates the value: it
    /// succeeds only if the cell is empty, and needs no drain because empty
    /// cells are never cached. A writer that finds the expected value reserved
    /// by another writer waits until it is published or abandoned.
    pub async fn write(
        &self,
        payload: Payload<C>,
        expected: Option<Version<C>>,
    ) -> Result<Option<Version<C>>, C::Error> {
        let Some(expected) = expected else {
            return self.cell.write(payload, self.published(None), None).await;
        };
        loop {
            let Some(current) = load(&self.cell).await? else {
                return Ok(None);
            };
            if current.value().version != expected {
                return Ok(None);
            }
            match current {
                Stored::Unleased { value } => {
                    let stored_version = value.version.clone();
                    return self
                        .revoke_then_publish(payload, value, stored_version, Duration::ZERO)
                        .await;
                }
                Stored::Stable {
                    value,
                    stored_version,
                    lease_duration,
                } => {
                    return self
                        .revoke_then_publish(payload, value, stored_version, lease_duration)
                        .await;
                }
                Stored::Revoked {
                    value,
                    stored_version,
                    lease_duration,
                } => {
                    if let Some(written) = self
                        .recover_if_abandoned(
                            &value,
                            stored_version,
                            lease_duration,
                            Some(&payload),
                        )
                        .await?
                    {
                        return Ok(Some(written));
                    }
                    self.environment
                        .sleep(
                            (self.config.drain_duration(lease_duration) / 8)
                                .max(Duration::from_millis(1)),
                        )
                        .await;
                }
            }
        }
    }

    /// If the reservation of `reserved` at `stored_version`, stored with
    /// `stored_lease`, has been pending for the recovery timeout since this
    /// instance first saw it, roll forward to
    /// `roll_forward` if given, otherwise roll back by republishing `reserved`
    /// as stable, and return the new stored version. Returns `None` while the
    /// reservation is not yet abandoned, or if a concurrent publish or recovery
    /// replaced it first.
    async fn recover_if_abandoned(
        &self,
        reserved: &Leased<Payload<C>, Version<C>>,
        stored_version: Version<C>,
        stored_lease: Duration,
        roll_forward: Option<&Payload<C>>,
    ) -> Result<Option<Version<C>>, C::Error> {
        let now = self.now();
        let first_seen = match &mut *self.pending.borrow_mut() {
            Some(pending) if pending.stored_version == stored_version => pending.since,
            pending => {
                *pending = Some(PendingRevocation {
                    stored_version: stored_version.clone(),
                    since: now,
                });
                now
            }
        };
        if now < first_seen + self.config.recovery_timeout(stored_lease) {
            return Ok(None);
        }
        let (payload, leasing) = match roll_forward {
            Some(payload) => (payload.clone(), self.published(None)),
            None => (
                reserved.payload.clone(),
                self.published(Some(reserved.version.clone())),
            ),
        };
        self.cell
            .write(payload, leasing, Some(stored_version))
            .await
    }

    /// Reserve `current`, stored at `stored_version` with `stored_lease`, then
    /// publish `payload` once leases on it have drained.
    async fn revoke_then_publish(
        &self,
        payload: Payload<C>,
        current: Leased<Payload<C>, Version<C>>,
        stored_version: Version<C>,
        stored_lease: Duration,
    ) -> Result<Option<Version<C>>, C::Error> {
        let reservation = LeaseMetadata {
            revoked: true,
            visible_version: Some(current.version),
            lease_duration: stored_lease,
        };
        let Some(reserved) = self
            .cell
            .write(current.payload, reservation, Some(stored_version))
            .await?
        else {
            return Ok(None);
        };

        self.environment
            .sleep(self.config.drain_duration(stored_lease))
            .await;
        self.cell
            .write(payload, self.published(None), Some(reserved))
            .await
    }

    /// Metadata for a stable value written by this participant, optionally
    /// reporting `visible_version` instead of the stored version.
    fn published(&self, visible_version: Option<Version<C>>) -> LeaseMetadata<Version<C>> {
        LeaseMetadata {
            revoked: false,
            visible_version,
            lease_duration: self.config.lease_duration,
        }
    }

    fn now(&self) -> MonotonicInstant {
        self.environment.with_clock(|clock| clock.monotonic_time())
    }
}

async fn load<C: CasCell>(cell: &C) -> Result<Option<Stored<C::Payload, C::Version>>, C::Error> {
    let Some(StoredValue {
        payload,
        leasing,
        version: stored_version,
    }) = cell.read().await?
    else {
        return Ok(None);
    };
    let Some(leasing) = leasing else {
        return Ok(Some(Stored::Unleased {
            value: Leased {
                payload,
                version: stored_version,
            },
        }));
    };
    let value = Leased {
        payload,
        version: leasing
            .visible_version
            .unwrap_or_else(|| stored_version.clone()),
    };
    let lease_duration = leasing.lease_duration;
    Ok(Some(if leasing.revoked {
        Stored::Revoked {
            value,
            stored_version,
            lease_duration,
        }
    } else {
        Stored::Stable {
            value,
            stored_version,
            lease_duration,
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::pin;

    use agentbus_simulator::Simulator;
    use agentbus_simulator::generate_seed;
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::*;

    const CONFIG: LeaseConfig = LeaseConfig {
        lease_duration: Duration::from_millis(100),
        drift_allowance: Duration::from_millis(10),
        recovery_grace: Duration::from_millis(10),
    };

    #[derive(Default)]
    struct Store {
        value: Option<StoredValue<Bytes, i64>>,
        reads: usize,
        /// Runs before the next write, to model a concurrent writer landing first.
        before_next_write: Option<Box<dyn FnOnce(&mut Store)>>,
    }

    /// An in-memory cell that stores payloads and metadata unencoded.
    #[derive(Clone)]
    struct MemoryCell {
        store: Rc<RefCell<Store>>,
    }

    impl MemoryCell {
        fn reads(&self) -> usize {
            self.store.borrow().reads
        }
    }

    impl CasCell for MemoryCell {
        type Payload = Bytes;
        type Version = i64;
        type Error = Infallible;

        async fn read(&self) -> Result<Option<StoredValue<Bytes, i64>>, Self::Error> {
            let mut store = self.store.borrow_mut();
            store.reads += 1;
            Ok(store.value.clone())
        }

        async fn write(
            &self,
            payload: Bytes,
            leasing: LeaseMetadata<i64>,
            expected: Option<i64>,
        ) -> Result<Option<i64>, Self::Error> {
            let mut store = self.store.borrow_mut();
            if let Some(hook) = store.before_next_write.take() {
                hook(&mut store);
            }
            let current = store.value.as_ref().map(|stored| stored.version);
            if current != expected {
                return Ok(None);
            }
            let version = current.map_or(0, |version| version + 1);
            store.value = Some(StoredValue {
                payload,
                leasing: Some(leasing),
                version,
            });
            Ok(Some(version))
        }
    }

    type Enforcer = LeasedCell<MemoryCell, Simulator>;

    /// Run `test` in a simulator. `enforcer` creates an independently cached
    /// participant.
    fn simulate<F>(test: impl Fn(Rc<Simulator>, MemoryCell, Rc<dyn Fn() -> Enforcer>) -> F)
    where
        F: Future<Output = Result<(), Infallible>> + 'static,
    {
        let env = Rc::new(Simulator::new(generate_seed()));
        let cell = MemoryCell {
            store: Rc::default(),
        };
        let enforcer: Rc<dyn Fn() -> Enforcer> = {
            let (env, cell) = (env.clone(), cell.clone());
            Rc::new(move || LeasedCell::new(cell.clone(), env.clone(), CONFIG))
        };
        let handle = env.spawn(test(env.clone(), cell, enforcer));
        env.run();
        block_on(handle)
            .expect("test task should complete")
            .expect("test should pass");
    }

    async fn read(enforcer: &Enforcer) -> Leased<Bytes, i64> {
        enforcer
            .read()
            .await
            .expect("read should succeed")
            .expect("cell should be written")
    }

    /// The value a caller would see, read from storage without a lease.
    async fn stored(cell: &MemoryCell) -> Leased<Bytes, i64> {
        let Ok(stored) = load(cell).await;
        stored.expect("cell should be written").value().clone()
    }

    fn now(env: &Simulator) -> MonotonicInstant {
        env.with_clock(|clock| clock.monotonic_time())
    }

    #[test]
    fn stable_reads_are_served_from_the_lease() {
        simulate(|env, cell, enforcer| async move {
            let participant = enforcer();
            participant.write(Bytes::from("v1"), None).await?;

            let first = read(&participant).await;
            env.sleep(CONFIG.lease_duration / 2).await;
            assert_eq!(read(&participant).await, first);
            assert_eq!(cell.reads(), 1, "the second read should hit the lease");

            env.sleep(CONFIG.lease_duration).await;
            assert_eq!(read(&participant).await, first);
            assert_eq!(cell.reads(), 2, "an expired lease should be refreshed");
            Ok(())
        });
    }

    #[test]
    fn unleased_values_are_not_cached_until_a_protocol_write() {
        simulate(|env, cell, enforcer| async move {
            let unleased = StoredValue {
                payload: Bytes::from("v1"),
                leasing: None,
                version: 0,
            };
            cell.store.borrow_mut().value = Some(unleased.clone());
            let reader = enforcer();

            let first = read(&reader).await;
            assert_eq!(read(&reader).await, first);
            assert_eq!(cell.reads(), 2, "an unleased value should not be cached");
            env.sleep(CONFIG.recovery_timeout(CONFIG.lease_duration))
                .await;
            assert_eq!(read(&reader).await, first);
            assert_eq!(
                cell.store.borrow().value,
                Some(unleased),
                "readers should not rewrite an unleased value"
            );

            let outcome = enforcer()
                .write(Bytes::from("v2"), Some(first.version))
                .await?;
            assert!(outcome.is_some());
            let published = read(&reader).await;
            assert_eq!(published.payload, "v2");
            let reads = cell.reads();
            assert_eq!(read(&reader).await, published);
            assert_eq!(
                cell.reads(),
                reads,
                "a protocol write should enable leasing"
            );
            Ok(())
        });
    }

    #[test]
    fn empty_cells_are_not_cached() {
        simulate(|_, _, enforcer| async move {
            let participant = enforcer();
            assert_eq!(participant.read().await?, None);
            enforcer().write(Bytes::from("v1"), None).await?;
            assert_eq!(read(&participant).await.payload, "v1");
            Ok(())
        });
    }

    #[test]
    fn writes_with_the_wrong_version_mismatch() {
        simulate(|_, cell, enforcer| async move {
            let writer = enforcer();
            assert_eq!(
                writer.write(Bytes::from("v1"), Some(0)).await?,
                None,
                "an expected version requires a written cell"
            );
            writer.write(Bytes::from("v1"), None).await?;
            let current = stored(&cell).await;
            assert_eq!(
                writer.write(Bytes::from("v2"), None).await?,
                None,
                "no expected version requires an empty cell"
            );
            assert_eq!(
                writer
                    .write(Bytes::from("v2"), Some(current.version + 1))
                    .await?,
                None,
                "a stale expected version should mismatch"
            );
            assert_eq!(stored(&cell).await, current);
            Ok(())
        });
    }

    #[test]
    fn writes_drain_leases_before_publishing() {
        simulate(|env, cell, enforcer| async move {
            let (writer, reader, observer) = (enforcer(), enforcer(), enforcer());
            writer.write(Bytes::from("v1"), None).await?;
            let old = read(&reader).await;
            let written = Cell::new(false);

            let write = async {
                let started = now(&env);
                let outcome = writer.write(Bytes::from("v2"), Some(old.version)).await;
                written.set(true);
                (outcome, now(&env) - started)
            };
            let reads = async {
                let mut observed_new = false;
                while !written.get() {
                    for current in [read(&reader).await, read(&observer).await] {
                        if current.payload == "v2" {
                            observed_new = true;
                        } else {
                            assert!(!observed_new, "read the old value after the new one");
                            assert_eq!(current, old, "a reservation should not change the version");
                        }
                    }
                    env.sleep(Duration::from_millis(5)).await;
                }
            };
            let ((outcome, elapsed), ()) = futures::join!(write, reads);

            assert!(outcome?.is_some());
            assert!(
                elapsed >= CONFIG.drain_duration(CONFIG.lease_duration),
                "the write should drain leases"
            );
            let new = stored(&cell).await;
            assert_eq!(new.payload, "v2");
            assert_ne!(new.version, old.version);
            Ok(())
        });
    }

    /// Start a write from `crashed` and abandon it after its reservation.
    async fn abandon_write(crashed: &Enforcer, expected: i64) {
        let mut write = pin!(crashed.write(Bytes::from("abandoned"), Some(expected)));
        assert!(
            futures::poll!(write.as_mut()).is_pending(),
            "the writer should reserve and then drain"
        );
    }

    #[test]
    fn readers_roll_back_an_abandoned_reservation() {
        simulate(|env, cell, enforcer| async move {
            let (crashed, reader) = (enforcer(), enforcer());
            crashed.write(Bytes::from("v1"), None).await?;
            let old = read(&reader).await;
            env.sleep(CONFIG.lease_duration).await;
            abandon_write(&crashed, old.version).await;

            assert_eq!(read(&reader).await, old);
            env.sleep(CONFIG.recovery_timeout(CONFIG.lease_duration))
                .await;
            assert_eq!(read(&reader).await, old, "this read should roll back");
            assert_eq!(read(&reader).await, old);
            let reads = cell.reads();
            assert_eq!(read(&reader).await, old);
            assert_eq!(cell.reads(), reads, "the recovered value should be leased");

            let outcome = enforcer()
                .write(Bytes::from("v3"), Some(old.version))
                .await?;
            assert!(outcome.is_some());
            assert_eq!(stored(&cell).await.payload, "v3");
            Ok(())
        });
    }

    #[test]
    fn writers_roll_forward_an_abandoned_reservation() {
        simulate(|env, cell, enforcer| async move {
            let (crashed, writer) = (enforcer(), enforcer());
            crashed.write(Bytes::from("v1"), None).await?;
            let old = stored(&cell).await;
            abandon_write(&crashed, old.version).await;

            let started = now(&env);
            let outcome = writer.write(Bytes::from("v2"), Some(old.version)).await?;
            assert!(outcome.is_some());
            assert!(
                now(&env) - started
                    >= CONFIG.drain_duration(CONFIG.lease_duration) + CONFIG.recovery_grace,
                "the writer should wait for leases to drain, then the grace"
            );
            let new = stored(&cell).await;
            assert_eq!(new.payload, "v2");
            assert_ne!(new.version, old.version);
            Ok(())
        });
    }

    #[test]
    fn writers_retry_when_a_reader_recovers_first() {
        simulate(|_, cell, enforcer| async move {
            let (crashed, writer) = (enforcer(), enforcer());
            crashed.write(Bytes::from("v1"), None).await?;
            let old = stored(&cell).await;
            abandon_write(&crashed, old.version).await;

            // A reader rolls the reservation back just before the writer's
            // roll-forward lands.
            let visible = old.version;
            cell.store.borrow_mut().before_next_write = Some(Box::new(move |store: &mut Store| {
                let stored = store.value.as_mut().expect("cell is written");
                stored.leasing = Some(LeaseMetadata {
                    revoked: false,
                    visible_version: Some(visible),
                    lease_duration: CONFIG.lease_duration,
                });
                stored.version += 1;
            }));
            let outcome = writer.write(Bytes::from("v2"), Some(old.version)).await?;
            assert!(outcome.is_some(), "the visible version still matched");
            assert_eq!(stored(&cell).await.payload, "v2");
            Ok(())
        });
    }

    #[test]
    fn participants_honor_the_stored_lease() {
        const LONG: LeaseConfig = LeaseConfig {
            lease_duration: Duration::from_millis(1000),
            ..CONFIG
        };
        simulate(|env, cell, short| async move {
            let long = || LeasedCell::new(cell.clone(), env.clone(), LONG);
            let long_reader = long();
            short().write(Bytes::from("v1"), None).await?;
            long_reader.read().await?;
            env.sleep(CONFIG.lease_duration).await;
            long_reader.read().await?;
            assert_eq!(cell.reads(), 2, "readers lease for the stored duration");

            let current = stored(&cell).await;
            long()
                .write(Bytes::from("v2"), Some(current.version))
                .await?;
            let short_reader = short();
            let current = short_reader.read().await?.expect("cell is written");
            let reads = cell.reads();
            env.sleep(CONFIG.lease_duration * 2).await;
            short_reader.read().await?;
            assert_eq!(cell.reads(), reads, "a reader's own config does not matter");

            let started = now(&env);
            short()
                .write(Bytes::from("v3"), Some(current.version))
                .await?;
            assert!(
                now(&env) - started >= LONG.drain_duration(LONG.lease_duration),
                "writers drain for the stored duration"
            );
            Ok(())
        });
    }
}
