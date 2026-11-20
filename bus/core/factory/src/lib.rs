/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::RequestGate;
use agentbus_conditional_write_space::WriteOnceSpaceAdapter;
use agentbus_core::AgentBus;
use agentbus_core::AgentBusMetrics;
use agentbus_core::AgentbusLogger;
use agentbus_core::ChanneledAgentBus;
use agentbus_core::RealEnvironment;
use agentbus_core::client::AgentBusClient;
use agentbus_core::server_lib::ParsedBackend;
use agentbus_sqlite::SqliteConditionalWriteSpace;
use agentbus_sqlite::SqliteDb;
use agentbus_writeonce::InMemoryWriteOnceSpace;
use agentbus_writeonce::ObservableWriteOnceSpace;
use agentbus_writeonce::WriteOnceAgentBus;
use anyhow::Result;
use channeled_svc_agentbus::ChanneledAgentBus as GatedAgentBus;

/// Shared, type-erased metrics factory.
///
/// The boxed callable keeps `build_on()` non-generic to avoid large per-closure
/// AgentBus construction codegen.
pub type MakeMetrics = Box<dyn Fn() -> Result<Rc<dyn AgentBusMetrics>> + 'static>;

/// Shared, type-erased logger factory.
pub type MakeLogger = Box<dyn Fn() -> Result<Rc<dyn AgentbusLogger>> + 'static>;

/// Builds a type-erased `AgentBus` handle from a parsed backend.
pub trait ChanneledAgentBusFactory: Sized {
    /// Build the requested backend on `env` and its local executor.
    fn build_on(
        self,
        env: Rc<RealEnvironment>,
        backend: ParsedBackend,
    ) -> Result<ChanneledAgentBus>;
}

/// Factory for OSS/core AgentBus backends.
pub struct CoreAgentBusFactory {
    blocking_poll_interval: Option<Duration>,
    make_metrics: MakeMetrics,
    logger_factory: MakeLogger,
}

impl CoreAgentBusFactory {
    pub fn new(
        blocking_poll_interval: Option<Duration>,
        make_metrics: MakeMetrics,
        logger_factory: MakeLogger,
    ) -> Self {
        Self {
            blocking_poll_interval,
            make_metrics,
            logger_factory,
        }
    }
}

impl CoreAgentBusFactory {
    /// Build the requested backend on `env` behind a `channeled_svc`
    /// `ChanneledAgentBus` that admits each operation through `gate`.
    pub fn build_gated_on<G: RequestGate + 'static>(
        self,
        env: Rc<RealEnvironment>,
        backend: ParsedBackend,
        gate: G,
    ) -> Result<GatedAgentBus> {
        self.build_with(env, backend, GatedChannel(gate))
    }

    /// Build the requested backend on `env` and put it behind the channel
    /// `wrap` chooses.
    pub fn build_with<W: MigrationSwitchAgentBusWrapper>(
        self,
        env: Rc<RealEnvironment>,
        backend: ParsedBackend,
        wrap: W,
    ) -> Result<W::Output> {
        let Self {
            blocking_poll_interval,
            make_metrics,
            logger_factory,
        } = self;
        match backend {
            ParsedBackend::Memory => {
                let space = ObservableWriteOnceSpace::new(
                    InMemoryWriteOnceSpace::new(),
                    make_metrics()?,
                    logger_factory()?,
                    env.clone(),
                );
                let bus = WriteOnceAgentBus::new(space, env.clone(), blocking_poll_interval);
                Ok(wrap.wrap(env, bus))
            }
            ParsedBackend::Http(grpc_url) => {
                let endpoint = tonic::transport::Channel::from_shared(grpc_url)?;
                Ok(wrap.wrap(env, AgentBusClient::new(endpoint.connect_lazy())))
            }
            ParsedBackend::Sqlite(path) => {
                let db = SqliteDb::open(Path::new(&path))?;
                let space = WriteOnceSpaceAdapter::new(SqliteConditionalWriteSpace::new(&db)?);
                let space = ObservableWriteOnceSpace::new(
                    space,
                    make_metrics()?,
                    logger_factory()?,
                    env.clone(),
                );
                let bus = WriteOnceAgentBus::new(space, env.clone(), blocking_poll_interval);
                Ok(wrap.wrap(env, bus))
            }
            ParsedBackend::Other(url) => {
                anyhow::bail!(
                    "Unsupported URL '{}'. Supported schemes: memory://, http://, https://, sqlite://",
                    url
                );
            }
        }
    }
}

impl ChanneledAgentBusFactory for CoreAgentBusFactory {
    fn build_on(
        self,
        env: Rc<RealEnvironment>,
        backend: ParsedBackend,
    ) -> Result<ChanneledAgentBus> {
        self.build_with(env, backend, LegacyChannel)
    }
}

/// Migration scaffolding: puts a backend that a factory built behind either
/// the legacy channel or the gated `channeled_svc` channel while
/// `enable_channeled_svc` rolls out.
///
/// TODO: remove this trait, `LegacyChannel` and `build_with` once only the
/// gated channel remains.
pub trait MigrationSwitchAgentBusWrapper {
    type Output;

    fn wrap<T: AgentBus + 'static>(self, env: Rc<RealEnvironment>, bus: T) -> Self::Output;
}

/// The legacy `ChanneledAgentBus`, as `build_on` returns.
pub struct LegacyChannel;

impl MigrationSwitchAgentBusWrapper for LegacyChannel {
    type Output = ChanneledAgentBus;

    fn wrap<T: AgentBus + 'static>(self, env: Rc<RealEnvironment>, bus: T) -> ChanneledAgentBus {
        ChanneledAgentBus::new_on_environment(env, move || bus)
    }
}

/// A `channeled_svc` `ChanneledAgentBus` that admits each operation through
/// the gate.
pub struct GatedChannel<G>(pub G);

impl<G: RequestGate + 'static> MigrationSwitchAgentBusWrapper for GatedChannel<G> {
    type Output = GatedAgentBus;

    fn wrap<T: AgentBus + 'static>(self, env: Rc<RealEnvironment>, bus: T) -> GatedAgentBus {
        GatedAgentBus::spawn_on(env, bus, self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use agentbus_api::EligibilityGate;
    use agentbus_api::RequestContext;
    use agentbus_core::AgentBus;
    use agentbus_core::AgentBusError;
    use agentbus_core::AppendRequest;
    use agentbus_core::BusId;
    use agentbus_core::Intention;
    use agentbus_core::NoopLogger;
    use agentbus_core::NoopMetrics;
    use agentbus_core::Payload;
    use agentbus_core::PollRequest;
    use agentbus_core::payload;

    use super::*;

    /// A factory that counts how many metrics and logger handles it builds.
    fn factory(builds: &Rc<Cell<usize>>) -> CoreAgentBusFactory {
        let (metrics_builds, logger_builds) = (builds.clone(), builds.clone());
        CoreAgentBusFactory::new(
            None,
            Box::new(move || {
                metrics_builds.set(metrics_builds.get() + 1);
                Ok(Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>)
            }),
            Box::new(move || {
                logger_builds.set(logger_builds.get() + 1);
                Ok(Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>)
            }),
        )
    }

    /// alice is eligible, bob is not, and carol's lookup fails.
    fn gate(env: &Rc<RealEnvironment>) -> EligibilityGate<RealEnvironment> {
        EligibilityGate::new(
            |unixname: String| async move {
                match unixname.as_str() {
                    "carol" => Err(anyhow::anyhow!("lookup failed")),
                    user => Ok(user == "alice"),
                }
            },
            env.clone(),
        )
    }

    fn bus_id() -> Option<BusId> {
        Some(BusId {
            agent_bus_id: "codex.alice.factory".to_owned(),
        })
    }

    fn append() -> AppendRequest {
        AppendRequest {
            bus_id: bus_id(),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention::default())),
            }),
            ..Default::default()
        }
    }

    fn poll() -> PollRequest {
        PollRequest {
            bus_id: bus_id(),
            max_entries: 10,
            ..Default::default()
        }
    }

    fn alice() -> RequestContext {
        RequestContext::default().with_user("alice")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_on_serves_requests_on_the_callers_executor() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let bus = factory(&Rc::default())
                    .build_on(Rc::new(RealEnvironment::new()), ParsedBackend::Memory)
                    .unwrap();
                bus.append(append()).await.unwrap();
                assert_eq!(bus.poll(poll()).await.unwrap().entries.len(), 1);
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_gated_on_admits_requests_through_the_gate() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let env = Rc::new(RealEnvironment::new());
                let bus = factory(&Rc::default())
                    .build_gated_on(env.clone(), ParsedBackend::Memory, gate(&env))
                    .unwrap();
                let bob = bus.for_caller(RequestContext::default().with_user("bob"));
                let carol = bus.for_caller(RequestContext::default().with_user("carol"));

                for error in [
                    bus.append(append()).await.expect_err("no caller"),
                    bob.append(append()).await.expect_err("bob is not eligible"),
                    bob.poll(poll()).await.expect_err("bob is not eligible"),
                ] {
                    assert!(
                        matches!(error, AgentBusError::InvalidArgument(_)),
                        "{error:?}"
                    );
                }
                let error = carol
                    .append(append())
                    .await
                    .expect_err("carol's lookup fails");
                assert!(matches!(error, AgentBusError::Unavailable(_)), "{error:?}");

                let alice = bus.for_caller(alice());
                alice.append(append()).await.expect("alice is eligible");
                assert_eq!(
                    alice.poll(poll()).await.unwrap().entries.len(),
                    1,
                    "only alice's append should apply"
                );
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn both_channels_build_the_same_observable_backend() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("bus.db").display().to_string();
                let env = Rc::new(RealEnvironment::new());

                let gated_builds = Rc::default();
                factory(&gated_builds)
                    .build_gated_on(env.clone(), ParsedBackend::Sqlite(path.clone()), gate(&env))
                    .unwrap()
                    .for_caller(alice())
                    .append(append())
                    .await
                    .expect("alice is eligible");

                let legacy_builds = Rc::default();
                let legacy = factory(&legacy_builds)
                    .build_on(env, ParsedBackend::Sqlite(path))
                    .unwrap();
                assert_eq!(
                    legacy.poll(poll()).await.unwrap().entries.len(),
                    1,
                    "the legacy channel should read what the gated channel wrote"
                );
                assert_eq!(
                    gated_builds, legacy_builds,
                    "both channels should observe the backend the same way"
                );
                assert_eq!(legacy_builds.get(), 2, "one metrics and one logger handle");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsupported_backends_fail_on_both_channels() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let env = Rc::new(RealEnvironment::new());
                let backend = || ParsedBackend::Other("unknown://bus".to_owned());
                assert!(
                    factory(&Rc::default())
                        .build_on(env.clone(), backend())
                        .is_err()
                );
                assert!(
                    factory(&Rc::default())
                        .build_gated_on(env.clone(), backend(), gate(&env))
                        .is_err()
                );
            })
            .await;
    }
}
