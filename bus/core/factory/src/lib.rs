/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use agentbus_conditional_write_space::WriteOnceSpaceAdapter;
use agentbus_core::AgentBusMetrics;
use agentbus_core::AgentbusLogger;
use agentbus_core::ChanneledAgentBus;
use agentbus_core::FeatureFlags;
use agentbus_core::RealEnvironment;
use agentbus_core::client::AgentBusClient;
use agentbus_core::server_lib::ParsedBackend;
use agentbus_sqlite::SqliteConditionalWriteSpace;
use agentbus_sqlite::SqliteDb;
use agentbus_writeonce::InMemoryWriteOnceSpace;
use agentbus_writeonce::ObservableWriteOnceSpace;
use agentbus_writeonce::WriteOnceAgentBus;
use anyhow::Result;

/// Shared, type-erased metrics factory.
///
/// The boxed callable keeps `build()` non-generic to avoid large per-closure
/// AgentBus construction codegen. `build(self)` lets the worker own it.
pub type MakeMetrics = Box<dyn Fn() -> Result<Rc<dyn AgentBusMetrics>> + Send + 'static>;

/// Shared, type-erased logger factory used for worker-owned loggers.
pub type MakeLogger = Box<dyn Fn() -> Result<Rc<dyn AgentbusLogger>> + Send + 'static>;

/// Builds a type-erased `AgentBus` handle from a parsed backend.
pub trait ChanneledAgentBusFactory: Sized {
    /// Build the requested backend behind a channel-backed `AgentBus` handle.
    #[deprecated(note = "Use `build_on`, which takes the caller's environment.")]
    fn build(self, backend: ParsedBackend) -> Result<ChanneledAgentBus>;

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
    feature_flags: FeatureFlags,
}

impl CoreAgentBusFactory {
    pub fn with_feature_flags(mut self, feature_flags: FeatureFlags) -> Self {
        self.feature_flags = feature_flags;
        self
    }

    pub fn new(
        blocking_poll_interval: Option<Duration>,
        make_metrics: MakeMetrics,
        logger_factory: MakeLogger,
    ) -> Self {
        Self {
            blocking_poll_interval,
            make_metrics,
            logger_factory,
            feature_flags: FeatureFlags::default(),
        }
    }
}

impl ChanneledAgentBusFactory for CoreAgentBusFactory {
    #[expect(
        deprecated,
        reason = "legacy worker-thread path; deleted once enable_single_env is on"
    )]
    fn build(self, backend: ParsedBackend) -> Result<ChanneledAgentBus> {
        let Self {
            blocking_poll_interval,
            make_metrics,
            logger_factory,
            feature_flags,
        } = self;
        match backend {
            ParsedBackend::Memory => Ok(ChanneledAgentBus::new_in_process_with_feature_flags(
                feature_flags,
                move |bus_env| {
                    let metrics = make_metrics()
                        .expect("write-once metrics factory should initialize in AgentBus worker");
                    let logger = logger_factory()
                        .expect("write-once logger factory should initialize in AgentBus worker");
                    let space = ObservableWriteOnceSpace::new(
                        InMemoryWriteOnceSpace::new(),
                        metrics,
                        logger,
                        bus_env.clone(),
                    );
                    WriteOnceAgentBus::new(space, bus_env, blocking_poll_interval)
                },
            )),
            ParsedBackend::Http(grpc_url) => {
                let endpoint = tonic::transport::Channel::from_shared(grpc_url)?;
                Ok(ChanneledAgentBus::new_in_process_with_feature_flags(
                    feature_flags,
                    move |_| AgentBusClient::new(endpoint.connect_lazy()),
                ))
            }
            ParsedBackend::Sqlite(path) => {
                let db = SqliteDb::open(Path::new(&path))?;
                let cws = SqliteConditionalWriteSpace::new(&db)?;
                Ok(ChanneledAgentBus::new_in_process_with_feature_flags(
                    feature_flags,
                    move |bus_env| {
                        let space = WriteOnceSpaceAdapter::new(cws);
                        let metrics = make_metrics().expect(
                            "write-once metrics factory should initialize in AgentBus worker",
                        );
                        let logger = logger_factory().expect(
                            "write-once logger factory should initialize in AgentBus worker",
                        );
                        let space =
                            ObservableWriteOnceSpace::new(space, metrics, logger, bus_env.clone());
                        WriteOnceAgentBus::new(space, bus_env, blocking_poll_interval)
                    },
                ))
            }
            ParsedBackend::Other(url) => {
                anyhow::bail!(
                    "Unsupported URL '{}'. Supported schemes: memory://, http://, https://, sqlite://",
                    url
                );
            }
        }
    }

    fn build_on(
        self,
        env: Rc<RealEnvironment>,
        backend: ParsedBackend,
    ) -> Result<ChanneledAgentBus> {
        let Self {
            blocking_poll_interval,
            make_metrics,
            logger_factory,
            feature_flags: _,
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
                Ok(ChanneledAgentBus::new_on_environment(env, move || bus))
            }
            ParsedBackend::Http(grpc_url) => {
                let endpoint = tonic::transport::Channel::from_shared(grpc_url)?;
                Ok(ChanneledAgentBus::new_on_environment(env, move || {
                    AgentBusClient::new(endpoint.connect_lazy())
                }))
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
                Ok(ChanneledAgentBus::new_on_environment(env, move || bus))
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

#[cfg(test)]
mod tests {
    use agentbus_core::AgentBus;
    use agentbus_core::AppendRequest;
    use agentbus_core::BusId;
    use agentbus_core::Intention;
    use agentbus_core::NoopLogger;
    use agentbus_core::NoopMetrics;
    use agentbus_core::Payload;
    use agentbus_core::PollRequest;
    use agentbus_core::payload;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn build_on_serves_requests_on_the_callers_executor() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let factory = CoreAgentBusFactory::new(
                    None,
                    Box::new(|| Ok(Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>)),
                    Box::new(|| Ok(Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>)),
                );
                let bus = factory
                    .build_on(Rc::new(RealEnvironment::new()), ParsedBackend::Memory)
                    .unwrap();
                let bus_id = Some(BusId {
                    agent_bus_id: "build-on".to_owned(),
                });
                bus.append(AppendRequest {
                    bus_id: bus_id.clone(),
                    payload: Some(Payload {
                        payload: Some(payload::Payload::Intention(Intention::default())),
                    }),
                    ..Default::default()
                })
                .await
                .unwrap();
                let polled = bus
                    .poll(PollRequest {
                        bus_id,
                        max_entries: 10,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                assert_eq!(polled.entries.len(), 1);
            })
            .await;
    }
}
