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

impl ChanneledAgentBusFactory for CoreAgentBusFactory {
    fn build_on(
        self,
        env: Rc<RealEnvironment>,
        backend: ParsedBackend,
    ) -> Result<ChanneledAgentBus> {
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
                let metrics = Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>;
                let logger = Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>;
                let factory = CoreAgentBusFactory::new(
                    None,
                    Box::new(move || Ok(metrics.clone())),
                    Box::new(move || Ok(logger.clone())),
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
