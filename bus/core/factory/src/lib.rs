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
/// The boxed callable keeps `build()` non-generic to avoid large per-closure
/// AgentBus construction codegen. `build(self)` lets the worker own it.
pub type MakeMetrics = Box<dyn Fn() -> Result<Rc<dyn AgentBusMetrics>> + Send + 'static>;

/// Shared, type-erased logger factory used for worker-owned loggers.
pub type MakeLogger = Box<dyn Fn() -> Result<Rc<dyn AgentbusLogger>> + Send + 'static>;

/// Builds a type-erased `AgentBus` handle from a parsed backend.
pub trait ChanneledAgentBusFactory: Sized {
    /// Build the requested backend behind a channel-backed `AgentBus` handle.
    fn build(self, backend: ParsedBackend) -> Result<ChanneledAgentBus>;
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
    fn build(self, backend: ParsedBackend) -> Result<ChanneledAgentBus> {
        let Self {
            blocking_poll_interval,
            make_metrics,
            logger_factory,
        } = self;
        match backend {
            ParsedBackend::Memory => Ok(ChanneledAgentBus::new_in_process(move || {
                let bus_env = Rc::new(RealEnvironment::new());
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
            })),
            ParsedBackend::Http(grpc_url) => {
                let endpoint = tonic::transport::Channel::from_shared(grpc_url)?;
                Ok(ChanneledAgentBus::new_in_process(move || {
                    AgentBusClient::new(endpoint.connect_lazy())
                }))
            }
            ParsedBackend::Sqlite(path) => {
                let db = SqliteDb::open(Path::new(&path))?;
                let cws = SqliteConditionalWriteSpace::new(&db)?;
                Ok(ChanneledAgentBus::new_in_process(move || {
                    let bus_env = Rc::new(RealEnvironment::new());
                    let space = WriteOnceSpaceAdapter::new(cws);
                    let metrics = make_metrics()
                        .expect("write-once metrics factory should initialize in AgentBus worker");
                    let logger = logger_factory()
                        .expect("write-once logger factory should initialize in AgentBus worker");
                    let space =
                        ObservableWriteOnceSpace::new(space, metrics, logger, bus_env.clone());
                    WriteOnceAgentBus::new(space, bus_env, blocking_poll_interval)
                }))
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
