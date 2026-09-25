/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_core::AgentBusMetrics;
use agentbus_core::AgentbusLogger;
use agentbus_core::NoopLogger;
use agentbus_core::NoopMetrics;
use agentbus_core::server_lib::CommonServerArgs;
use agentbus_core::server_lib::ParsedBackend;
use agentbus_factory::ChanneledAgentBusFactory;
use agentbus_factory::CoreAgentBusFactory;
use anyhow::Result;
use clap::Parser;
use fbinit::FacebookInit;

#[derive(Parser, Debug)]
#[clap(name = "agent_bus_service", about = "AgentBus gRPC service")]
struct Args {
    #[clap(flatten)]
    common: CommonServerArgs,

    /// Backend URL (memory://, sqlite:///abs/path/to/db, http://host:port, or https://host:port)
    #[clap(long)]
    agentbus: Option<String>,
}

#[fbinit::main]
fn main(_fb: FacebookInit) -> Result<()> {
    agentbus_core::server_lib::init_logging();

    let args = Args::parse();
    let metrics = NoopMetrics;
    let backend = match &args.agentbus {
        Some(url) => agentbus_core::server_lib::parse_backend_url(url)?,
        None => ParsedBackend::Memory,
    };

    agentbus_core::server_lib::run_server(&args.common, metrics, move || {
        let factory = CoreAgentBusFactory::new(
            None,
            Box::new(|| Ok(Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>)),
            Box::new(|| Ok(Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>)),
        );
        factory
            .build(backend)
            .expect("Failed to initialize AgentBus backend")
    })
}
