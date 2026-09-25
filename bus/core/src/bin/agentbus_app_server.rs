/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Standalone AppServer binary.
//!
//! Runs the decider, voter, and mailbox polling loops internally,
//! and exposes a gRPC API for gate checks, mail, append, and poll.

use std::rc::Rc;
use std::time::Duration;

use agentbus_core::AgentBusMetrics;
use agentbus_core::AgentbusLogger;
use agentbus_core::NoopLogger;
use agentbus_core::NoopMetrics;
use agentbus_core::RealEnvironment;
use agentbus_core::appserver::AppServerConfig;
use agentbus_core::appserver::build_and_spawn;
use agentbus_core::appserver::grpc::run_grpc_server;
use agentbus_core::appserver::grpc::spawn_grpc_worker;
use agentbus_core::config::AppServerConfigFile;
use agentbus_core::config::InitialVoterConfig;
use agentbus_core::server_lib::ParsedBackend;
use agentbus_factory::ChanneledAgentBusFactory;
use agentbus_factory::CoreAgentBusFactory;
use anyhow::Result;
use clap::Parser;
use fbinit::FacebookInit;
use tracing::info;

#[derive(Parser, Debug)]
#[clap(name = "agentbus_app_server", about = "AgentBus AppServer (gRPC)")]
struct Args {
    /// Backend URL (memory://, sqlite:///path, http://host:port for upstream gRPC)
    #[clap(long)]
    agentbus: String,

    /// gRPC port to listen on
    #[clap(long, default_value = "8080")]
    port: u16,

    /// Address to bind the gRPC server to
    #[clap(long, default_value = "[::]")]
    host: String,

    /// Path to YAML config file. When provided, all other config flags
    /// (bus_id, timeouts, voter flags) are ignored.
    #[clap(long, conflicts_with_all = &["bus_id", "run_voter"])]
    config: Option<String>,

    /// Bus ID to operate on (required without --config)
    #[clap(long, required_unless_present = "config")]
    bus_id: Option<String>,

    /// Gate check timeout in milliseconds
    #[clap(long, default_value = "30000")]
    gate_check_timeout_ms: u64,

    /// Internal polling interval in milliseconds for WriteOnceSpace-backed buses.
    /// Controls how often the backing store is polled inside blocking_poll.
    /// Ignored for non-WriteOnce backends (which use waiters instead).
    #[clap(long)]
    blocking_poll_interval_ms: Option<u64>,

    /// Enable a single LLM voter (legacy; prefer --config for multiple voters)
    #[clap(long)]
    run_voter: bool,

    /// Voter API endpoint override
    #[clap(long, env = "VOTER_API_ENDPOINT")]
    voter_api_endpoint: Option<String>,

    /// Voter API key override
    #[clap(long, env = "VOTER_API_KEY")]
    voter_api_key: Option<String>,

    /// Voter LLM model override
    #[clap(long, env = "VOTER_MODEL")]
    voter_model: Option<String>,
}

/// Build `(AppServerConfig, voter_builder)` from either `--config` file or
/// CLI flags. The two modes are mutually exclusive (enforced by clap).
///
/// Returns a Send closure that builds !Send voters inside a LocalSet.
fn make_app_config(
    args: &Args,
) -> Result<(
    AppServerConfig,
    Box<dyn FnOnce() -> Vec<Box<dyn agentbus_core::voter::Voter>> + Send>,
)> {
    let (config, voter_configs) = if let Some(ref path) = args.config {
        let file_config = AppServerConfigFile::from_path(path)?;
        file_config.into_config()?
    } else {
        let config = AppServerConfig {
            bus_id: args.bus_id.clone().unwrap_or_default(),
            gate_check_timeout: Duration::from_millis(args.gate_check_timeout_ms),
            blocking_poll_interval: args.blocking_poll_interval_ms.map(Duration::from_millis),
        };
        let mut voter_configs = Vec::new();
        if args.run_voter {
            voter_configs.push(InitialVoterConfig::Llm {
                config: llm_voter_proto_rust::llm_voter::LlmVoterConfig {
                    prompt_override: String::new(),
                },
                model: args.voter_model.clone(),
                api_endpoint: args.voter_api_endpoint.clone(),
            });
        }
        (config, voter_configs)
    };

    // Capture api_key for all LLM voters (shared CLI/env secret)
    let api_key = args.voter_api_key.clone();

    let voter_builder: Box<dyn FnOnce() -> Vec<Box<dyn agentbus_core::voter::Voter>> + Send> =
        Box::new(move || {
            voter_configs
                .into_iter()
                .map(|config| -> Box<dyn agentbus_core::voter::Voter> {
                    match config {
                        InitialVoterConfig::Llm {
                            config: llm_cfg,
                            model,
                            api_endpoint,
                        } => Box::new(agentbus_voter_llm::from_typed_config(
                            llm_cfg,
                            api_key.clone(),
                            model,
                            api_endpoint,
                        )),
                        InitialVoterConfig::RuleBased(rb_cfg) => Box::new(
                            agentbus_voter_rule_based::from_typed_config(rb_cfg).unwrap_or_else(
                                |e| panic!("Failed to build rule-based voter: {}", e),
                            ),
                        ),
                    }
                })
                .collect()
        });

    Ok((config, voter_builder))
}

#[fbinit::main]
fn main(_fb: FacebookInit) -> Result<()> {
    agentbus_core::server_lib::init_logging();

    let args = Args::parse();
    let port = args.port;
    let backend = agentbus_core::server_lib::parse_backend_url(&args.agentbus)?;
    let (config, voter_builder) = make_app_config(&args)?;
    let host = args.host;

    info!(
        agentbus = %args.agentbus,
        port = port,
        bus_id = %config.bus_id,
        "Starting agentbus_app_server"
    );

    run_with_bus(&host, port, backend, config, voter_builder)
}

fn run_with_bus(
    host: &str,
    port: u16,
    backend: ParsedBackend,
    config: AppServerConfig,
    voter_builder: Box<dyn FnOnce() -> Vec<Box<dyn agentbus_core::voter::Voter>> + Send>,
) -> Result<()> {
    let blocking_poll_interval = config.blocking_poll_interval;

    // Spawn AppServer on a dedicated thread with single-threaded runtime.
    // This isolates the Rc-based AppServer, decider, mailbox, and voter from
    // the gRPC server, preventing starvation of background loops.
    let (handler_tx, handler_rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for AppServer worker");

        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async move {
            let env = Rc::new(RealEnvironment::new());
            let factory = CoreAgentBusFactory::new(
                blocking_poll_interval,
                Box::new(|| Ok(Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>)),
                Box::new(|| Ok(Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>)),
            );
            let bus = factory
                .build(backend)
                .expect("Failed to initialize AgentBus backend");
            let initial_voters = voter_builder();
            let app = build_and_spawn(bus, config, env, initial_voters);
            let app = Rc::new(app);

            let handler = spawn_grpc_worker(app);
            let _ = handler_tx.send(handler);

            // Keep the LocalSet alive so background loops (decider, mailbox, voter) run.
            std::future::pending::<()>().await;
        });
    });

    let handler = handler_rx
        .recv()
        .map_err(|_| anyhow::anyhow!("AppServer worker thread failed to start"))?;

    // Run gRPC server on a multi-threaded runtime with graceful shutdown.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_grpc_server(handler, host, port))?;

    Ok(())
}
