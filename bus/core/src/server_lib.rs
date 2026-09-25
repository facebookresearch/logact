/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AgentBus server shared library
//!
//! Provides common server functionality shared by AgentBus server frontends.

use std::io::IsTerminal;
use std::io::stderr;
use std::net::SocketAddr;

use anyhow::Result;
use futures::StreamExt;
use signal_hook::consts::signal::SIGINT;
use signal_hook::consts::signal::SIGTERM;
use signal_hook_tokio::Signals;
use tracing::info;
use tracing_glog::Glog;
use tracing_glog::GlogFields;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

use crate::AgentBusHandler;
use crate::AgentBusMetrics;
use crate::AgentBusServiceServer;
use crate::ChanneledAgentBus;

/// Parsed backend URL scheme.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedBackend {
    /// In-memory backend (`memory://`)
    Memory,
    /// HTTP/HTTPS relay to an upstream gRPC server (`http://` or `https://`)
    Http(String),
    /// SQLite file backend (`sqlite:///abs/path/to/db`)
    Sqlite(String),
    /// Unrecognized scheme; callers can handle additional schemes.
    Other(String),
}

/// Parse a backend URL into a known scheme.
pub fn parse_backend_url(url: &str) -> Result<ParsedBackend> {
    let (scheme, _) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("Invalid URL '{}': expected <scheme>://...", url))?;
    match scheme {
        "memory" => Ok(ParsedBackend::Memory),
        "http" | "https" => Ok(ParsedBackend::Http(url.to_string())),
        "sqlite" => {
            let path = url.strip_prefix("sqlite://").expect("already split on ://");
            if path.is_empty() {
                anyhow::bail!("sqlite:// requires a file path, e.g. sqlite:///abs/path/to/db");
            }
            Ok(ParsedBackend::Sqlite(path.to_string()))
        }
        _ => Ok(ParsedBackend::Other(url.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_backend_url_preserves_unknown_url() {
        assert_eq!(
            parse_backend_url("custom://backend").unwrap(),
            ParsedBackend::Other("custom://backend".to_owned())
        );
    }

    #[test]
    fn test_parse_backend_url_rejects_missing_scheme() {
        let error = parse_backend_url("not-a-url").unwrap_err();
        let error = format!("{error:#}");
        assert!(
            error.contains("expected <scheme>://"),
            "unexpected parse error: {error}"
        );
    }
}

/// Common server arguments shared by all AgentBus server variants.
/// Use `#[clap(flatten)]` to embed these in your binary's `Args` struct.
#[derive(clap::Args, Debug)]
pub struct CommonServerArgs {
    /// Port to listen on for gRPC connections
    #[clap(short, long, env = "AGENT_BUS_PORT", default_value = "9999")]
    pub port: u16,

    /// Address to bind the gRPC server to
    #[clap(long, default_value = "[::]")]
    pub host: String,
}

pub fn init_logging() {
    let fmt = tracing_subscriber::fmt::Layer::default()
        .with_ansi(stderr().is_terminal())
        .with_writer(std::io::stderr)
        .event_format(Glog::default().with_timer(tracing_glog::LocalTime::default()))
        .fmt_fields(GlogFields::default());

    let filter = EnvFilter::from_default_env();

    let subscriber = Registry::default().with(filter).with(fmt);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");
}

/// Like [`init_logging`] but stacks an additional tracing layer on top of the
/// default Glog formatter.
pub fn init_logging_with_layer<L>(extra: L)
where
    L: tracing_subscriber::Layer<Registry> + Send + Sync + 'static,
{
    let fmt = tracing_subscriber::fmt::Layer::default()
        .with_ansi(stderr().is_terminal())
        .with_writer(std::io::stderr)
        .event_format(Glog::default().with_timer(tracing_glog::LocalTime::default()))
        .fmt_fields(GlogFields::default());

    let filter = EnvFilter::from_default_env();

    // `extra` composes directly with Registry so its Layer bound is simple.
    // EnvFilter's `enabled()` propagates globally regardless of ordering.
    let subscriber = Registry::default().with(extra).with(filter).with(fmt);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");
}

async fn serve_grpc_service_with_shutdown<F>(
    handler: AgentBusHandler,
    host: &str,
    port: u16,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;
    let service = AgentBusServiceServer::new(handler);

    tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown)
        .await?;
    Ok(())
}

async fn run_grpc_service(handler: AgentBusHandler, host: &str, port: u16) -> Result<()> {
    let mut signals = Signals::new([SIGTERM, SIGINT])?;

    serve_grpc_service_with_shutdown(handler, host, port, async move {
        signals.next().await;
        info!("Shutting down...");
        signals.handle().close();
    })
    .await
}

/// Run the AgentBus server with the given channel-backed bus factory.
///
/// This is the main entry point for AgentBus server frontends.
/// The `make_bus` closure creates the storage backend behind a channel-backed
/// handle.
pub fn run_server<F, M>(args: &CommonServerArgs, _metrics: M, make_bus: F) -> Result<()>
where
    F: FnOnce() -> ChanneledAgentBus + Send + 'static,
    M: AgentBusMetrics + Send,
{
    let port = args.port;
    let host = args.host.clone();

    let handler = AgentBusHandler::new(make_bus());

    info!(port = port, host = %host, "Starting AgentBusService gRPC service");

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run_grpc_service(handler, &host, port))
}
