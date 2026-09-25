/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Portable command-line client for a local LogAct server.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use logact_commit_cli_lib::claude_hooks;
use logact_commit_cli_lib::codex_hooks;
use logact_commit_cli_lib::manual;
use logact_commit_cli_lib::muse_hooks;
use logact_commit_service_grpc::GrpcCommitSvcClient;
use tonic::transport::Endpoint;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 60;

#[derive(Parser, Debug)]
#[command(name = "logact_commit_cli")]
struct Cli {
    /// Unix-domain socket exposed by `logact_local_server`.
    #[arg(long, env = "LOGACT_SOCKET")]
    socket: PathBuf,

    /// Maximum time for one gRPC request to the local LogAct server.
    #[arg(
        long,
        env = "LOGACT_REQUEST_TIMEOUT_SECS",
        default_value_t = DEFAULT_REQUEST_TIMEOUT_SECS
    )]
    request_timeout_secs: u64,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Send one event from command-line flags.
    Commit(manual::CommitArgs),
    /// Handle a Claude Code hook event from stdin.
    ClaudeHook(claude_hooks::HookArgs),
    /// Handle a Codex hook event from stdin.
    CodexHook(codex_hooks::HookArgs),
    /// Handle a Muse Code hook event from stdin.
    MuseHook(muse_hooks::HookArgs),
}

#[tokio::main]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) if !error.use_stderr() => error.exit(),
        Err(error) => {
            let _ = error.print();
            std::process::exit(1);
        }
    };
    if let Err(error) = run(cli).await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let endpoint = Endpoint::from_shared(format!("unix://{}", cli.socket.display()))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(Duration::from_secs(cli.request_timeout_secs));
    let client = GrpcCommitSvcClient::new(
        endpoint
            .connect()
            .await
            .with_context(|| format!("failed to connect to {}", cli.socket.display()))?,
    );
    match cli.cmd {
        Cmd::Commit(args) => manual::run(&client, args).await,
        Cmd::ClaudeHook(args) => claude_hooks::run(&client, args, "claude_code", true)
            .await
            .map_err(Into::into),
        Cmd::CodexHook(args) => codex_hooks::run(&client, args, "codex", true)
            .await
            .map_err(Into::into),
        Cmd::MuseHook(args) => muse_hooks::run(&client, args, "muse", true)
            .await
            .map_err(Into::into),
    }
}
