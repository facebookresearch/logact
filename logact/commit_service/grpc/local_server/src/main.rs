/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use logact_local_server_lib::run_server;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;

#[derive(Parser, Debug)]
#[command(name = "logact_local_server")]
struct Args {
    #[arg(long, env = "LOGACT_SOCKET")]
    socket: PathBuf,

    #[arg(long, env = "LOGACT_SQLITE_PATH")]
    sqlite_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    agentbus_core::server_lib::init_logging();
    let args = Args::parse();
    run_server(args.socket, args.sqlite_path, shutdown_signal()?).await
}

fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    })
}
