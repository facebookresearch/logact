/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AgentBus CLI - Command-line tool for interacting with AgentBus backends

use agentbus_cli_lib::CommonArgs;
use anyhow::Result;
use clap::Parser;
use fbinit::FacebookInit;

/// AgentBus CLI - Command-line interface for AgentBus operations
#[derive(Parser, Debug)]
#[clap(name = "agentbus", about = "AgentBus command-line interface")]
struct Args {
    #[clap(flatten)]
    common: CommonArgs,
}

#[fbinit::main]
async fn main(_fb: FacebookInit) -> Result<()> {
    agentbus_cli_lib::init_logging();

    let args = Args::parse();

    agentbus_cli_lib::run_common(args.common).await
}
