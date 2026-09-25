/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Manual `commit` subcommand: send one Event from command-line flags.

use agent_bus_proto_rust::agent_bus as agentbus;
use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BusId;
use anyhow::Result;
use anyhow::bail;
use clap::Args;
use clap::Subcommand;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitSvc;

use crate::hooks::action_output_payload;
use crate::hooks::agent_input_payload;
use crate::hooks::agent_output_payload;

/// Arguments for submitting one event manually.
#[derive(Args, Debug)]
pub struct CommitArgs {
    #[arg(long)]
    pub agent_id: String,

    #[command(subcommand)]
    pub event: EventArg,
}

/// Event accepted by the manual `commit` command.
#[derive(Subcommand, Debug)]
pub enum EventArg {
    AgentInput { body: String },
    Intention { body: String },
    ActionOutput { body: String },
    AgentOutput { body: String },
}

/// Submit one manually constructed event.
pub async fn run<C: CommitSvc + ?Sized>(commit_client: &C, args: CommitArgs) -> Result<()> {
    let CommitArgs { agent_id, event } = args;
    match event {
        EventArg::Intention { body } => {
            let outcome = commit_client
                .commit_intention(CommitIntentionCommand {
                    bus_id: BusId {
                        agent_bus_id: agent_id,
                    },
                    intention: agentbus::intention::Intention::StringIntention(body),
                })
                .await?;
            println!("approved={} reason={:?}", outcome.approved, outcome.reason);
            if !outcome.approved {
                bail!("commit rejected");
            }
        }
        EventArg::AgentInput { body } => {
            append_payload(
                commit_client.agent_bus(),
                agent_id,
                agent_input_payload(body),
            )
            .await?
        }
        EventArg::ActionOutput { body } => {
            append_payload(
                commit_client.agent_bus(),
                agent_id,
                action_output_payload(body),
            )
            .await?
        }
        EventArg::AgentOutput { body } => {
            append_payload(
                commit_client.agent_bus(),
                agent_id,
                agent_output_payload(body),
            )
            .await?
        }
    }
    Ok(())
}

async fn append_payload<B: agentbus_api::AgentBus + ?Sized>(
    bus: &B,
    agent_id: String,
    payload: agentbus::Payload,
) -> Result<()> {
    bus.append(AppendRequest {
        agent_bus_id: agent_id.clone(),
        bus_id: Some(BusId {
            agent_bus_id: agent_id,
        }),
        payload: Some(payload),
    })
    .await?;
    println!("committed");
    Ok(())
}
