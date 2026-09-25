/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AgentBus CLI shared library
//!
//! Provides commands, REPL, and helpers shared by AgentBus CLI frontends.

use std::io::IsTerminal;
use std::io::stderr;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBus;
use agentbus_api::pack_any;
use agentbus_api::unpack_any;
use agentbus_core::AgentBusMetrics;
use agentbus_core::AgentbusLogger;
use agentbus_core::NoopLogger;
use agentbus_core::NoopMetrics;
use agentbus_core::server_lib::ParsedBackend;
use agentbus_core::server_lib::parse_backend_url;
use agentbus_factory::ChanneledAgentBusFactory;
use agentbus_factory::CoreAgentBusFactory;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use clap::Subcommand;
use colored::Colorize;
use tracing::info;
use tracing_glog::Glog;
use tracing_glog::GlogFields;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// Common CLI arguments shared by all AgentBus CLI variants.
/// Use `#[clap(flatten)]` to embed these in your binary's `Args` struct.
#[derive(clap::Args, Debug)]
pub struct CommonArgs {
    /// Agent bus ID to operate on
    #[clap(long, env = "AGENT_BUS_ID")]
    pub agent_bus_id: String,

    /// Backend URL to operate on
    #[clap(long, env = "AGENTBUS")]
    pub agentbus: String,

    #[clap(subcommand)]
    pub command: Commands,
}

/// Run the CLI using the configured `--agentbus` backend URL.
pub async fn run_common(args: CommonArgs) -> Result<()> {
    run_agentbus_url(&args.agentbus, args.agent_bus_id, args.command).await
}

/// Run a CLI command against a direct AgentBus backend URL.
pub async fn run_agentbus_url(
    agentbus_url: &str,
    agent_bus_id: String,
    command: Commands,
) -> Result<()> {
    let factory = CoreAgentBusFactory::new(
        None,
        Box::new(|| Ok(Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>)),
        Box::new(|| Ok(Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>)),
    );
    run_agentbus_url_with_factory(factory, agentbus_url, agent_bus_id, command).await
}

/// Run a CLI command against a backend URL using the supplied AgentBus factory.
pub async fn run_agentbus_url_with_factory<F>(
    factory: F,
    agentbus_url: &str,
    agent_bus_id: String,
    command: Commands,
) -> Result<()>
where
    F: ChanneledAgentBusFactory,
{
    let backend = parse_backend_url(agentbus_url)?;
    match &backend {
        ParsedBackend::Memory => {
            anyhow::bail!(
                "memory:// is not supported by the AgentBus CLI. Use sqlite:///abs/path/to/db for local testing."
            );
        }
        ParsedBackend::Http(grpc_url) => {
            info!(
                agent_bus_id = %agent_bus_id,
                url = %grpc_url,
                "Connecting to AgentBus gRPC backend"
            );
        }
        ParsedBackend::Sqlite(path) => {
            info!(
                agent_bus_id = %agent_bus_id,
                path = %path,
                "Using SQLite AgentBus backend"
            );
        }
        ParsedBackend::Other(_) => {}
    }

    let bus = factory.build(backend)?;
    run(&bus, agent_bus_id, command).await
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Append a new intention to the AgentBus
    Intention {
        /// The intention string to append
        #[clap(value_name = "INTENTION")]
        intention: String,
    },
    /// Append a decider policy change to the AgentBus
    DeciderPolicy {
        /// The decider policy to set (OFF_BY_DEFAULT, ON_BY_DEFAULT, or FIRST_BOOLEAN_WINS)
        #[clap(value_name = "POLICY")]
        policy: String,
    },
    /// Append a voter policy change to the AgentBus
    VoterPolicy {
        /// Prompt override broadcast to all LLM voters
        #[clap(value_name = "PROMPT_OVERRIDE")]
        prompt_update: String,
    },
    /// Poll entries from the AgentBus
    Poll {
        /// Starting log position
        #[clap(long)]
        start: i64,

        /// Maximum number of entries to retrieve
        #[clap(long)]
        limit: i16,
    },
    /// Print the tail position of the AgentBus
    CheckTail,
    /// Start interactive REPL mode
    Repl,
}

/// Run the CLI with the given bus, agent_bus_id, and command.
pub async fn run(bus: &impl AgentBus, agent_bus_id: String, command: Commands) -> Result<()> {
    match command {
        Commands::Intention { intention } => {
            append_intention(bus, &agent_bus_id, &intention).await?;
        }
        Commands::DeciderPolicy { policy } => {
            append_decider_policy(bus, &agent_bus_id, &policy).await?;
        }
        Commands::VoterPolicy { prompt_update } => {
            append_voter_policy(bus, &agent_bus_id, &prompt_update).await?;
        }
        Commands::Poll { start, limit } => {
            poll_entries(bus, &agent_bus_id, start, limit, None).await?;
        }
        Commands::CheckTail => {
            check_tail(bus, &agent_bus_id).await?;
        }
        Commands::Repl => {
            run_repl(bus, agent_bus_id).await?;
        }
    }
    Ok(())
}

/// Append a new intention to the AgentBus
async fn append_intention(
    bus: &impl AgentBus,
    agent_bus_id: &str,
    intention_str: &str,
) -> Result<()> {
    info!(intention = %intention_str, "Appending intention");

    let payload = Payload {
        payload: Some(payload::Payload::Intention(Intention {
            intention: Some(intention::Intention::StringIntention(
                intention_str.to_owned(),
            )),
            ..Default::default()
        })),
    };
    let request = AppendRequest {
        agent_bus_id: agent_bus_id.to_owned(),
        bus_id: Some(BusId {
            agent_bus_id: agent_bus_id.to_owned(),
        }),
        payload: Some(payload),
        ..Default::default()
    };

    let response = bus
        .append(request)
        .await
        .context("Failed to append intention")?;

    println!(
        "✓ Intention appended successfully at log position: {}",
        response.log_position
    );
    println!("  Intention: \"{}\"", intention_str);
    println!("  Agent Bus ID: {}", agent_bus_id);

    Ok(())
}

/// Append a decider policy change to the AgentBus
async fn append_decider_policy(
    bus: &impl AgentBus,
    agent_bus_id: &str,
    policy_str: &str,
) -> Result<()> {
    info!(decider_policy = %policy_str, "Appending decider policy");

    // Parse the decider policy string
    let decider_policy = match policy_str.to_uppercase().as_str() {
        "OFF_BY_DEFAULT" | "OFF" => DeciderPolicy::OffByDefault as i32,
        "ON_BY_DEFAULT" | "ON" => DeciderPolicy::OnByDefault as i32,
        "FIRST_BOOLEAN_WINS" | "FIRST" => DeciderPolicy::FirstBooleanWins as i32,
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid decider policy: {}. Valid options: OFF_BY_DEFAULT, ON_BY_DEFAULT, FIRST_BOOLEAN_WINS",
                policy_str
            ));
        }
    };

    let payload = Payload {
        payload: Some(payload::Payload::DeciderPolicy(decider_policy)),
    };
    let request = AppendRequest {
        agent_bus_id: agent_bus_id.to_owned(),
        bus_id: Some(BusId {
            agent_bus_id: agent_bus_id.to_owned(),
        }),
        payload: Some(payload),
        ..Default::default()
    };

    let response = bus
        .append(request)
        .await
        .context("Failed to append policy")?;

    println!(
        "✓ Decider policy appended successfully at log position: {}",
        response.log_position
    );
    println!("  Decider Policy: {}", policy_str);
    println!("  Agent Bus ID: {}", agent_bus_id);

    Ok(())
}

/// Append a voter policy change to the AgentBus
async fn append_voter_policy(
    bus: &impl AgentBus,
    agent_bus_id: &str,
    prompt_update: &str,
) -> Result<()> {
    info!(prompt_update = %prompt_update, "Appending voter policy");

    let llm_config = llm_voter_proto_rust::llm_voter::LlmVoterConfig {
        prompt_override: prompt_update.to_owned(),
    };
    let voter_policy = VoterPolicy {
        config: Some(pack_any(&llm_config)),
        ..Default::default()
    };
    let payload = Payload {
        payload: Some(payload::Payload::VoterPolicy(voter_policy)),
    };
    let request = AppendRequest {
        agent_bus_id: agent_bus_id.to_owned(),
        bus_id: Some(BusId {
            agent_bus_id: agent_bus_id.to_owned(),
        }),
        payload: Some(payload),
        ..Default::default()
    };

    let response = bus.append(request).await.map_err(|e| {
        eprintln!("Error: Failed to append voter policy");
        eprintln!("Exception details: {:#?}", e);
        anyhow::anyhow!("Failed to append voter policy: {}", e)
    })?;

    println!(
        "✓ Voter policy appended successfully at log position: {}",
        response.log_position
    );
    println!("  Prompt Override: \"{}\"", prompt_update);
    println!("  Agent Bus ID: {}", agent_bus_id);

    Ok(())
}

fn decider_policy_name(policy: i32) -> &'static str {
    DeciderPolicy::try_from(policy)
        .map(|policy| policy.as_str_name())
        .unwrap_or("Unknown")
}

fn voter_config_description(config: &VoterConfig) -> String {
    match config.config.as_ref() {
        Some(voter_config::Config::Llm(config)) if config.prompt_override.is_empty() => {
            "llm".to_string()
        }
        Some(voter_config::Config::Llm(config)) => {
            format!("llm(prompt_override={:?})", config.prompt_override)
        }
        Some(voter_config::Config::RuleBased(config)) => {
            format!("rule_based(rules={})", config.rules.len())
        }
        Some(voter_config::Config::Custom(any)) => format!("custom(type_url={})", any.type_url),
        None => "none".to_string(),
    }
}

fn format_timestamp(rt_timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(rt_timestamp_ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| format!("{}ms", rt_timestamp_ms))
}

/// Print a single bus entry
fn print_entry(entry: &BusEntry) {
    let header = entry.header.as_ref();
    let position = header.map(|h| h.log_position).unwrap_or(0);
    let ts = header.map(|h| h.rt_timestamp_ms).unwrap_or(0);
    if ts > 0 {
        print!("[Position {} @ {}] ", position, format_timestamp(ts));
    } else {
        print!("[Position {}] ", position);
    }

    if let Some(ref payload) = entry.payload {
        match &payload.payload {
            Some(payload::Payload::Intention(intention)) => {
                println!("{}", "INTENTION:".bright_cyan());
                if let Some(ref int) = intention.intention {
                    match int {
                        intention::Intention::StringIntention(s) => {
                            for line in s.lines() {
                                println!("\t{}", line.white().on_black());
                            }
                        }
                    }
                } else {
                    println!("<unknown>");
                }
            }
            Some(payload::Payload::Vote(vote)) => {
                if let Some(ref vote_type) = vote.abstract_vote {
                    if let Some(vote_type::VoteType::BooleanVote(b)) = vote_type.vote_type {
                        let colored_vote_tag = if b {
                            "VOTE:".bright_green()
                        } else {
                            "VOTE:".red()
                        };
                        print!(
                            "{} intention_id={}, vote=",
                            colored_vote_tag, vote.intention_id
                        );
                        println!("{}", b);
                        if !vote.reason.is_empty() {
                            let colored_reason = if b {
                                vote.reason.as_str().bright_green()
                            } else {
                                vote.reason.as_str().red()
                            };
                            println!("\tReason: {}", colored_reason);
                        }
                        if let Some(ref config) = vote.voter_config {
                            println!("\tConfig: {}", voter_config_description(config));
                        }
                    } else {
                        print!(
                            "{} intention_id={}, vote=",
                            "VOTE:".blue(),
                            vote.intention_id
                        );
                        println!("<unknown>");
                    }
                } else {
                    print!(
                        "{} intention_id={}, vote=",
                        "VOTE:".blue(),
                        vote.intention_id
                    );
                    println!("<unknown>");
                }
            }
            Some(payload::Payload::Commit(commit)) => {
                println!(
                    "{} intention_id={}",
                    "COMMIT:".bright_green(),
                    commit.intention_id
                );
                println!("\tReason: {}", commit.reason.bright_green());
            }
            Some(payload::Payload::Abort(abort)) => {
                println!("{} intention_id={}", "ABORT:".red(), abort.intention_id);
                println!("\tReason: {}", abort.reason.red());
            }
            Some(payload::Payload::DeciderPolicy(decider_policy)) => {
                println!(
                    "{} {}",
                    "DECIDER_POLICY:".yellow(),
                    decider_policy_name(*decider_policy)
                );
            }
            Some(payload::Payload::VoterPolicy(voter_policy)) => {
                print!("{}", "VOTER_POLICY:".magenta());
                match voter_policy.config.as_ref() {
                    Some(config) => {
                        if let Some(llm_config) =
                            unpack_any::<llm_voter_proto_rust::llm_voter::LlmVoterConfig>(config)
                        {
                            println!(" llm(prompt_override={:?})", llm_config.prompt_override,);
                        } else {
                            println!(" type={}", config.type_url);
                        }
                    }
                    None => println!(" <no config>"),
                }
            }
            Some(payload::Payload::Control(control)) => {
                println!("{}", "CONTROL:".bright_magenta());
                if let Some(ref ctrl) = control.control {
                    match ctrl {
                        control::Control::BaseEngineControl(control) => {
                            match control.control.as_ref() {
                                Some(base_engine_control::Control::AddVoter(add)) => {
                                    let config_description = add
                                        .config
                                        .as_ref()
                                        .map(voter_config_description)
                                        .unwrap_or_else(|| "none".to_string());
                                    println!(
                                        "\tBaseEngine: add_voter config={}",
                                        config_description
                                    );
                                }
                                Some(base_engine_control::Control::RemoveVoter(remove)) => {
                                    println!(
                                        "\tBaseEngine: remove_voter voter_id={}",
                                        remove.voter_id
                                    );
                                }
                                Some(base_engine_control::Control::PolicyBatch(batch)) => {
                                    println!("\tPolicyBatch:");
                                    println!(
                                        "\t- expected_current_version={:?}",
                                        batch.expected_current_version
                                    );
                                    println!("\t- new_version={}", batch.new_version);
                                    if let Some(policy) = batch.decider_policy {
                                        println!(
                                            "\t- decider_policy={}",
                                            decider_policy_name(policy)
                                        );
                                    }
                                    let mut voter_ops: Vec<_> = batch.voter_ops.iter().collect();
                                    voter_ops.sort_by_key(|(a, _)| *a);
                                    for (voter_id, op) in voter_ops {
                                        match op.op.as_ref() {
                                            Some(voter_op::Op::Add(add)) => {
                                                let config_description = add
                                                    .config
                                                    .as_ref()
                                                    .map(voter_config_description)
                                                    .unwrap_or_else(|| "none".to_string());
                                                println!(
                                                    "\t- add_voter voter_id={} config={}",
                                                    voter_id, config_description
                                                );
                                            }
                                            Some(voter_op::Op::Remove(_)) => {
                                                println!("\t- remove_voter voter_id={}", voter_id);
                                            }
                                            None => println!(
                                                "\t- voter_op voter_id={} operation=<empty>",
                                                voter_id
                                            ),
                                        }
                                    }
                                }
                                None => println!("\tBaseEngine: <empty>"),
                            }
                        }
                    }
                } else {
                    println!("\t<unknown>");
                }
            }
            Some(payload::Payload::InferenceInput(inference_input)) => {
                println!("{}", "INFERENCE_INPUT:".bright_blue());
                if let Some(ref input) = inference_input.inference_input {
                    match input {
                        inference_input::InferenceInput::StringInferenceInput(s) => {
                            for line in s.lines() {
                                println!("\t{}", line.white().on_black());
                            }
                        }
                    }
                } else {
                    println!("\t<unknown>");
                }
            }
            Some(payload::Payload::InferenceOutput(inference_output)) => {
                println!("{}", "INFERENCE_OUTPUT:".bright_blue());
                if let Some(ref output) = inference_output.inference_output {
                    match output {
                        inference_output::InferenceOutput::StringInferenceOutput(s) => {
                            for line in s.lines() {
                                println!("\t{}", line.white().on_black());
                            }
                        }
                    }
                } else {
                    println!("\t<unknown>");
                }
            }
            Some(payload::Payload::ActionOutput(action_output)) => {
                println!(
                    "{} intention_id={}",
                    "ACTION_OUTPUT:".bright_yellow(),
                    action_output.intention_id
                );
                if let Some(ref output) = action_output.action_output {
                    match output {
                        action_output::ActionOutput::StringActionOutput(s) => {
                            for line in s.lines() {
                                println!("\t{}", line.white().on_black());
                            }
                        }
                    }
                } else {
                    println!("\t<unknown>");
                }
            }
            Some(payload::Payload::AgentInput(agent_input)) => {
                println!("{}", "AGENT_INPUT:".bright_green());
                if let Some(ref input) = agent_input.agent_input {
                    match input {
                        agent_input::AgentInput::StringAgentInput(s) => {
                            for line in s.lines() {
                                println!("\t{}", line.white().on_black());
                            }
                        }
                    }
                } else {
                    println!("\t<unknown>");
                }
            }
            Some(payload::Payload::AgentOutput(agent_output)) => {
                println!("{}", "AGENT_OUTPUT:".bright_green());
                if let Some(ref output) = agent_output.agent_output {
                    match output {
                        agent_output::AgentOutput::StringAgentOutput(s) => {
                            for line in s.lines() {
                                println!("\t{}", line.white().on_black());
                            }
                        }
                    }
                } else {
                    println!("\t<unknown>");
                }
            }
            Some(payload::Payload::Mail(mail)) => {
                println!("{} from={}", "MAIL:".bright_cyan(), mail.sender_agent_id);
                let body = match &mail.content {
                    Some(mail::Content::Message(msg)) => msg.body.as_str(),
                    Some(mail::Content::Reply(reply)) => reply
                        .message
                        .as_ref()
                        .map(|m| m.body.as_str())
                        .unwrap_or(""),
                    None => "",
                };
                println!("\t{}", body.white().on_black());
            }
            None => {
                println!("{}", "UNKNOWN".bright_black());
            }
        }
    } else {
        println!("{}", "UNKNOWN".bright_black());
    }
}

/// Poll entries from the AgentBus.
/// If `end_position` is None, uses check_tail to determine the end.
async fn poll_entries(
    bus: &impl AgentBus,
    agent_bus_id: &str,
    start_position: i64,
    max_entries: i16,
    end_position: Option<i64>,
) -> Result<()> {
    info!(
        start_position = start_position,
        max_entries = max_entries,
        "Polling entries"
    );

    let tail = match end_position {
        Some(end) => end,
        None => {
            bus.check_tail(CheckTailRequest {
                agent_bus_id: agent_bus_id.to_owned(),
                bus_id: Some(BusId {
                    agent_bus_id: agent_bus_id.to_owned(),
                }),
            })
            .await
            .context("Failed to check tail")?
            .tail_position
        }
    };

    let (entries, _) = agentbus_api::read_range(
        bus,
        agent_bus_id,
        start_position,
        tail,
        max_entries as i32,
        None,
    )
    .await
    .context("Failed to read entries")?;

    if entries.is_empty() {
        println!("No entries found");
        println!("  Agent Bus ID: {}", agent_bus_id);
        println!("  Start Position: {}", start_position);
    } else {
        for entry in &entries {
            print_entry(entry);
        }
        println!("\n✓ Retrieved {} entries", entries.len());
        println!("  Agent Bus ID: {}", agent_bus_id);
    }
    println!("  Tail Position: {}", tail);

    Ok(())
}

/// Autocomplete helper for the REPL
#[derive(Clone)]
struct AgentBusCompleter;

const COMMANDS: &[&str] = &[
    "intention",
    "decider-policy",
    "voter-policy",
    "poll",
    "tail",
    "set-id",
    "help",
    "quit",
    "exit",
];
const DECIDER_POLICIES: &[&str] = &["OFF_BY_DEFAULT", "ON_BY_DEFAULT", "FIRST_BOOLEAN_WINS"];

impl rustyline::completion::Completer for AgentBusCompleter {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let line = &line[..pos];
        let parts: Vec<&str> = line.split_whitespace().collect();

        // Completing the first word (command)
        if parts.is_empty() || (parts.len() == 1 && !line.ends_with(' ')) {
            let prefix = parts.first().unwrap_or(&"");
            let matches: Vec<String> = COMMANDS
                .iter()
                .filter(|cmd| cmd.starts_with(prefix))
                .map(|s| s.to_string())
                .collect();
            return Ok((line.len() - prefix.len(), matches));
        }

        // Completing decider-policy argument
        if parts[0] == "decider-policy" && parts.len() <= 2 {
            let prefix = parts.get(1).unwrap_or(&"").to_uppercase();
            let matches: Vec<String> = DECIDER_POLICIES
                .iter()
                .filter(|p| p.starts_with(&prefix))
                .map(|s| s.to_string())
                .collect();
            let last_word_start = line.rfind(' ').map(|i| i + 1).unwrap_or(0);
            return Ok((last_word_start, matches));
        }

        Ok((pos, vec![]))
    }
}

impl rustyline::hint::Hinter for AgentBusCompleter {
    type Hint = String;
}

impl rustyline::highlight::Highlighter for AgentBusCompleter {}

impl rustyline::validate::Validator for AgentBusCompleter {}

impl rustyline::Helper for AgentBusCompleter {}

/// Find the end position of the log
async fn find_end_position(bus: &impl AgentBus, agent_bus_id: &str) -> Result<i64> {
    let tail = bus
        .check_tail(CheckTailRequest {
            agent_bus_id: agent_bus_id.to_owned(),
            bus_id: Some(BusId {
                agent_bus_id: agent_bus_id.to_owned(),
            }),
        })
        .await
        .context("Failed to check tail")?;
    Ok(tail.tail_position)
}

/// Check and display the tail position of the log
async fn check_tail(bus: &impl AgentBus, agent_bus_id: &str) -> Result<()> {
    let tail_position = find_end_position(bus, agent_bus_id).await?;
    println!("Tail Position: {}", tail_position);
    Ok(())
}

/// Find the tail of the log and display the last N entries, returning the end position
async fn find_tail(bus: &impl AgentBus, agent_bus_id: &str, tail_n: i16) -> Result<i64> {
    let tail = find_end_position(bus, agent_bus_id).await?;
    let start = (tail - tail_n as i64).max(0);
    poll_entries(bus, agent_bus_id, start, tail_n, Some(tail)).await?;
    Ok(tail)
}

/// Tail with follow mode - continuously show new entries
async fn tail_follow(bus: &impl AgentBus, agent_bus_id: &str, tail_n: i16) -> Result<()> {
    use futures::FutureExt;
    use tokio::io::AsyncReadExt;
    use tokio::io::stdin;

    // First, show the last N entries and get the end position
    let mut pos = find_tail(bus, agent_bus_id, tail_n).await?;

    println!("\n[Following... Press any key to exit]");

    let mut stdin = stdin();
    let mut buf = [0u8; 1];

    loop {
        let read_fut = stdin.read(&mut buf).fuse();
        futures::pin_mut!(read_fut);

        futures::select_biased! {
            _ = read_fut => {
                println!("\n[Exiting follow mode]");
                break;
            }
            result = bus.blocking_poll(BlockingPollRequest {
                agent_bus_id: agent_bus_id.to_owned(),
                bus_id: Some(BusId {
                    agent_bus_id: agent_bus_id.to_owned(),
                }),
                start_log_position: pos,
                max_entries: 100,
                filter: None,
                timeout_ms: 500,
            }).fuse() => {
                match result {
                    Ok(resp) => {
                        for entry in &resp.entries {
                            print_entry(entry);
                        }
                        // Paranoia: blocking_poll contract already forbids regression.
                        if resp.next_start_position > pos {
                            pos = resp.next_start_position;
                        }
                    }
                    Err(e) => {
                        println!("Error polling: {}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Run interactive REPL mode
async fn run_repl(bus: &impl AgentBus, initial_agent_bus_id: String) -> Result<()> {
    use rustyline::Editor;
    use rustyline::error::ReadlineError;

    let mut agent_bus_id = initial_agent_bus_id;

    println!("AgentBus REPL - Interactive Mode");
    println!("Agent Bus ID: {}", agent_bus_id);
    println!();
    println!("Commands:");
    println!("  intention <text>           - Append a new intention");
    println!(
        "  decider-policy <policy>    - Append a decider policy (OFF_BY_DEFAULT, ON_BY_DEFAULT, FIRST_BOOLEAN_WINS)"
    );
    println!("  voter-policy <text>        - Append a voter policy prompt override");
    println!("  poll <start> <limit>       - Poll entries (both parameters required)");
    println!("  tail [-f] [n]              - Show last n entries (default: 10), -f to follow");
    println!("  set-id <id>                - Change the current agent bus ID");
    println!("  help                       - Show this help");
    println!("  quit or exit               - Exit REPL");
    println!();
    println!("Press Tab for autocomplete");
    println!();

    let config = rustyline::Config::builder()
        .completion_type(rustyline::CompletionType::Circular)
        .build();
    let mut rl = Editor::with_config(config)?;
    rl.set_helper(Some(AgentBusCompleter));

    loop {
        let readline = rl.readline("agentbus> ");

        let line = match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                line
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl+C
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl+D
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        let command = parts[0];

        match command {
            "quit" | "exit" => {
                println!("Goodbye!");
                break;
            }
            "help" => {
                println!("Commands:");
                println!("  intention <text>           - Append a new intention");
                println!(
                    "  decider-policy <policy>    - Append a decider policy (OFF_BY_DEFAULT, ON_BY_DEFAULT, FIRST_BOOLEAN_WINS)"
                );
                println!("  voter-policy <text>        - Append a voter policy prompt override");
                println!("  poll <start> <limit>       - Poll entries (both parameters required)");
                println!(
                    "  tail [-f] [n]              - Show last n entries (default: 10), -f to follow"
                );
                println!("  set-id <id>                - Change the current agent bus ID");
                println!("  help                       - Show this help");
                println!("  quit or exit               - Exit REPL");
            }
            "set-id" => {
                if parts.len() < 2 {
                    println!("Error: set-id requires an ID");
                    println!("Usage: set-id <id>");
                    continue;
                }
                let new_id = parts[1].to_string();
                agent_bus_id = new_id;
                println!("✓ Current agent bus ID set to: {}", agent_bus_id);
            }
            "intention" => {
                if parts.len() < 2 {
                    println!("Error: intention requires a text string");
                    println!("Usage: intention <text>");
                    continue;
                }
                let intention = parts[1].to_string();

                match append_intention(bus, &agent_bus_id, &intention).await {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            "decider-policy" => {
                if parts.len() < 2 {
                    println!("Error: decider-policy requires a policy type");
                    println!("Usage: decider-policy <policy>");
                    println!("Valid policies: OFF_BY_DEFAULT, ON_BY_DEFAULT, FIRST_BOOLEAN_WINS");
                    continue;
                }
                let policy = parts[1].to_string();

                match append_decider_policy(bus, &agent_bus_id, &policy).await {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            "voter-policy" => {
                if parts.len() < 2 {
                    println!("Error: voter-policy requires a prompt override text");
                    println!("Usage: voter-policy <text>");
                    continue;
                }
                let prompt_update = parts[1].to_string();

                match append_voter_policy(bus, &agent_bus_id, &prompt_update).await {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            "poll" => {
                if parts.len() < 2 {
                    println!("Error: poll requires both start and limit parameters");
                    println!("Usage: poll <start> <limit>");
                    continue;
                }

                let poll_args: Vec<&str> = parts[1].split_whitespace().collect();

                if poll_args.len() < 2 {
                    println!("Error: poll requires both start and limit parameters");
                    println!("Usage: poll <start> <limit>");
                    continue;
                }

                let start = match poll_args[0].parse::<i64>() {
                    Ok(s) => s,
                    Err(_) => {
                        println!("Error: invalid start position '{}'", poll_args[0]);
                        continue;
                    }
                };

                let limit = match poll_args[1].parse::<i16>() {
                    Ok(l) => l,
                    Err(_) => {
                        println!("Error: invalid limit '{}'", poll_args[1]);
                        continue;
                    }
                };

                match poll_entries(bus, &agent_bus_id, start, limit, None).await {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            "tail" => {
                let args = if parts.len() >= 2 {
                    parts[1].split_whitespace().collect::<Vec<_>>()
                } else {
                    vec![]
                };

                let mut tail_n = 10i16;
                let mut follow = false;

                for arg in args {
                    if arg == "-f" {
                        follow = true;
                    } else if let Ok(n) = arg.parse::<i16>() {
                        tail_n = n;
                    } else {
                        println!("Error: invalid argument '{}'", arg);
                        println!("Usage: tail [-f] [n]");
                        continue;
                    }
                }

                if follow {
                    match tail_follow(bus, &agent_bus_id, tail_n).await {
                        Ok(()) => {}
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                } else {
                    match find_tail(bus, &agent_bus_id, tail_n).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                }
            }
            _ => {
                println!("Unknown command: {}", command);
                println!("Type 'help' for available commands");
            }
        }
    }

    Ok(())
}

/// Initialize logging with glog format
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
