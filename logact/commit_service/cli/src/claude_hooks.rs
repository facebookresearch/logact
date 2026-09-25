/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io::Write;

use anyhow::Result;
use clap::Args;
use clap::Subcommand;
use logact_commit_service_api::CommitSvc;
use serde::Serialize;

use crate::hooks;
use crate::hooks::HookRunError;

/// Arguments for a Claude Code hook invocation.
#[derive(Args, Debug)]
pub struct HookArgs {
    #[command(subcommand)]
    event: HookEvent,
}

#[derive(Subcommand, Debug)]
enum HookEvent {
    /// User submitted a prompt: record as an AgentInput.
    UserPromptSubmit,
    /// Claude is about to execute a tool: gate as an Intention.
    PreToolUse,
    /// A tool just finished successfully: record as an ActionOutput.
    PostToolUse,
    /// A tool just failed: record as an ActionOutput. Cannot block.
    PostToolUseFailure,
    /// Claude finished a turn: record as an AgentOutput.
    Stop,
    /// Turn ended due to an API error: record as an AgentOutput.
    StopFailure,
}

impl HookArgs {
    /// Return whether this invocation can gate tool execution.
    pub fn is_pre_tool_use(&self) -> bool {
        matches!(self.event, HookEvent::PreToolUse)
    }

    /// Stable observability label for this event.
    pub fn event_label(&self) -> &'static str {
        self.event_kind().event_label()
    }

    fn event_kind(&self) -> hooks::EventKind {
        match &self.event {
            HookEvent::UserPromptSubmit => hooks::EventKind::UserPromptSubmit,
            HookEvent::PreToolUse => hooks::EventKind::PreToolUse,
            HookEvent::PostToolUse => hooks::EventKind::PostToolUse,
            HookEvent::PostToolUseFailure => hooks::EventKind::PostToolUseFailure,
            HookEvent::Stop => hooks::EventKind::Stop,
            HookEvent::StopFailure => hooks::EventKind::StopFailure,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HookOutput {
    suppress_output: bool,
    hook_specific_output: HookSpecificOutput,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HookSpecificOutput {
    hook_event_name: &'static str,
    permission_decision: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    permission_decision_reason: Option<String>,
}

/// Run one Claude Code hook invocation.
pub async fn run<C: CommitSvc + ?Sized>(
    commit_client: &C,
    args: HookArgs,
    agent_id_prefix: &str,
    enforced: bool,
) -> Result<(), HookRunError> {
    let hook_event = args.event_kind();
    let prepared = hooks::prepare().await?;
    let outcome = hooks::dispatch(
        commit_client,
        hook_event,
        prepared.into_dispatch(agent_id_prefix),
    )
    .await
    .map_err(HookRunError::Dispatch)?;
    if let Some(output) = hook_output(outcome, enforced) {
        emit_output(&output).map_err(HookRunError::Output)?;
    }
    Ok(())
}

/// Translate a rejected AgentBus decision into Claude's PreToolUse response.
/// Approval is empty stdout so Claude's native permission flow remains intact.
fn hook_output(outcome: hooks::DispatchOutcome, enforced: bool) -> Option<HookOutput> {
    if !enforced {
        return None;
    }
    let hooks::DispatchOutcome::Intention { approved, reason } = outcome else {
        return None;
    };
    if approved {
        return None;
    }
    Some(HookOutput {
        suppress_output: true,
        hook_specific_output: HookSpecificOutput {
            hook_event_name: "PreToolUse",
            permission_decision: "ask",
            permission_decision_reason: nonblank(reason),
        },
    })
}

fn nonblank(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn emit_output(output: &HookOutput) -> Result<()> {
    emit_output_to(std::io::stdout(), output)
}

fn emit_output_to(mut writer: impl Write, output: &HookOutput) -> Result<()> {
    writeln!(writer, "{}", serde_json::to_string(output)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(outcome: hooks::DispatchOutcome, enforced: bool) -> Option<String> {
        hook_output(outcome, enforced).map(|output| serde_json::to_string(&output).unwrap())
    }

    #[test]
    fn shadow_mode_emits_nothing() {
        assert!(
            render(
                hooks::DispatchOutcome::Intention {
                    approved: false,
                    reason: "blocked".to_owned(),
                },
                false,
            )
            .is_none()
        );
    }

    #[test]
    fn recorded_event_emits_nothing() {
        assert!(render(hooks::DispatchOutcome::Recorded, true).is_none());
    }

    #[test]
    fn approval_emits_nothing() {
        assert!(
            render(
                hooks::DispatchOutcome::Intention {
                    approved: true,
                    reason: String::new(),
                },
                true,
            )
            .is_none()
        );
    }

    #[test]
    fn rejection_emits_claude_ask_and_trimmed_reason() {
        assert_eq!(
            render(
                hooks::DispatchOutcome::Intention {
                    approved: false,
                    reason: "  blocked by policy \t".to_owned(),
                },
                true,
            )
            .unwrap(),
            r#"{"suppressOutput":true,"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"ask","permissionDecisionReason":"blocked by policy"}}"#
        );
    }

    #[test]
    fn output_writer_appends_newline() {
        let output = hook_output(
            hooks::DispatchOutcome::Intention {
                approved: false,
                reason: String::new(),
            },
            true,
        )
        .unwrap();
        let mut stdout = Vec::new();
        emit_output_to(&mut stdout, &output).unwrap();

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "{\"suppressOutput\":true,\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"ask\"}}\n"
        );
    }
}
