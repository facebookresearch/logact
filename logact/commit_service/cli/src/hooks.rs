/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Harness-neutral parsing and AgentBus dispatch for native hook protocols.

use std::io::Read;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus as agentbus;
use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::Payload;
use agentbus_api::AgentBus;
use anyhow::Context;
use anyhow::Result;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitSvc;
use serde::Deserialize;

#[derive(Clone, Copy, Debug)]
pub(crate) enum EventKind {
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    Stop,
    StopFailure,
}

impl EventKind {
    /// Bus event type for this hook, matching the `event` column the per-call
    /// success samples use. Logged on a fail-open error from a harness runner.
    pub(crate) fn event_label(&self) -> &'static str {
        match self {
            EventKind::UserPromptSubmit => "agent_input",
            EventKind::PreToolUse => "intention",
            EventKind::PostToolUse | EventKind::PostToolUseFailure => "action_output",
            EventKind::Stop | EventKind::StopFailure => "agent_output",
        }
    }
}

/// Failure while reading and parsing a hook invocation.
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    #[error("timed out after {after:?} reading the hook payload from stdin")]
    InputTimeout { after: Duration },

    #[error("{0:#}")]
    Input(anyhow::Error),

    #[error("USER env var not set")]
    UserUnset,
}

/// Failure while processing a hook invocation.
#[derive(Debug, thiserror::Error)]
pub enum HookRunError {
    #[error(transparent)]
    Prepare(#[from] PrepareError),

    #[error("hook dispatch failed: {0:#}")]
    Dispatch(anyhow::Error),

    #[error("failed to write hook response: {0:#}")]
    Output(anyhow::Error),
}

/// Subset of the hook stdin we care about. The rest stays in the raw body.
#[derive(Deserialize)]
struct HookInput {
    session_id: String,
    // Claude and Codex both send agent_id for hooks running in subagents.
    // Codex behavior was observed in live payloads in July 2026, but is not
    // currently documented.
    #[serde(rename = "agent_id")]
    subagent_id: Option<String>,
}

/// Parsed common input, before the harness adds its AgentBus identity prefix.
#[derive(Debug)]
pub(crate) struct PreparedHook {
    user_name: String,
    session_id: String,
    subagent_id: Option<String>,
    body: String,
}

impl PreparedHook {
    pub(crate) fn into_dispatch(self, agent_id_prefix: &str) -> DispatchInput {
        let base = format!("{agent_id_prefix}.{}.{}", self.user_name, self.session_id);
        let agent_id = match self.subagent_id.as_deref() {
            Some(subagent_id) if !subagent_id.is_empty() => {
                format!("{base}.agent.{subagent_id}")
            }
            _ => base,
        };
        DispatchInput {
            agent_id,
            body: self.body,
        }
    }
}

/// Normalize an optional harness name for AgentBus identity construction.
pub fn agent_id_prefix(agent_platform: Option<&str>) -> &str {
    agent_platform
        .filter(|platform| !platform.is_empty())
        .unwrap_or("unknown")
}

/// Harness-neutral input to the AgentBus RPC dispatch.
pub(crate) struct DispatchInput {
    pub(crate) agent_id: String,
    pub(crate) body: String,
}

/// Harness-neutral result. Callers decide whether and how to write it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DispatchOutcome {
    Recorded,
    Intention { approved: bool, reason: String },
}

/// Hard cap on the stdin read; past this we fail open rather than hang the turn.
const STDIN_READ_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn prepare() -> Result<PreparedHook, PrepareError> {
    let body = read_hook_payload().await?;
    let user_name = std::env::var("USER").map_err(|_| PrepareError::UserUnset)?;
    prepare_body(&user_name, body)
}

fn prepare_body(user_name: &str, body: String) -> Result<PreparedHook, PrepareError> {
    let input: HookInput = serde_json::from_str(&body).map_err(|error| {
        PrepareError::Input(anyhow::Error::new(error).context("invalid stdin JSON"))
    })?;
    Ok(PreparedHook {
        user_name: user_name.to_owned(),
        session_id: input.session_id,
        subagent_id: input.subagent_id,
        body,
    })
}

/// Record the event and return only the harness-neutral RPC outcome.
pub(crate) async fn dispatch<C: CommitSvc + ?Sized>(
    commit_client: &C,
    event: EventKind,
    input: DispatchInput,
) -> Result<DispatchOutcome> {
    let DispatchInput { agent_id, body } = input;
    match event {
        EventKind::UserPromptSubmit => {
            record(
                commit_client.agent_bus(),
                agent_id,
                agent_input_payload(body),
            )
            .await?;
            Ok(DispatchOutcome::Recorded)
        }
        EventKind::PreToolUse => pre_tool_use(commit_client, agent_id, body).await,
        EventKind::PostToolUse | EventKind::PostToolUseFailure => {
            record(
                commit_client.agent_bus(),
                agent_id,
                action_output_payload(body),
            )
            .await?;
            Ok(DispatchOutcome::Recorded)
        }
        EventKind::Stop | EventKind::StopFailure => {
            record(
                commit_client.agent_bus(),
                agent_id,
                agent_output_payload(body),
            )
            .await?;
            Ok(DispatchOutcome::Recorded)
        }
    }
}

/// Read the hook payload from stdin under a timeout. The blocking read runs on a
/// separate thread so the timer can fire; on timeout it is abandoned and dies
/// with the process.
async fn read_hook_payload() -> Result<String, PrepareError> {
    let read = tokio::task::spawn_blocking(|| {
        let mut body = String::new();
        std::io::stdin().read_to_string(&mut body)?;
        anyhow::Ok(body)
    });
    read_within(STDIN_READ_TIMEOUT, async move {
        read.await.context("stdin reader thread panicked")?
    })
    .await
}

/// Resolve `read` within `timeout`. Split from `read_hook_payload` so the timeout
/// path is unit-testable without a 5s wait or real stdin. A timeout and a read
/// failure map to distinct client codes.
async fn read_within<F>(timeout: Duration, read: F) -> Result<String, PrepareError>
where
    F: std::future::Future<Output = Result<String>>,
{
    match tokio::time::timeout(timeout, read).await {
        Ok(Ok(body)) => Ok(body),
        Ok(Err(error)) => Err(PrepareError::Input(error)),
        Err(_elapsed) => Err(PrepareError::InputTimeout { after: timeout }),
    }
}

pub(crate) fn agent_input_payload(body: String) -> Payload {
    Payload {
        payload: Some(agentbus::payload::Payload::AgentInput(
            agentbus::AgentInput {
                agent_input: Some(agentbus::agent_input::AgentInput::StringAgentInput(body)),
            },
        )),
    }
}

pub(crate) fn action_output_payload(body: String) -> Payload {
    Payload {
        payload: Some(agentbus::payload::Payload::ActionOutput(
            agentbus::ActionOutput {
                intention_id: -1,
                action_output: Some(agentbus::action_output::ActionOutput::StringActionOutput(
                    body,
                )),
            },
        )),
    }
}

pub(crate) fn agent_output_payload(body: String) -> Payload {
    Payload {
        payload: Some(agentbus::payload::Payload::AgentOutput(
            agentbus::AgentOutput {
                agent_output: Some(agentbus::agent_output::AgentOutput::StringAgentOutput(body)),
            },
        )),
    }
}

async fn pre_tool_use<C: CommitSvc + ?Sized>(
    commit_client: &C,
    agent_id: String,
    body: String,
) -> Result<DispatchOutcome> {
    let outcome = commit_client
        .commit_intention(CommitIntentionCommand {
            bus_id: BusId {
                agent_bus_id: agent_id,
            },
            intention: agentbus::intention::Intention::StringIntention(body),
        })
        .await?;
    Ok(DispatchOutcome::Intention {
        approved: outcome.approved,
        reason: outcome.reason,
    })
}

async fn record<B: AgentBus + ?Sized>(bus: &B, agent_id: String, payload: Payload) -> Result<()> {
    bus.append(AppendRequest {
        agent_bus_id: agent_id.clone(),
        bus_id: Some(BusId {
            agent_bus_id: agent_id,
        }),
        payload: Some(payload),
    })
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agentbus_api::BusResult;
    use logact_commit_service_api::CommitError;
    use logact_commit_service_api::CommitIntentionOutcome;
    use logact_commit_service_api::CommitResult;

    use super::*;

    const PAYLOAD: &str =
        r#"{"session_id":"session-123","tool_name":"functions.exec","tool_input":{"cmd":"pwd"}}"#;

    #[derive(Clone, Copy)]
    enum FakeIntentionResult {
        Approved,
        Rejected,
        RpcError,
    }

    struct FakeCommitClient {
        intention_result: FakeIntentionResult,
        event_requests: Mutex<Vec<AppendRequest>>,
        intention_requests: Mutex<Vec<CommitIntentionCommand>>,
    }

    impl FakeCommitClient {
        fn new(intention_result: FakeIntentionResult) -> Self {
            Self {
                intention_result,
                event_requests: Mutex::new(Vec::new()),
                intention_requests: Mutex::new(Vec::new()),
            }
        }

        fn intention_request(&self) -> CommitIntentionCommand {
            let requests = self.intention_requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "expected exactly one intention RPC");
            requests[0].clone()
        }

        fn event_requests(&self) -> Vec<AppendRequest> {
            self.event_requests.lock().unwrap().clone()
        }
    }

    impl CommitSvc for FakeCommitClient {
        type Bus = Self;

        fn agent_bus(&self) -> &Self::Bus {
            self
        }

        async fn commit_intention(
            &self,
            command: CommitIntentionCommand,
        ) -> CommitResult<CommitIntentionOutcome> {
            self.intention_requests.lock().unwrap().push(command);
            match self.intention_result {
                FakeIntentionResult::Approved => Ok(CommitIntentionOutcome {
                    approved: true,
                    reason: String::new(),
                    log_position: 1,
                }),
                FakeIntentionResult::Rejected => Ok(CommitIntentionOutcome {
                    approved: false,
                    reason: "blocked by policy".to_owned(),
                    log_position: 1,
                }),
                FakeIntentionResult::RpcError => {
                    Err(CommitError::Internal(anyhow::anyhow!("commit failed")))
                }
            }
        }
    }

    impl AgentBus for FakeCommitClient {
        async fn append(&self, request: AppendRequest) -> BusResult<agentbus::AppendResponse> {
            self.event_requests.lock().unwrap().push(request);
            Ok(agentbus::AppendResponse { log_position: 1 })
        }

        async fn poll(&self, _request: agentbus::PollRequest) -> BusResult<agentbus::PollResponse> {
            panic!("poll is not used by commit CLI tests")
        }

        async fn read_next(
            &self,
            _request: agentbus::ReadNextRequest,
        ) -> BusResult<agentbus::ReadNextResponse> {
            panic!("read_next is not used by commit CLI tests")
        }

        async fn check_tail(
            &self,
            _request: agentbus::CheckTailRequest,
        ) -> BusResult<agentbus::CheckTailResponse> {
            panic!("check_tail is not used by commit CLI tests")
        }

        async fn blocking_poll(
            &self,
            _request: agentbus::BlockingPollRequest,
        ) -> BusResult<agentbus::BlockingPollResponse> {
            panic!("blocking_poll is not used by commit CLI tests")
        }
    }

    fn prepared(prefix: &str) -> DispatchInput {
        prepare_body("alice", PAYLOAD.to_owned())
            .unwrap()
            .into_dispatch(prefix)
    }

    fn dispatch_sync(
        client: &FakeCommitClient,
        event: EventKind,
        input: DispatchInput,
    ) -> Result<DispatchOutcome> {
        test_runtime().block_on(dispatch(client, event, input))
    }

    #[test]
    fn main_thread_identity_uses_harness_user_and_session() {
        let input = prepared("codex");

        assert_eq!(input.agent_id, "codex.alice.session-123");
        assert_eq!(input.body, PAYLOAD);
    }

    #[test]
    fn hook_identity_uses_agent_platform_as_prefix() {
        for agent_platform in ["claude_code", "codex", "metacode"] {
            let input = prepared(agent_id_prefix(Some(agent_platform)));

            assert_eq!(
                input.agent_id,
                format!("{agent_platform}.alice.session-123")
            );
        }
    }

    #[test]
    fn hook_identity_uses_unknown_when_agent_platform_is_missing() {
        let input = prepared(agent_id_prefix(None));

        assert_eq!(input.agent_id, "unknown.alice.session-123");
    }

    #[test]
    fn subagent_identity_includes_agent_id() {
        let body = r#"{"session_id":"parent-session","agent_id":"child-agent","hook_event_name":"PreToolUse"}"#;
        let input = prepare_body("alice", body.to_owned())
            .unwrap()
            .into_dispatch("codex");

        assert_eq!(
            input.agent_id,
            "codex.alice.parent-session.agent.child-agent"
        );
        assert_eq!(input.body, body);
    }

    #[test]
    fn empty_subagent_id_uses_main_thread_identity() {
        let body = r#"{"session_id":"session-123","agent_id":""}"#;
        let input = prepare_body("alice", body.to_owned())
            .unwrap()
            .into_dispatch("claude");

        assert_eq!(input.agent_id, "claude.alice.session-123");
    }

    #[test]
    fn invalid_json_is_an_input_error() {
        let error = prepare_body("alice", "not JSON".to_owned()).unwrap_err();

        assert!(matches!(error, PrepareError::Input(_)));
        assert!(format!("{error:#}").contains("invalid stdin JSON"));
    }

    #[test]
    fn approved_intention_returns_raw_outcome_and_preserves_request() {
        let client = FakeCommitClient::new(FakeIntentionResult::Approved);
        let outcome = dispatch_sync(&client, EventKind::PreToolUse, prepared("codex")).unwrap();

        assert_eq!(
            outcome,
            DispatchOutcome::Intention {
                approved: true,
                reason: String::new(),
            }
        );
        let request = client.intention_request();
        assert_eq!(request.bus_id.agent_bus_id, "codex.alice.session-123");
        assert_eq!(
            request.intention,
            agentbus::intention::Intention::StringIntention(PAYLOAD.to_owned())
        );
    }

    #[test]
    fn rejected_intention_returns_reason_without_harness_translation() {
        let client = FakeCommitClient::new(FakeIntentionResult::Rejected);
        let outcome = dispatch_sync(&client, EventKind::PreToolUse, prepared("claude")).unwrap();

        assert_eq!(
            outcome,
            DispatchOutcome::Intention {
                approved: false,
                reason: "blocked by policy".to_owned(),
            }
        );
        assert_eq!(
            client.intention_request().bus_id.agent_bus_id,
            "claude.alice.session-123"
        );
    }

    #[test]
    fn intention_rpc_failure_is_returned_after_one_request() {
        let client = FakeCommitClient::new(FakeIntentionResult::RpcError);
        let result = dispatch_sync(&client, EventKind::PreToolUse, prepared("codex"));

        let error = result.expect_err("RPC failure must be returned");
        assert!(format!("{error:#}").contains("commit failed"));
        assert_eq!(
            client.intention_request().intention,
            agentbus::intention::Intention::StringIntention(PAYLOAD.to_owned())
        );
    }

    #[test]
    fn ungated_events_map_to_expected_bus_payloads() {
        let client = FakeCommitClient::new(FakeIntentionResult::Approved);
        for event in [
            EventKind::UserPromptSubmit,
            EventKind::PostToolUse,
            EventKind::PostToolUseFailure,
            EventKind::Stop,
            EventKind::StopFailure,
        ] {
            assert_eq!(
                dispatch_sync(&client, event, prepared("claude")).unwrap(),
                DispatchOutcome::Recorded
            );
        }

        let requests = client.event_requests();
        assert_eq!(requests.len(), 5);
        assert!(matches!(
            &requests[0].payload,
            Some(Payload {
                payload: Some(agentbus::payload::Payload::AgentInput(input)),
            }) if matches!(
                &input.agent_input,
                Some(agentbus::agent_input::AgentInput::StringAgentInput(body)) if body == PAYLOAD
            )
        ));
        assert!(matches!(
            &requests[1].payload,
            Some(Payload {
                payload: Some(agentbus::payload::Payload::ActionOutput(output)),
            }) if matches!(
                &output.action_output,
                Some(agentbus::action_output::ActionOutput::StringActionOutput(body)) if body == PAYLOAD
            )
        ));
        assert!(matches!(
            &requests[2].payload,
            Some(Payload {
                payload: Some(agentbus::payload::Payload::ActionOutput(output)),
            }) if matches!(
                &output.action_output,
                Some(agentbus::action_output::ActionOutput::StringActionOutput(body)) if body == PAYLOAD
            )
        ));
        assert!(matches!(
            &requests[3].payload,
            Some(Payload {
                payload: Some(agentbus::payload::Payload::AgentOutput(output)),
            }) if matches!(
                &output.agent_output,
                Some(agentbus::agent_output::AgentOutput::StringAgentOutput(body)) if body == PAYLOAD
            )
        ));
        assert!(matches!(
            &requests[4].payload,
            Some(Payload {
                payload: Some(agentbus::payload::Payload::AgentOutput(output)),
            }) if matches!(
                &output.agent_output,
                Some(agentbus::agent_output::AgentOutput::StringAgentOutput(body)) if body == PAYLOAD
            )
        ));
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime")
    }

    #[test]
    fn read_within_fails_open_when_read_stalls() {
        let result = test_runtime().block_on(read_within(
            Duration::from_millis(10),
            std::future::pending::<Result<String>>(),
        ));
        let error = result.expect_err("a stalled read must time out");
        assert!(matches!(error, PrepareError::InputTimeout { .. }));
        assert!(error.to_string().contains("timed out"), "got: {error}");
    }

    #[test]
    fn read_within_maps_read_failure_to_input_error() {
        let result = test_runtime().block_on(read_within(Duration::from_secs(5), async {
            Err(anyhow::anyhow!("broken pipe"))
        }));
        let error = result.expect_err("a read failure must surface");
        assert!(matches!(error, PrepareError::Input(_)));
    }

    #[test]
    fn read_within_returns_payload_when_ready() {
        let result = test_runtime().block_on(read_within(Duration::from_secs(5), async {
            Ok("payload".to_owned())
        }));
        assert_eq!(result.unwrap(), "payload");
    }
}
