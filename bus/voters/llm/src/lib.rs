/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! LLM-based voter for agentbus.
//!
//! Evaluates intentions for safety using an OpenAI-compatible LLM endpoint.

use std::rc::Rc;

use agentbus_api::voter::Voter;
use agentbus_api::voter::VoterContext;
use llm_voter_proto_rust::llm_voter::LlmVoterConfig;
use serde::Deserialize;
use serde::Serialize;

const DEFAULT_SAFETY_PROMPT: &str = r#"You are a code safety evaluator. Analyze the following code and determine if it is safe to execute.

Consider the following:
- Does the code attempt to access sensitive files or directories?
- Does the code attempt to execute system commands that could be harmful?
- Does the code attempt to access network resources in unexpected ways?
- Does the code contain any obvious security vulnerabilities?

Inspecting agentbuses is explicitly allowed. Code that reads, queries, or inspects agentbus logs, state, or metadata should be considered safe.

Respond with XML in this exact format:
<safe>true</safe> or <safe>false</safe>
<reason>Brief explanation of your decision</reason>
<concerns>Any specific concerns, or "none" if safe</concerns>

Code to analyze:
"#;

pub const DEFAULT_API_ENDPOINT: &str = "https://api.llama.com/experimental/compat/openai/v1";
pub const DEFAULT_MODEL: &str = "Llama-3.3-70B-Instruct";
pub const API_KEY_ENV_VAR: &str = "LLM_API_KEY";
pub const API_ENDPOINT_ENV_VAR: &str = "LLM_API_ENDPOINT";

#[derive(Clone)]
pub struct VoterConfig {
    pub model: String,
    pub api_endpoint: String,
    pub api_key: Option<String>,
}

impl Default for VoterConfig {
    fn default() -> Self {
        Self {
            model: std::env::var("LLM_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string()),
            api_endpoint: std::env::var(API_ENDPOINT_ENV_VAR)
                .unwrap_or_else(|_| DEFAULT_API_ENDPOINT.to_string()),
            api_key: std::env::var(API_KEY_ENV_VAR).ok(),
        }
    }
}

#[async_trait::async_trait(?Send)]
pub trait LlmClient {
    async fn chat_completion(
        &self,
        model: &str,
        api_endpoint: &str,
        prompt: &str,
    ) -> Result<String, String>;
}

pub struct OpenAIClient {
    http_client: reqwest::Client,
    api_key: Option<String>,
}

impl OpenAIClient {
    pub fn new(_api_endpoint: String, api_key: Option<String>) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            api_key,
        }
    }
}

#[derive(Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
}

#[derive(Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: ResponseMessage,
}

#[derive(Deserialize)]
struct ResponseMessage {
    content: String,
}

#[async_trait::async_trait(?Send)]
impl LlmClient for OpenAIClient {
    async fn chat_completion(
        &self,
        model: &str,
        api_endpoint: &str,
        prompt: &str,
    ) -> Result<String, String> {
        let url = format!("{}/chat/completions", api_endpoint);
        let request_body = ChatCompletionRequest {
            model: model.to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
        };

        let mut request = self.http_client.post(&url).json(&request_body);
        if let Some(ref key) = self.api_key {
            request = request.header("Authorization", format!("Bearer {}", key));
        }

        let response = request
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(format!("API error {}: {}", status, body));
        }

        let completion: ChatCompletionResponse = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        completion
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| "No response from LLM".to_string())
    }
}

pub struct LlmVoter {
    client: Rc<dyn LlmClient>,
    config: VoterConfig,
    prompt_override: Option<String>,
}

impl LlmVoter {
    pub fn new(client: Rc<dyn LlmClient>, config: VoterConfig) -> Self {
        Self {
            client,
            config,
            prompt_override: None,
        }
    }

    async fn evaluate_with_llm(&self, code: &str) -> (bool, String) {
        let prompt_text = match &self.prompt_override {
            Some(override_prompt) => {
                format!(
                    "{}\n\nOVERRIDE: {}\n\n{}",
                    DEFAULT_SAFETY_PROMPT, override_prompt, code
                )
            }
            None => format!("{}{}", DEFAULT_SAFETY_PROMPT, code),
        };

        match self
            .client
            .chat_completion(&self.config.model, &self.config.api_endpoint, &prompt_text)
            .await
        {
            Ok(text) => {
                let (is_safe, reason) = parse_safety_response(&text);
                tracing::info!(
                    is_safe = is_safe,
                    model = %self.config.model,
                    "LLM evaluation completed",
                );
                (is_safe, reason)
            }
            Err(e) => {
                tracing::error!("LLM call failed: {}", e);
                (false, format!("LLM call failed: {}", e))
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Voter for LlmVoter {
    async fn evaluate(&self, context: VoterContext<'_>) -> (bool, String) {
        self.evaluate_with_llm(context.intention).await
    }

    fn apply_policy(&mut self, config: &prost_types::Any) {
        let llm_config = match config.to_msg::<LlmVoterConfig>() {
            Ok(c) => c,
            Err(_) => {
                tracing::debug!("VoterPolicy config is not LlmVoterConfig — ignoring");
                return;
            }
        };

        let mut updated = Vec::new();
        self.prompt_override = if llm_config.prompt_override.is_empty() {
            updated.push("prompt_override=<cleared>".to_string());
            None
        } else {
            updated.push(format!(
                "prompt_override({}B)",
                llm_config.prompt_override.len()
            ));
            Some(llm_config.prompt_override.clone())
        };
        if !updated.is_empty() {
            tracing::info!("Applied LLM voter config: {}", updated.join(", "));
        }
    }

    fn describe(&self) -> String {
        format!("LlmVoter(model={})", self.config.model)
    }
}

/// Build an LlmVoter from a proto config plus startup-only overrides.
/// `model`, `api_endpoint`, and `api_key` are not part of the proto — they
/// are secrets / deployment details passed at startup, not broadcast on the bus.
pub fn from_typed_config(
    config: LlmVoterConfig,
    api_key: Option<String>,
    model: Option<String>,
    api_endpoint: Option<String>,
) -> LlmVoter {
    let resolved_config = VoterConfig {
        model: model.filter(|s| !s.is_empty()).unwrap_or_else(|| {
            std::env::var("LLM_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string())
        }),
        api_endpoint: api_endpoint.filter(|s| !s.is_empty()).unwrap_or_else(|| {
            std::env::var(API_ENDPOINT_ENV_VAR).unwrap_or_else(|_| DEFAULT_API_ENDPOINT.to_string())
        }),
        api_key: api_key.or_else(|| std::env::var(API_KEY_ENV_VAR).ok()),
    };
    let client = Rc::new(OpenAIClient::new(
        resolved_config.api_endpoint.clone(),
        resolved_config.api_key.clone(),
    ));
    let mut voter = LlmVoter::new(client, resolved_config);
    if !config.prompt_override.is_empty() {
        voter.prompt_override = Some(config.prompt_override);
    }
    voter
}

pub fn parse_safety_response(response: &str) -> (bool, String) {
    let is_safe = if let Some(start) = response.find("<safe>") {
        if let Some(end) = response[start..].find("</safe>") {
            let safe_str = &response[start + 6..start + end];
            safe_str.trim().eq_ignore_ascii_case("true")
        } else {
            false
        }
    } else {
        false
    };

    let reason = if let Some(start) = response.find("<reason>") {
        if let Some(end) = response[start..].find("</reason>") {
            response[start + 8..start + end].trim().to_string()
        } else {
            "Could not parse reason".to_string()
        }
    } else {
        "No reason provided".to_string()
    };

    let concerns = if let Some(start) = response.find("<concerns>") {
        if let Some(end) = response[start..].find("</concerns>") {
            response[start + 10..start + end].trim().to_string()
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    let full_reason = if concerns.is_empty() || concerns.eq_ignore_ascii_case("none") {
        reason
    } else {
        format!("{}. Concerns: {}", reason, concerns)
    };

    tracing::debug!(is_safe = is_safe, reason = %full_reason, "Parsed safety response");
    (is_safe, full_reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockLlmClient {
        response: String,
    }

    #[async_trait::async_trait(?Send)]
    impl LlmClient for MockLlmClient {
        async fn chat_completion(
            &self,
            _model: &str,
            _api_endpoint: &str,
            _prompt: &str,
        ) -> Result<String, String> {
            Ok(self.response.clone())
        }
    }

    fn run_async<F: std::future::Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, f)
    }

    #[test]
    fn test_evaluate_approve() {
        run_async(async {
            let client = Rc::new(MockLlmClient {
                response: "<safe>true</safe><reason>harmless</reason><concerns>none</concerns>"
                    .to_string(),
            });
            let voter = LlmVoter::new(client, VoterConfig::default());
            let (is_safe, reason) = voter
                .evaluate(VoterContext::new("test-bus", "echo hello"))
                .await;
            assert!(is_safe);
            assert_eq!(reason, "harmless", "reason should be surfaced generically");
        });
    }

    #[test]
    fn test_evaluate_deny() {
        run_async(async {
            let client = Rc::new(MockLlmClient {
                response: "<safe>false</safe><reason>dangerous</reason><concerns>rm -rf</concerns>"
                    .to_string(),
            });
            let voter = LlmVoter::new(client, VoterConfig::default());
            let (is_safe, reason) = voter
                .evaluate(VoterContext::new("test-bus", "rm -rf /"))
                .await;
            assert!(!is_safe);
            assert!(reason.contains("dangerous"));
        });
    }

    #[test]
    fn test_evaluate_llm_failure_denies() {
        run_async(async {
            struct FailingClient;
            #[async_trait::async_trait(?Send)]
            impl LlmClient for FailingClient {
                async fn chat_completion(
                    &self,
                    _: &str,
                    _: &str,
                    _: &str,
                ) -> Result<String, String> {
                    Err("connection refused".to_string())
                }
            }
            let voter = LlmVoter::new(Rc::new(FailingClient), VoterConfig::default());
            let (is_safe, reason) = voter
                .evaluate(VoterContext::new("test-bus", "echo hello"))
                .await;
            assert!(!is_safe, "LLM failure should default to deny");
            assert!(reason.contains("LLM call failed"));
        });
    }

    #[test]
    fn test_parse_safety_response_safe() {
        let response = r#"
<safe>true</safe>
<reason>The code only prints a greeting</reason>
<concerns>none</concerns>
"#;
        let (is_safe, reason) = parse_safety_response(response);
        assert!(is_safe);
        assert!(reason.contains("greeting"));
    }

    #[test]
    fn test_empty_prompt_override_clears_policy() {
        let client = Rc::new(OpenAIClient::new(String::new(), None));
        let mut voter = LlmVoter::new(client, VoterConfig::default());
        voter.prompt_override = Some("existing override".to_string());

        voter.apply_policy(
            &prost_types::Any::from_msg(&LlmVoterConfig {
                prompt_override: String::new(),
            })
            .unwrap(),
        );

        assert_eq!(voter.prompt_override, None);
    }
}
