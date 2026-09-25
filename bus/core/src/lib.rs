/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Module declarations
pub mod appserver;
pub mod channeled_agentbus;
pub mod client;
pub mod config;
pub mod decider;
pub mod mailbox;
pub mod server_lib;
pub mod tracing_events;
pub mod vote_trackers;
pub mod voter;

pub use agent_bus_proto_rust::agent_bus::agent_bus_service_server::AgentBusService as AgentBusServiceTrait;
pub use agent_bus_proto_rust::agent_bus::agent_bus_service_server::AgentBusServiceServer;
pub use agent_bus_proto_rust::agent_bus::*;
pub use agentbus_api::AgentBus;
pub use agentbus_api::AgentBusError;
pub use agentbus_api::AgentBusMetrics;
pub use agentbus_api::AgentbusLogger;
pub use agentbus_api::BusResult;
pub use agentbus_api::Environment;
pub use agentbus_api::NoopLogger;
pub use agentbus_api::NoopMetrics;
pub use agentbus_api::RealEnvironment;
pub use agentbus_observable::ObservableAgentBus;
use async_trait::async_trait;
pub use channeled_agentbus::ChanneledAgentBus;
use tonic::Code;
use tonic::Request;
use tonic::Response;
use tonic::Status;

fn status_code_from_agent_bus_error(error: &AgentBusError) -> Code {
    match error {
        AgentBusError::InvalidArgument(_) => Code::InvalidArgument,
        AgentBusError::Timeout(_) => Code::DeadlineExceeded,
        AgentBusError::Unavailable(_) => Code::Unavailable,
        AgentBusError::Internal(_) => Code::Internal,
    }
}

fn status_from_agent_bus_error(operation: &str, error: AgentBusError) -> Status {
    Status::new(
        status_code_from_agent_bus_error(&error),
        format!("{operation}: {error:#}"),
    )
}

pub(crate) fn agent_bus_error_from_status(operation: &str, status: Status) -> AgentBusError {
    let code = status.code();
    let source = anyhow::anyhow!("gRPC client {operation} error: {}", status.message());
    match code {
        Code::InvalidArgument => AgentBusError::InvalidArgument(source),
        Code::DeadlineExceeded => AgentBusError::Timeout(source),
        Code::ResourceExhausted | Code::Unavailable => AgentBusError::Unavailable(source),
        _ => AgentBusError::Internal(source),
    }
}

//This is a thread-safe gRPC handler;
//Internally, it uses a channel to communicate with a single-threaded implementation of AgentBus.

#[derive(Clone)]
pub struct AgentBusHandler {
    bus: ChanneledAgentBus,
}

impl AgentBusHandler {
    /// Create a gRPC handler that forwards to an existing channel-backed bus.
    pub fn new(bus: ChanneledAgentBus) -> Self {
        Self { bus }
    }
}

#[async_trait]
impl AgentBusServiceTrait for AgentBusHandler {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let response = self
            .bus
            .append(request.into_inner())
            .await
            .map_err(|error| status_from_agent_bus_error("append", error))?;
        Ok(Response::new(response))
    }

    async fn poll(&self, request: Request<PollRequest>) -> Result<Response<PollResponse>, Status> {
        let response = self
            .bus
            .poll(request.into_inner())
            .await
            .map_err(|error| status_from_agent_bus_error("poll", error))?;
        Ok(Response::new(response))
    }

    async fn read_next(
        &self,
        request: Request<ReadNextRequest>,
    ) -> Result<Response<ReadNextResponse>, Status> {
        let response = self
            .bus
            .read_next(request.into_inner())
            .await
            .map_err(|error| status_from_agent_bus_error("read_next", error))?;
        Ok(Response::new(response))
    }

    async fn check_tail(
        &self,
        request: Request<CheckTailRequest>,
    ) -> Result<Response<CheckTailResponse>, Status> {
        let response = self
            .bus
            .check_tail(request.into_inner())
            .await
            .map_err(|error| status_from_agent_bus_error("check_tail", error))?;
        Ok(Response::new(response))
    }

    async fn blocking_poll(
        &self,
        request: Request<BlockingPollRequest>,
    ) -> Result<Response<BlockingPollResponse>, Status> {
        let response = self
            .bus
            .blocking_poll(request.into_inner())
            .await
            .map_err(|error| status_from_agent_bus_error("blocking_poll", error))?;
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_status_preserves_agent_bus_error_category() {
        let cases = [
            (
                AgentBusError::InvalidArgument(anyhow::anyhow!("source")),
                Code::InvalidArgument,
            ),
            (
                AgentBusError::Timeout(anyhow::anyhow!("source")),
                Code::DeadlineExceeded,
            ),
            (
                AgentBusError::Unavailable(anyhow::anyhow!("source")),
                Code::Unavailable,
            ),
            (
                AgentBusError::Internal(anyhow::anyhow!("source")),
                Code::Internal,
            ),
        ];

        for (error, expected_code) in cases {
            let status = status_from_agent_bus_error("append", error);
            assert_eq!(status.code(), expected_code);
            let error = agent_bus_error_from_status("append", status);
            assert!(
                matches!(
                    (&error, expected_code),
                    (AgentBusError::InvalidArgument(_), Code::InvalidArgument)
                        | (AgentBusError::Timeout(_), Code::DeadlineExceeded)
                        | (AgentBusError::Unavailable(_), Code::Unavailable)
                        | (AgentBusError::Internal(_), Code::Internal)
                ),
                "gRPC round trip should preserve the AgentBus error category"
            );
        }
    }

    #[test]
    fn grpc_status_preserves_error_context() {
        let error = AgentBusError::Timeout(anyhow::anyhow!("source").context("context"));

        let status = status_from_agent_bus_error("append", error);

        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(status.message(), "append: context: source");
    }

    #[test]
    fn grpc_client_classifies_framework_statuses() {
        for (code, expected) in [
            (Code::Cancelled, "internal"),
            (Code::DeadlineExceeded, "timeout"),
            (Code::ResourceExhausted, "unavailable"),
            (Code::Unavailable, "unavailable"),
            (Code::Unknown, "internal"),
            (Code::NotFound, "internal"),
        ] {
            let error = agent_bus_error_from_status("append", Status::new(code, "source"));
            assert_eq!(
                error.to_string(),
                "gRPC client append error: source",
                "plain error display should retain the operation and remote status message"
            );
            let actual = match error {
                AgentBusError::Timeout(_) => "timeout",
                AgentBusError::Unavailable(_) => "unavailable",
                AgentBusError::Internal(_) => "internal",
                AgentBusError::InvalidArgument(_) => "invalid argument",
            };
            assert_eq!(actual, expected, "unexpected classification for {code:?}");
        }
    }

    #[test]
    fn grpc_client_does_not_expose_status_details_or_metadata() {
        let mut status = Status::with_details(Code::Unavailable, "source", vec![1_u8, 2, 3].into());
        status.metadata_mut().insert(
            "test-metadata",
            "present".parse().expect("metadata should parse"),
        );

        let error = agent_bus_error_from_status("append", status);

        assert!(matches!(&error, AgentBusError::Unavailable(_)));
        assert_eq!(error.to_string(), "gRPC client append error: source");
    }
}
