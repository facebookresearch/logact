/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agentbus_core::client::AgentBusClient;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_grpc_proto_rust::logact_commit_service::commit_service_client::CommitServiceClient;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;

use crate::translation;

/// Core-facing client for a CommitService gRPC channel.
#[derive(Clone)]
pub struct GrpcCommitSvcClient {
    commit: CommitServiceClient<Channel>,
    bus: AgentBusClient,
}

impl GrpcCommitSvcClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            commit: CommitServiceClient::new(channel.clone()),
            bus: AgentBusClient::new(channel),
        }
    }
}

impl CommitSvc for GrpcCommitSvcClient {
    type Bus = AgentBusClient;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn commit_intention(
        &self,
        command: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        let response = self
            .commit
            .clone()
            .commit_intention(translation::commit_intention::proto_from_native_request(
                command,
            ))
            .await
            .map_err(commit_error)?
            .into_inner();
        Ok(translation::commit_intention::native_from_proto_response(
            response,
        ))
    }
}

fn commit_error(error: Status) -> CommitError {
    match error.code() {
        Code::InvalidArgument => CommitError::InvalidArgument(error.into()),
        Code::Aborted => CommitError::Concurrency(error.into()),
        Code::DeadlineExceeded => CommitError::Timeout(error.into()),
        Code::ResourceExhausted | Code::Unavailable => CommitError::Unavailable(error.into()),
        _ => CommitError::Internal(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes_are_classified() {
        for (code, expected) in [
            (Code::InvalidArgument, "invalid argument"),
            (Code::Aborted, "concurrency"),
            (Code::DeadlineExceeded, "timeout"),
            (Code::ResourceExhausted, "unavailable"),
            (Code::Unavailable, "unavailable"),
            (Code::Internal, "internal"),
        ] {
            let error = commit_error(Status::new(code, "source"));
            let actual = match error {
                CommitError::InvalidArgument(_) => "invalid argument",
                CommitError::Concurrency(_) => "concurrency",
                CommitError::Timeout(_) => "timeout",
                CommitError::Unavailable(_) => "unavailable",
                CommitError::Internal(_) => "internal",
            };
            assert_eq!(actual, expected, "unexpected classification for {code:?}");
        }
    }
}
