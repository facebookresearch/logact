/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use logact_commit_service_api::CommitError;
use logact_commit_service_core::ChanneledCommitServiceHandle;
use logact_commit_service_grpc_proto_rust::logact_commit_service::CommitIntentionRequest;
use logact_commit_service_grpc_proto_rust::logact_commit_service::CommitIntentionResponse;
use logact_commit_service_grpc_proto_rust::logact_commit_service::commit_service_server::CommitService;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::translation;

/// Thin gRPC handler for a channel-backed CommitService.
#[derive(Clone)]
pub struct GrpcCommitServiceHandler {
    service: ChanneledCommitServiceHandle,
}

impl GrpcCommitServiceHandler {
    pub fn new(service: ChanneledCommitServiceHandle) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl CommitService for GrpcCommitServiceHandler {
    async fn commit_intention(
        &self,
        request: Request<CommitIntentionRequest>,
    ) -> Result<Response<CommitIntentionResponse>, Status> {
        let command =
            translation::commit_intention::native_from_proto_request(request.into_inner())?;
        let outcome = self
            .service
            .commit_intention(command)
            .await
            .map_err(commit_status)?;

        Ok(Response::new(
            translation::commit_intention::proto_from_native_response(outcome),
        ))
    }
}

fn commit_status(error: CommitError) -> Status {
    match error {
        CommitError::InvalidArgument(_) => Status::invalid_argument(error.to_string()),
        CommitError::Concurrency(_) => Status::aborted(error.to_string()),
        CommitError::Timeout(_) => Status::deadline_exceeded(error.to_string()),
        CommitError::Unavailable(_) => Status::unavailable(error.to_string()),
        CommitError::Internal(_) => Status::internal(error.to_string()),
    }
}
