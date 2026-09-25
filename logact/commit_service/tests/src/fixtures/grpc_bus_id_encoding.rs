/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! gRPC wire adapter for the shared CommitService bus-ID encoding fixture.

use std::marker::PhantomData;

use agent_bus_proto_rust::agent_bus::BusId;
use agentbus_core::client::AgentBusClient;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_grpc::translation;
use logact_commit_service_grpc_proto_rust::logact_commit_service::commit_service_client::CommitServiceClient;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;

use super::bus_id_encoding::BusIdEncoding;
use super::bus_id_encoding::BusIdEncodingFixtureFactory;
use super::grpc::GrpcCommitServiceFixture;

pub struct BusIdEncodingGrpcCommitSvcClient<M> {
    commit: CommitServiceClient<Channel>,
    bus: AgentBusClient,
    mode: PhantomData<M>,
}

impl<M> Clone for BusIdEncodingGrpcCommitSvcClient<M> {
    fn clone(&self) -> Self {
        Self {
            commit: self.commit.clone(),
            bus: self.bus.clone(),
            mode: PhantomData,
        }
    }
}

impl<M> BusIdEncodingGrpcCommitSvcClient<M> {
    fn new(channel: Channel) -> Self {
        Self {
            commit: CommitServiceClient::new(channel.clone()),
            bus: AgentBusClient::new(channel),
            mode: PhantomData,
        }
    }
}

impl<M: BusIdEncoding> CommitSvc for BusIdEncodingGrpcCommitSvcClient<M> {
    type Bus = AgentBusClient;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn commit_intention(
        &self,
        command: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        let mut request = translation::commit_intention::proto_from_native_request(command);
        let encoded = M::encode(
            &request.bus_id,
            request
                .typed_bus_id
                .as_ref()
                .map(|bus_id| bus_id.agent_bus_id.as_str()),
        );
        request.bus_id = encoded.legacy_bus_id;
        if !encoded.retain_typed_bus_id {
            request.typed_bus_id = None;
        }
        let response = self
            .commit
            .clone()
            .commit_intention(request)
            .await
            .map_err(commit_error)?
            .into_inner();
        Ok(translation::commit_intention::native_from_proto_response(
            response,
        ))
    }
}

impl<M: BusIdEncoding> BusIdEncodingFixtureFactory<M> for GrpcCommitServiceFixture {
    type EncodedImpl = BusIdEncodingGrpcCommitSvcClient<M>;

    fn create_encoded_impl(&self) -> Self::EncodedImpl {
        BusIdEncodingGrpcCommitSvcClient::new(self.channel())
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
