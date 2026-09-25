/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AgentBus client wrapper that implements the AgentBus trait
//!
//! This wrapper allows Decider to work with both:
//! - Direct AgentBus implementations (for simtests)
//! - gRPC client connections (for production DeciderService)

use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::AppendResponse;
use agent_bus_proto_rust::agent_bus::BlockingPollRequest;
use agent_bus_proto_rust::agent_bus::BlockingPollResponse;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::CheckTailResponse;
use agent_bus_proto_rust::agent_bus::PollRequest;
use agent_bus_proto_rust::agent_bus::PollResponse;
use agent_bus_proto_rust::agent_bus::ReadNextRequest;
use agent_bus_proto_rust::agent_bus::ReadNextResponse;
use agent_bus_proto_rust::agent_bus::agent_bus_service_client::AgentBusServiceClient;
use agentbus_api::AgentBus;
use agentbus_api::BusResult;
use agentbus_api::resolve_bus_id;
use anyhow::Result;
use tonic::transport::Channel;
use tracing::warn;

use crate::agent_bus_error_from_status;

/// Wrapper around a gRPC AgentBusService client that implements the AgentBus trait
///
/// This allows Decider to work with remote AgentBus instances via gRPC while
/// maintaining the same interface as direct AgentBus implementations.
///
/// # Example
/// ```ignore
/// use agent_bus_client::AgentBusClient;
/// use agent_bus_decider::Decider;
/// use std::sync::Arc;
///
/// // Create gRPC client
/// let channel = tonic::transport::Channel::from_static("http://[::1]:9999")
///     .connect()
///     .await?;
/// let client = AgentBusClient::new(channel);
///
/// // Wrap in Arc
/// let client = Arc::new(client);
///
/// // Use with Decider
/// let decider = Decider::new(client, agent_bus_id, factory);
/// ```
#[derive(Clone)]
pub struct AgentBusClient {
    /// The gRPC client that talks to the remote AgentBus service
    client: AgentBusServiceClient<Channel>,
}

impl AgentBusClient {
    /// Create a new AgentBusClient wrapping a gRPC channel
    pub fn new(channel: Channel) -> Self {
        Self {
            client: AgentBusServiceClient::new(channel),
        }
    }

    /// Create a new AgentBusClient by connecting to a host:port
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let channel = Channel::from_shared(addr.into())?.connect().await?;
        Ok(Self::new(channel))
    }
}

impl AgentBus for AgentBusClient {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        // Clone the client to get a mutable reference
        let mut client = self.client.clone();

        // Forward to gRPC client
        let response = client.append(request.clone()).await.map_err(|e| {
            // Log the error for observability
            warn!(
                error = ?e,
                agent_bus_id = bus_id,
                "gRPC client append failed"
            );
            agent_bus_error_from_status("append", e)
        })?;

        Ok(response.into_inner())
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        // Clone the client to get a mutable reference
        let mut client = self.client.clone();

        // Forward to gRPC client
        let response = client.poll(request.clone()).await.map_err(|e| {
            // Log the error for observability
            warn!(
                error = ?e,
                agent_bus_id = bus_id,
                start_position = request.start_log_position,
                max_entries = request.max_entries,
                "gRPC client poll failed"
            );
            agent_bus_error_from_status("poll", e)
        })?;

        Ok(response.into_inner())
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        let mut client = self.client.clone();

        let response = client.read_next(request.clone()).await.map_err(|e| {
            warn!(
                error = ?e,
                agent_bus_id = bus_id,
                start_position = request.start_log_position,
                end_position = request.end_log_position,
                max_entries = request.max_entries,
                "gRPC client read_next failed"
            );
            agent_bus_error_from_status("read_next", e)
        })?;

        Ok(response.into_inner())
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        let mut client = self.client.clone();

        let response = client.check_tail(request.clone()).await.map_err(|e| {
            warn!(
                error = ?e,
                agent_bus_id = bus_id,
                "gRPC client check_tail failed"
            );
            agent_bus_error_from_status("check_tail", e)
        })?;

        Ok(response.into_inner())
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        let bus_id = resolve_bus_id(&request.agent_bus_id, request.bus_id.as_ref());
        let mut client = self.client.clone();

        let response = client.blocking_poll(request.clone()).await.map_err(|e| {
            warn!(
                error = ?e,
                agent_bus_id = bus_id,
                "gRPC client blocking_poll failed"
            );
            agent_bus_error_from_status("blocking_poll", e)
        })?;

        Ok(response.into_inner())
    }
}
