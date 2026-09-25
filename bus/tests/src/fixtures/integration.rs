/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::net::SocketAddr;
use std::rc::Rc;

use agentbus_api::environment::RealEnvironment;
use agentbus_core::AgentBusHandler;
use agentbus_core::AgentBusServiceServer;
use agentbus_core::ChanneledAgentBus;
use agentbus_core::client::AgentBusClient;
use anyhow::Result;
use conformance::ConformanceFixture;
use fbinit::FacebookInit;
use tokio::time::Duration;
use tonic::transport::Server;

use super::IntegrationFixture;
use super::WriteOnceAgentBusGenericFixture;
use crate::conditional_write_space::fixtures::sqlite::SqliteConditionalWriteSpaceFixture;
use crate::write_once_space::fixtures::WriteOnceAdapterFixture;

pub type SqliteAgentBusFixture =
    WriteOnceAgentBusGenericFixture<WriteOnceAdapterFixture<SqliteConditionalWriteSpaceFixture>>;

pub struct IntegrationTestFixture {
    server_handle: tokio::task::JoinHandle<()>,
    #[allow(dead_code)]
    fb: FacebookInit,
    #[allow(dead_code)]
    addr: SocketAddr,
    env: Rc<RealEnvironment>,
    client: AgentBusClient,
}

impl ConformanceFixture for IntegrationTestFixture {
    type Env = RealEnvironment;
    type Impl = AgentBusClient;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        // Per tonic docs: cloning is cheap and the underlying communication channel is shared.
        // https://docs.rs/tonic/latest/tonic/client/index.html
        self.client.clone()
    }
}

impl IntegrationTestFixture {
    pub async fn new_async(fb: FacebookInit) -> Result<Self> {
        let (server_handle, port) = create_test_grpc_server().await?;
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        let env = Rc::new(RealEnvironment::new());

        // Create the client once
        let client = crate::common::integration_utils::make_client(fb, addr).await?;

        Ok(Self {
            server_handle,
            fb,
            addr,
            env,
            client,
        })
    }
}

impl Drop for IntegrationTestFixture {
    fn drop(&mut self) {
        self.server_handle.abort();
    }
}

impl IntegrationFixture for IntegrationTestFixture {
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        Self::new_async(fb).await
    }
}

async fn create_test_grpc_server() -> Result<(tokio::task::JoinHandle<()>, u16)> {
    let port = crate::common::integration_utils::find_free_port().await?;
    let env = RealEnvironment::new();
    let bus = ChanneledAgentBus::new_in_process(move || {
        use std::rc::Rc;

        use agentbus_simple::InMemoryAgentBus;

        InMemoryAgentBus::new(Rc::new(env))
    });
    let handler = AgentBusHandler::new(bus);

    let service = AgentBusServiceServer::new(handler);
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let server_handle = tokio::spawn(async move {
        let _ = Server::builder().add_service(service).serve(addr).await;
    });

    Ok((server_handle, port))
}
