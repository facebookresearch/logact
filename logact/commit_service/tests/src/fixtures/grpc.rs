/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! CommitService conformance fixture over the local gRPC and SQLite stack.

use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use agentbus_api::RealEnvironment;
use anyhow::Context as _;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::IntegrationFixture;
use fbinit::FacebookInit;
use logact_commit_service_grpc::GrpcCommitSvcClient;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

pub struct GrpcCommitServiceFixture {
    channel: Channel,
    environment: Rc<RealEnvironment>,
    server: JoinHandle<Result<()>>,
    _temp: Arc<TempDir>,
}

impl GrpcCommitServiceFixture {
    pub(super) fn channel(&self) -> Channel {
        self.channel.clone()
    }
}

impl ConformanceFixture for GrpcCommitServiceFixture {
    type Env = RealEnvironment;
    type Impl = GrpcCommitSvcClient;

    fn get_env(&self) -> Rc<Self::Env> {
        self.environment.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        GrpcCommitSvcClient::new(self.channel())
    }
}

impl IntegrationFixture for GrpcCommitServiceFixture {
    async fn new_async(_fb: FacebookInit) -> Result<Self> {
        let temp = Arc::new(tempfile::tempdir()?);
        let state_dir = temp.path().join("state");
        let socket = state_dir.join("logact.sock");
        let sqlite_path = state_dir.join("logact.sqlite");
        let server_socket = socket.clone();
        let server_temp = temp.clone();
        let mut server = tokio::spawn(async move {
            let _temp = server_temp;
            logact_local_server_lib::run_server(server_socket, sqlite_path, std::future::pending())
                .await
        });
        let channel = tokio::select! {
            channel = connect(&socket) => channel,
            result = &mut server => {
                result.context("local LogAct server task failed during startup")??;
                anyhow::bail!("local LogAct server stopped during startup");
            }
        };
        let channel = match channel {
            Ok(channel) => channel,
            Err(error) => {
                server.abort();
                let _ = server.await;
                return Err(error);
            }
        };
        Ok(Self {
            channel,
            environment: Rc::new(RealEnvironment::new()),
            server,
            _temp: temp,
        })
    }
}

impl Drop for GrpcCommitServiceFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn connect(socket: &Path) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(format!("unix://{}", socket.display()))?
        .connect_timeout(Duration::from_millis(250));
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return Ok(channel),
            Err(_error) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => {
                return Err(error).context("timed out connecting to the local LogAct server");
            }
        }
    }
}
