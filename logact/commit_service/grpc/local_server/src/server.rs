/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::path::PathBuf;

use agentbus_core::AgentBusHandler;
use agentbus_core::AgentBusServiceServer;
use agentbus_core::ChanneledAgentBus;
use anyhow::Context as _;
use anyhow::Result;
use logact_commit_service_api::CommitSvc as _;
use logact_commit_service_core::ChanneledCommitService;
use logact_commit_service_grpc::GrpcCommitServiceHandler;
use logact_commit_service_grpc_proto_rust::logact_commit_service::commit_service_server::CommitServiceServer;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use crate::socket::bind_socket;
use crate::sqlite_backed_commit_service::create_sqlite_backed_commit_service;

/// Serve LogAct and AgentBus gRPC services on a bound Unix-domain socket.
pub(crate) async fn serve_with_shutdown<Shutdown>(
    listener: UnixListener,
    service: ChanneledCommitService<ChanneledAgentBus>,
    shutdown: Shutdown,
) -> Result<()>
where
    Shutdown: Future<Output = ()> + Send + 'static,
{
    let bus = service.agent_bus().clone();
    let commit = service.into_commit_handle();
    let commit_service = CommitServiceServer::new(GrpcCommitServiceHandler::new(commit));
    let agent_bus_service = AgentBusServiceServer::new(AgentBusHandler::new(bus));

    Server::builder()
        .add_service(commit_service)
        .add_service(agent_bus_service)
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
        .context("local LogAct gRPC server failed")
}

/// Run a foreground server until `shutdown` resolves.
pub async fn run_server<Shutdown>(
    socket: PathBuf,
    sqlite_path: PathBuf,
    shutdown: Shutdown,
) -> Result<()>
where
    Shutdown: Future<Output = ()> + Send + 'static,
{
    let service = create_sqlite_backed_commit_service(sqlite_path).await?;
    let listener = bind_socket(&socket).await?;
    let server_result = serve_with_shutdown(listener, service, shutdown).await;
    let cleanup_result = match tokio::fs::remove_file(&socket).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to remove {}", socket.display())),
    };

    match (server_result, cleanup_result) {
        (Ok(()), cleanup_result) => cleanup_result,
        (Err(server_error), Ok(())) => Err(server_error),
        (Err(server_error), Err(cleanup_error)) => Err(server_error.context(format!(
            "also failed to remove socket {}: {cleanup_error:#}",
            socket.display()
        ))),
    }
}
