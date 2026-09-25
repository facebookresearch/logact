/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::os::unix::fs::PermissionsExt as _;

use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::intention;
use agentbus_api::AgentBus as _;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitSvc as _;
use logact_commit_service_grpc::GrpcCommitSvcClient;
use tokio::sync::oneshot;
use tonic::transport::Endpoint;

use crate::server::serve_with_shutdown;
use crate::socket::bind_socket;
use crate::sqlite_backed_commit_service::create_sqlite_backed_commit_service;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_commit_service_and_agent_bus_over_one_socket() {
    let temp = tempfile::tempdir().expect("temporary directory should be created");
    let state_dir = temp.path().join("state");
    let socket = state_dir.join("logact.sock");
    let sqlite_path = state_dir.join("logact.db");
    let service = create_sqlite_backed_commit_service(&sqlite_path)
        .await
        .expect("embedded LogAct should initialize from SQLite");
    let (listener, _socket_lock) = bind_socket(&socket).await.expect("Unix socket should bind");
    assert_eq!(
        tokio::fs::metadata(&state_dir)
            .await
            .expect("state directory metadata should be readable")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        tokio::fs::metadata(&socket)
            .await
            .expect("socket metadata should be readable")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        tokio::fs::metadata(&sqlite_path)
            .await
            .expect("database metadata should be readable")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_with_shutdown(listener, service, async move {
        let _ = shutdown_rx.await;
    }));

    let endpoint = Endpoint::from_shared(format!("unix://{}", socket.display()))
        .expect("socket endpoint should be valid");
    let channel = endpoint
        .connect()
        .await
        .expect("client should connect to the local server");
    let client = GrpcCommitSvcClient::new(channel);

    let outcome = client
        .commit_intention(CommitIntentionCommand {
            bus_id: BusId {
                agent_bus_id: "test-session".to_string(),
            },
            intention: intention::Intention::StringIntention("echo hello".to_string()),
        })
        .await
        .expect("commit should succeed");
    assert!(outcome.approved);

    let tail = client
        .agent_bus()
        .check_tail(CheckTailRequest {
            agent_bus_id: "test-session".to_string(),
            bus_id: Some(BusId {
                agent_bus_id: "test-session".to_string(),
            }),
        })
        .await
        .expect("AgentBus should be served on the same socket");
    assert!(tail.tail_position > outcome.log_position);

    shutdown_tx
        .send(())
        .expect("server should still be waiting for shutdown");
    server
        .await
        .expect("server task should join")
        .expect("server should stop cleanly");
}

#[tokio::test]
async fn replaces_a_stale_socket() {
    let temp = tempfile::tempdir().expect("temporary directory should be created");
    let socket = temp.path().join("state/logact.sock");
    let (listener, socket_lock) = bind_socket(&socket).await.expect("Unix socket should bind");
    drop(listener);
    drop(socket_lock);

    let (replacement, _replacement_lock) = bind_socket(&socket)
        .await
        .expect("stale Unix socket should be replaced");

    drop(replacement);
}

#[tokio::test]
async fn prevents_a_second_socket_owner() {
    let temp = tempfile::tempdir().expect("temporary directory should be created");
    let socket = temp.path().join("state/logact.sock");
    let (listener, _socket_lock) = bind_socket(&socket).await.expect("Unix socket should bind");

    let error = bind_socket(&socket)
        .await
        .expect_err("live Unix socket should not be replaced");
    assert!(error.to_string().contains("daemon lock"));

    drop(listener);
}

#[tokio::test]
async fn preserves_a_live_socket_without_a_lock() {
    let temp = tempfile::tempdir().expect("temporary directory should be created");
    let socket = temp.path().join("state/logact.sock");
    let (listener, socket_lock) = bind_socket(&socket).await.expect("Unix socket should bind");
    drop(socket_lock);

    let error = bind_socket(&socket)
        .await
        .expect_err("live Unix socket should not be replaced");
    assert!(error.to_string().contains("already listening"));

    drop(listener);
}

#[tokio::test]
async fn preserves_a_non_socket_path() {
    let temp = tempfile::tempdir().expect("temporary directory should be created");
    let socket = temp.path().join("state/logact.sock");
    crate::ensure_private_parent_directory(&socket)
        .await
        .expect("private state directory should be created");
    tokio::fs::write(&socket, b"not a socket")
        .await
        .expect("file should be created");

    let error = bind_socket(&socket)
        .await
        .expect_err("non-socket path should not be replaced");
    assert!(error.to_string().contains("non-socket"));
    assert_eq!(
        tokio::fs::read(&socket)
            .await
            .expect("file should remain readable"),
        b"not a socket"
    );
}
