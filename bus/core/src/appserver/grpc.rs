/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! gRPC adapter for AppServer.
//!
//! Uses a channel-based pattern (like [`AgentBusHandler`]) to bridge the
//! `Send + Sync` tonic handler into the single-threaded `AppServer`.

use std::net::SocketAddr;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus;
use agentbus_api::AgentBus;
use agentbus_api::environment::Environment;
use appserver_proto_rust::appserver::app_server_service_server::AppServerService;
use appserver_proto_rust::appserver::app_server_service_server::AppServerServiceServer;
use appserver_proto_rust::appserver::*;
use async_trait::async_trait;
use futures::StreamExt;
use signal_hook::consts::signal::SIGINT;
use signal_hook::consts::signal::SIGTERM;
use signal_hook_tokio::Signals;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use super::AppServer;
use super::GateResult;
use crate::mailbox;

// ---------------------------------------------------------------------------
// Operations dispatched via channels
// ---------------------------------------------------------------------------

enum Op {
    Gate {
        intention: String,
        reply: oneshot::Sender<Result<GateResult, anyhow::Error>>,
    },
    CheckMail {
        reply: oneshot::Sender<Vec<crate::mailbox::MailLogEntry>>,
    },
    Append {
        payload: agent_bus::Payload,
        reply: oneshot::Sender<Result<i64, anyhow::Error>>,
    },
    Poll {
        start_log_position: i64,
        max_entries: i32,
        filter: Option<agent_bus::PayloadTypeFilter>,
        reply: oneshot::Sender<Result<(Vec<agent_bus::BusEntry>, bool), anyhow::Error>>,
    },
}

// ---------------------------------------------------------------------------
// gRPC handler (Clone + Send + Sync)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppServerGrpcHandler {
    tx: UnboundedSender<Op>,
}

impl AppServerGrpcHandler {
    /// Helper to handle channel-based RPC call: send an op, await the reply.
    async fn channel_call<T>(
        tx: &UnboundedSender<Op>,
        op_fn: impl FnOnce(oneshot::Sender<Result<T, anyhow::Error>>) -> Op,
        method_name: &'static str,
    ) -> Result<T, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(op_fn(reply_tx))
            .map_err(|_| Status::internal(format!("{}: worker terminated", method_name)))?;
        reply_rx
            .await
            .map_err(|_| Status::internal(format!("{}: worker terminated", method_name)))?
            .map_err(|e| Status::internal(format!("{}: {}", method_name, e)))
    }
}

/// Spawn a worker on the current LocalSet that receives Ops and dispatches
/// them to the AppServer. Returns the gRPC handler.
pub fn spawn_grpc_worker<T: AgentBus + Clone + 'static, E: Environment + 'static>(
    app: Rc<AppServer<T, E>>,
) -> AppServerGrpcHandler {
    let (tx, mut rx) = unbounded_channel::<Op>();

    tokio::task::spawn_local(async move {
        while let Some(op) = rx.recv().await {
            match op {
                Op::Gate { intention, reply } => {
                    let app = app.clone();
                    tokio::task::spawn_local(async move {
                        let result = super::gate_check(&app, &intention).await;
                        let _ = reply.send(result);
                    });
                }
                Op::CheckMail { reply } => {
                    let messages = mailbox::check_mail(app.mail_handle());
                    let _ = reply.send(messages);
                }
                Op::Append { payload, reply } => {
                    let req = agent_bus::AppendRequest {
                        agent_bus_id: app.bus_id().to_string(),
                        bus_id: Some(agent_bus::BusId {
                            agent_bus_id: app.bus_id().to_string(),
                        }),
                        payload: Some(payload),
                    };
                    let result = app
                        .bus()
                        .append(req)
                        .await
                        .map(|r| r.log_position)
                        .map_err(anyhow::Error::new);
                    let _ = reply.send(result);
                }
                Op::Poll {
                    start_log_position,
                    max_entries,
                    filter,
                    reply,
                } => {
                    let req = agent_bus::PollRequest {
                        agent_bus_id: app.bus_id().to_string(),
                        bus_id: Some(agent_bus::BusId {
                            agent_bus_id: app.bus_id().to_string(),
                        }),
                        start_log_position,
                        max_entries,
                        filter,
                    };
                    let result = app
                        .bus()
                        .poll(req)
                        .await
                        .map(|r| (r.entries, r.complete))
                        .map_err(anyhow::Error::new);
                    let _ = reply.send(result);
                }
            }
        }
    });

    AppServerGrpcHandler { tx }
}

// ---------------------------------------------------------------------------
// tonic service impl
// ---------------------------------------------------------------------------

#[async_trait]
impl AppServerService for AppServerGrpcHandler {
    async fn gate(&self, request: Request<GateRequest>) -> Result<Response<GateResponse>, Status> {
        let req = request.into_inner();
        let result = Self::channel_call(
            &self.tx,
            |reply| Op::Gate {
                intention: req.intention,
                reply,
            },
            "gate",
        )
        .await?;

        Ok(Response::new(GateResponse {
            approved: result.approved,
            reason: result.reason,
            intention_id: result.intention_id,
        }))
    }

    async fn check_mail(
        &self,
        _request: Request<CheckMailRequest>,
    ) -> Result<Response<CheckMailResponse>, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Op::CheckMail { reply: reply_tx })
            .map_err(|_| Status::internal("worker terminated"))?;

        let messages = reply_rx
            .await
            .map_err(|_| Status::internal("worker terminated"))?;

        let proto_messages = messages
            .into_iter()
            .map(|m| {
                let content = if m.in_reply_to.is_empty() {
                    Some(agent_bus::mail::Content::Message(agent_bus::Message {
                        body: m.content,
                        message_id: m.message_id,
                    }))
                } else {
                    Some(agent_bus::mail::Content::Reply(agent_bus::Reply {
                        message: Some(agent_bus::Message {
                            body: m.content,
                            message_id: m.message_id,
                        }),
                        in_reply_to: m.in_reply_to,
                    }))
                };
                MailMessage {
                    mail: Some(agent_bus::Mail {
                        sender_agent_id: m.from,
                        content,
                    }),
                    position: m.position,
                }
            })
            .collect();

        Ok(Response::new(CheckMailResponse {
            messages: proto_messages,
        }))
    }

    async fn append(
        &self,
        request: Request<AppServerAppendRequest>,
    ) -> Result<Response<AppServerAppendResponse>, Status> {
        let req = request.into_inner();
        let payload = req
            .payload
            .ok_or_else(|| Status::invalid_argument("missing payload"))?;

        let log_position =
            Self::channel_call(&self.tx, |reply| Op::Append { payload, reply }, "append").await?;

        Ok(Response::new(AppServerAppendResponse { log_position }))
    }

    async fn poll(
        &self,
        request: Request<AppServerPollRequest>,
    ) -> Result<Response<AppServerPollResponse>, Status> {
        let req = request.into_inner();

        let (entries, complete) = Self::channel_call(
            &self.tx,
            |reply| Op::Poll {
                start_log_position: req.start_log_position,
                max_entries: req.max_entries,
                filter: req.filter,
                reply,
            },
            "poll",
        )
        .await?;

        Ok(Response::new(AppServerPollResponse { entries, complete }))
    }
}

// ---------------------------------------------------------------------------
// Server startup
// ---------------------------------------------------------------------------

/// Run the gRPC server on the given port with graceful shutdown on SIGTERM/SIGINT.
pub async fn run_grpc_server(
    handler: AppServerGrpcHandler,
    host: &str,
    port: u16,
) -> Result<(), tonic::transport::Error> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap();
    tracing::info!(port = port, "AppServer gRPC listening");

    let mut signals = Signals::new([SIGTERM, SIGINT]).expect("failed to register signal handlers");

    tonic::transport::Server::builder()
        .add_service(AppServerServiceServer::new(handler))
        .serve_with_shutdown(addr, async move {
            signals.next().await;
            tracing::info!("Shutting down AppServer gRPC...");
            signals.handle().close();
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus;
    use agentbus_api::environment::RealEnvironment;
    use agentbus_simple::InMemoryAgentBus;
    use appserver_proto_rust::appserver::app_server_service_server::AppServerService;
    use tonic::Request;

    use super::*;

    /// Create a handler with an in-flight gate() that will never resolve.
    /// No decider is running, so no Commit/Abort is ever produced and
    /// gate_check blocks until timeout.
    async fn handler_with_blocking_gate() -> AppServerGrpcHandler {
        let env = Rc::new(RealEnvironment::new());
        let bus = InMemoryAgentBus::new(env.clone());
        let app = Rc::new(AppServer::new(
            bus,
            "test-bus".into(),
            Duration::from_secs(5),
            env,
        ));
        let handler = spawn_grpc_worker(app);

        let h = handler.clone();
        tokio::task::spawn_local(async move {
            let _ = h
                .gate(Request::new(GateRequest {
                    intention: "blocking".into(),
                }))
                .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        handler
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_poll_not_blocked_by_gate() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let handler = handler_with_blocking_gate().await;
                let result = tokio::time::timeout(
                    Duration::from_millis(200),
                    handler.poll(Request::new(AppServerPollRequest {
                        start_log_position: 0,
                        max_entries: 10,
                        filter: None,
                    })),
                )
                .await;
                assert!(result.is_ok(), "poll() blocked by in-flight gate()");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_append_not_blocked_by_gate() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let handler = handler_with_blocking_gate().await;
                let result = tokio::time::timeout(
                    Duration::from_millis(200),
                    handler.append(Request::new(AppServerAppendRequest {
                        payload: Some(agent_bus::Payload {
                            payload: Some(agent_bus::payload::Payload::Intention(
                                agent_bus::Intention {
                                    intention: Some(
                                        agent_bus::intention::Intention::StringIntention(
                                            "test".into(),
                                        ),
                                    ),
                                    ..Default::default()
                                },
                            )),
                        }),
                    })),
                )
                .await;
                assert!(result.is_ok(), "append() blocked by in-flight gate()");
            })
            .await;
    }
}
