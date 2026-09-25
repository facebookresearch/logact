/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mailbox component for agentbus
//!
//! Polls an AgentBus for Mail entries, buffering them into a shared deque
//! for consumption. Each agent has its own bus, so all mail on the bus
//! is addressed to that agent.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBus;
use agentbus_api::environment::Environment;
use thiserror::Error;

const MAX_MAIL_BUFFER: usize = 10_000;

#[derive(Error, Debug)]
pub enum MailboxError {
    #[error("AgentBus call failed: {0}")]
    FailedAgentBusCall(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
pub struct MailLogEntry {
    pub from: String,
    pub content: String,
    pub position: i64,
    pub message_id: String,
    pub in_reply_to: String,
}

pub struct Mailbox<T, E> {
    agent_bus: T,
    agent_bus_id: String,
    next_log_position: i64,
    mail: Rc<RefCell<VecDeque<MailLogEntry>>>,
    environment: Rc<E>,
}

impl<T: AgentBus, E: Environment> Mailbox<T, E> {
    pub fn new(agent_bus: T, agent_bus_id: String, environment: Rc<E>) -> Self {
        Self {
            agent_bus,
            agent_bus_id,
            next_log_position: 0,
            mail: Rc::new(RefCell::new(VecDeque::new())),
            environment,
        }
    }

    pub fn mail_handle(&self) -> Rc<RefCell<VecDeque<MailLogEntry>>> {
        self.mail.clone()
    }

    /// Poll the bus for new mail entries.
    /// `timeout`: how long to wait for new entries. `None` returns immediately (non-blocking).
    pub async fn poll_and_receive(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<usize, MailboxError> {
        let timeout_ms = timeout.map(|d| d.as_millis() as i32).unwrap_or(0);
        let payload_types = vec![SelectivePollType::Mail as i32];

        let response = self
            .agent_bus
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                start_log_position: self.next_log_position,
                max_entries: 64,
                filter: Some(PayloadTypeFilter {
                    payload_types: payload_types.clone(),
                }),
                timeout_ms,
            })
            .await
            .map_err(|e| {
                MailboxError::FailedAgentBusCall(anyhow::anyhow!("blocking_poll failed: {:?}", e))
            })?;

        if !response.entries.is_empty() {
            tracing::info!(
                bus_id = %self.agent_bus_id,
                entries = response.entries.len(),
                start_pos = self.next_log_position,
                "Mailbox polled entries from bus"
            );
        }

        let count = response.entries.len();
        for entry in &response.entries {
            self.process_entry(entry);
        }
        self.next_log_position = response.next_start_position;

        Ok(count)
    }

    /// Run the mailbox in a loop, calling poll_and_receive repeatedly.
    /// `blocking_timeout` controls how long each call waits. Defaults to 10s if `None`.
    ///
    /// Runs forever. Prefer `run_until_cancelled` in tests so the loop exits
    /// when the owning `BusRuntime` is dropped and the simulator can go idle.
    pub async fn run(&mut self, blocking_timeout: Option<Duration>) {
        self.run_until_cancelled(blocking_timeout, std::future::pending::<()>())
            .await
    }

    /// Like `run`, but exits when `stop` resolves. Used by `spawn_bus_runtime`
    /// so that dropping the owning `BusRuntime` causes this loop to terminate
    /// at the next yield point.
    pub async fn run_until_cancelled<S>(&mut self, blocking_timeout: Option<Duration>, stop: S)
    where
        S: std::future::Future + Unpin,
    {
        use futures::FutureExt;
        const DEFAULT_BLOCKING_TIMEOUT: Duration = Duration::from_secs(10);
        let timeout = Some(blocking_timeout.unwrap_or(DEFAULT_BLOCKING_TIMEOUT));
        let mut stop = stop.fuse();
        loop {
            futures::select_biased! {
                _ = stop => return,
                result = self.poll_and_receive(timeout).fuse() => {
                    if let Err(e) = result {
                        tracing::error!("Mailbox poll failed: {}", e);
                        self.environment.sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }

    fn process_entry(&mut self, entry: &BusEntry) {
        let log_position = entry.header.as_ref().map(|h| h.log_position).unwrap_or(0);

        if let Some(ref payload) = entry.payload {
            if let Some(payload::Payload::Mail(ref mail)) = payload.payload {
                if self.mail.borrow().len() >= MAX_MAIL_BUFFER {
                    tracing::warn!(
                        "Mailbox buffer full ({} entries), dropping oldest mail",
                        MAX_MAIL_BUFFER
                    );
                    self.mail.borrow_mut().pop_front();
                }
                tracing::info!(
                    pos = log_position,
                    from = %mail.sender_agent_id,
                    "Mailbox processing mail entry"
                );
                let (body, message_id, in_reply_to) = match &mail.content {
                    Some(mail::Content::Message(msg)) => {
                        (msg.body.clone(), msg.message_id.clone(), String::new())
                    }
                    Some(mail::Content::Reply(reply)) => {
                        let msg = reply.message.as_ref();
                        (
                            msg.map(|m| m.body.clone()).unwrap_or_default(),
                            msg.map(|m| m.message_id.clone()).unwrap_or_default(),
                            reply.in_reply_to.clone(),
                        )
                    }
                    None => (String::new(), String::new(), String::new()),
                };
                self.mail.borrow_mut().push_back(MailLogEntry {
                    from: mail.sender_agent_id.clone(),
                    content: body,
                    position: log_position,
                    message_id,
                    in_reply_to,
                });
                tracing::info!(
                    buffered = self.mail.borrow().len(),
                    "Mailbox buffered mail message"
                );
            } else {
                tracing::debug!(pos = log_position, "Mailbox skipping non-mail entry");
            }
        }
    }
}

/// Drain all messages from the mailbox handle.
pub fn check_mail(handle: &Rc<RefCell<VecDeque<MailLogEntry>>>) -> Vec<MailLogEntry> {
    let messages: Vec<MailLogEntry> = handle.borrow_mut().drain(..).collect();
    if !messages.is_empty() {
        tracing::info!(count = messages.len(), "check_mail drained messages");
    }
    messages
}

/// Append a Mail entry to the bus.
pub async fn send_mail(
    bus: &impl AgentBus,
    bus_id: &str,
    sender: &str,
    content: &str,
) -> Result<(), MailboxError> {
    let payload = Payload {
        payload: Some(payload::Payload::Mail(Mail {
            sender_agent_id: sender.to_string(),
            content: Some(mail::Content::Message(Message {
                body: content.to_string(),
                message_id: String::new(),
            })),
        })),
    };
    bus.append(AppendRequest {
        agent_bus_id: bus_id.to_string(),
        bus_id: Some(BusId {
            agent_bus_id: bus_id.to_string(),
        }),
        payload: Some(payload),
    })
    .await
    .map_err(|e| MailboxError::FailedAgentBusCall(anyhow::anyhow!("Send mail failed: {:?}", e)))?;
    Ok(())
}
