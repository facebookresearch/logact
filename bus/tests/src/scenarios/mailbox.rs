/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_mailbox_list)]
mod defs {
    use agent_bus_proto_rust::agent_bus::*;
    use agentbus_api::AgentBus;
    use agentbus_core::mailbox::Mailbox;
    use agentbus_core::mailbox::check_mail;
    use agentbus_core::mailbox::send_mail;

    use crate::fixtures::AgentBusTestFixture;

    #[scenario(sim_only)]
    pub async fn run_test_mailbox_send_and_receive<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let sender = fixture.create_impl();
        let receiver = fixture.create_impl();
        let env = fixture.get_env();
        let bus_id = "test-bus";
        let mut mailbox = Mailbox::new(receiver, bus_id.to_string(), env);

        send_mail(&sender, bus_id, "bob", "hello alice").await?;

        let count = mailbox.poll_and_receive(None).await?;
        assert_eq!(count, 1);

        let messages = check_mail(&mailbox.mail_handle());
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].from, "bob");
        assert_eq!(messages[0].content, "hello alice");
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_mailbox_check_drains<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let sender = fixture.create_impl();
        let receiver = fixture.create_impl();
        let env = fixture.get_env();
        let bus_id = "test-bus";
        let mut mailbox = Mailbox::new(receiver, bus_id.to_string(), env);

        send_mail(&sender, bus_id, "bob", "msg1").await?;
        mailbox.poll_and_receive(None).await?;

        let first = check_mail(&mailbox.mail_handle());
        assert_eq!(first.len(), 1);

        let second = check_mail(&mailbox.mail_handle());
        assert!(second.is_empty());
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_mailbox_multiple_messages<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let sender = fixture.create_impl();
        let receiver = fixture.create_impl();
        let env = fixture.get_env();
        let bus_id = "test-bus";
        let mut mailbox = Mailbox::new(receiver, bus_id.to_string(), env);

        send_mail(&sender, bus_id, "bob", "msg1").await?;
        send_mail(&sender, bus_id, "charlie", "msg2").await?;
        send_mail(&sender, bus_id, "dave", "msg3").await?;

        mailbox.poll_and_receive(None).await?;

        let messages = check_mail(&mailbox.mail_handle());
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].content, "msg1");
        assert_eq!(messages[1].content, "msg2");
        assert_eq!(messages[2].content, "msg3");
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_mailbox_message_id_and_reply<F: AgentBusTestFixture>(
        fixture: &F,
    ) -> anyhow::Result<()> {
        let sender_bus = fixture.create_impl();
        let receiver = fixture.create_impl();
        let env = fixture.get_env();
        let bus_id = "test-bus";
        let mut mailbox = Mailbox::new(receiver, bus_id.to_string(), env);

        // Send a message with a message_id
        sender_bus
            .append(AppendRequest {
                agent_bus_id: bus_id.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_string(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::Mail(Mail {
                        sender_agent_id: "bob".to_string(),
                        content: Some(mail::Content::Message(Message {
                            body: "hello".to_string(),
                            message_id: "msg-123".to_string(),
                        })),
                    })),
                }),
            })
            .await?;

        // Send a reply
        sender_bus
            .append(AppendRequest {
                agent_bus_id: bus_id.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_string(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::Mail(Mail {
                        sender_agent_id: "charlie".to_string(),
                        content: Some(mail::Content::Reply(Reply {
                            message: Some(Message {
                                body: "reply to bob".to_string(),
                                message_id: "msg-456".to_string(),
                            }),
                            in_reply_to: "msg-123".to_string(),
                        })),
                    })),
                }),
            })
            .await?;

        mailbox.poll_and_receive(None).await?;

        let messages = check_mail(&mailbox.mail_handle());
        assert_eq!(messages.len(), 2);

        assert_eq!(messages[0].from, "bob");
        assert_eq!(messages[0].content, "hello");
        assert_eq!(messages[0].message_id, "msg-123");
        assert_eq!(messages[0].in_reply_to, "");

        assert_eq!(messages[1].from, "charlie");
        assert_eq!(messages[1].content, "reply to bob");
        assert_eq!(messages[1].message_id, "msg-456");
        assert_eq!(messages[1].in_reply_to, "msg-123");
        Ok(())
    }
}
pub use defs::*;
