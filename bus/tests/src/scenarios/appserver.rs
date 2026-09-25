/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_appserver_list)]
mod defs {
    use std::rc::Rc;
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::*;
    use agentbus_core::appserver::AppServerConfig;
    use agentbus_core::appserver::build_and_spawn;
    use agentbus_core::appserver::gate_check;
    use agentbus_core::mailbox::Mailbox;
    use agentbus_core::mailbox::check_mail;
    use agentbus_core::mailbox::send_mail;

    use crate::common::helpers::append_decider_policy;
    use crate::fixtures::AgentBusTestFixture;

    #[scenario(sim_only)]
    pub async fn run_test_appserver_gate_auto_commit<F>(fixture: &F) -> anyhow::Result<()>
    where
        F: AgentBusTestFixture,
        F::Impl: Clone,
    {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let app = Rc::new(build_and_spawn(
            bus,
            AppServerConfig {
                bus_id: "bus-auto".to_string(),
                gate_check_timeout: Duration::from_secs(5),
                blocking_poll_interval: None,
            },
            env.clone(),
            Vec::new(),
        ));
        let result = gate_check(&app, "do something safe").await?;
        assert!(result.approved, "ON_BY_DEFAULT should auto-commit");
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_appserver_gate_off_by_default<F>(fixture: &F) -> anyhow::Result<()>
    where
        F: AgentBusTestFixture,
        F::Impl: Clone,
    {
        let env = fixture.get_env();
        let bus_for_policy = fixture.create_impl();
        append_decider_policy(
            &bus_for_policy,
            "bus-off".to_string(),
            DeciderPolicy::OffByDefault as i32,
        )
        .await;
        let bus = fixture.create_impl();
        let app = Rc::new(build_and_spawn(
            bus,
            AppServerConfig {
                bus_id: "bus-off".to_string(),
                gate_check_timeout: Duration::from_millis(200),
                blocking_poll_interval: None,
            },
            env.clone(),
            Vec::new(),
        ));
        let result = gate_check(&app, "do something").await?;
        assert!(!result.approved, "OFF_BY_DEFAULT with no voter should deny");
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_appserver_mail_via_build_and_spawn<F>(fixture: &F) -> anyhow::Result<()>
    where
        F: AgentBusTestFixture,
    {
        let env = fixture.get_env();
        let bus = fixture.create_impl();
        let append_bus = fixture.create_impl();
        let mut mailbox = Mailbox::new(bus, "bus-mail".to_string(), env.clone());
        let mail_handle = mailbox.mail_handle();

        send_mail(&append_bus, "bus-mail", "bob", "hello").await?;

        let count = mailbox.poll_and_receive(None).await?;
        assert_eq!(count, 1);

        let messages = check_mail(&mail_handle);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].from, "bob");
        assert_eq!(messages[0].content, "hello");
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_appserver_gate_timeout<F>(fixture: &F) -> anyhow::Result<()>
    where
        F: AgentBusTestFixture,
        F::Impl: Clone,
    {
        let env = fixture.get_env();
        let bus_for_policy = fixture.create_impl();
        append_decider_policy(
            &bus_for_policy,
            "bus-timeout".to_string(),
            DeciderPolicy::FirstBooleanWins as i32,
        )
        .await;
        // FIRST_BOOLEAN_WINS with no voter — decider waits for a vote that never comes
        let bus = fixture.create_impl();
        let app = Rc::new(build_and_spawn(
            bus,
            AppServerConfig {
                bus_id: "bus-timeout".to_string(),
                gate_check_timeout: Duration::from_millis(100),
                blocking_poll_interval: None,
            },
            env.clone(),
            Vec::new(),
        ));
        let result = gate_check(&app, "needs a vote").await?;
        assert!(
            !result.approved,
            "FIRST_BOOLEAN_WINS with no voter should timeout"
        );
        assert!(
            result.reason.contains("timed out"),
            "reason should mention timeout, got: {}",
            result.reason
        );
        Ok(())
    }
}
pub use defs::*;
