/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::CachingRequestGate;
use agentbus_api::EligibilityGate;
use agentbus_api::RequestGate;
use conformance::ConformanceFixture;
use tokio::sync::Notify;
use tokio::sync::oneshot;

use crate::eligibility::EXEMPT_SERVICE;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

pub trait EligibilityCacheTestFixture:
    ConformanceFixture<Env = Simulator, Impl: RequestGate>
{
    fn lookup_controller(&self) -> ControlledEligibilityLookup;
    fn create_with_config(&self, capacity: NonZeroUsize, ttl: Duration) -> Self::Impl;
}

type Reply = oneshot::Sender<anyhow::Result<bool>>;

/// Lookups wait until the scenario answers them; clones share pending requests.
/// Requests whose lookup was cancelled are discarded.
#[derive(Clone, Default)]
pub struct ControlledEligibilityLookup {
    requests: Rc<RefCell<VecDeque<(String, Reply)>>>,
    requested: Rc<Notify>,
}

impl ControlledEligibilityLookup {
    pub async fn wait_for_lookup(&self, unixname: &str) {
        loop {
            let notified = self.requested.notified();
            self.discard_cancelled();
            if self
                .requests
                .borrow()
                .iter()
                .any(|(name, _)| name == unixname)
            {
                return;
            }
            notified.await;
        }
    }

    /// Answers the oldest waiting lookup for `unixname`.
    pub fn answer(&self, unixname: &str, result: anyhow::Result<bool>) {
        self.reply(unixname, result, false);
    }

    /// Answers the newest waiting lookup for `unixname`.
    pub fn answer_last(&self, unixname: &str, result: anyhow::Result<bool>) {
        self.reply(unixname, result, true);
    }

    fn reply(&self, unixname: &str, result: anyhow::Result<bool>, newest: bool) {
        self.discard_cancelled();
        let mut requests = self.requests.borrow_mut();
        let is_match = |(requested, _): &(String, Reply)| requested == unixname;
        let index = if newest {
            requests.iter().rposition(is_match)
        } else {
            requests.iter().position(is_match)
        }
        .expect("a lookup should be waiting for the unixname");
        let (_, reply) = requests.remove(index).expect("the index should be valid");
        reply
            .send(result)
            .expect("the lookup should still be waiting");
    }

    pub fn waiting(&self) -> usize {
        self.discard_cancelled();
        self.requests.borrow().len()
    }

    fn discard_cancelled(&self) {
        self.requests
            .borrow_mut()
            .retain(|(_, reply)| !reply.is_closed());
    }
}

impl ControlledEligibilityLookup {
    pub async fn check(&self, unixname: &str) -> anyhow::Result<bool> {
        let (reply, response) = oneshot::channel();
        self.requests
            .borrow_mut()
            .push_back((unixname.to_owned(), reply));
        self.requested.notify_waiters();
        response.await?
    }
}

/// Fixture for `EligibilityGate` over a `ControlledEligibilityLookup`.
pub struct ControlledEligibilityGateFixture {
    lookup: ControlledEligibilityLookup,
    env: Rc<Simulator>,
}

impl ControlledEligibilityGateFixture {
    fn gate(&self) -> EligibilityGate<Simulator> {
        let lookup = self.lookup.clone();
        EligibilityGate::new(
            move |unixname| {
                let lookup = lookup.clone();
                async move { lookup.check(&unixname).await }
            },
            self.env.clone(),
        )
        .with_exempt_services([EXEMPT_SERVICE.to_owned()])
    }
}

impl ConformanceFixture for ControlledEligibilityGateFixture {
    type Env = Simulator;
    type Impl = Rc<EligibilityGate<Simulator>>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        Rc::new(self.gate())
    }
}

impl SimulatorFixture for ControlledEligibilityGateFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            lookup: ControlledEligibilityLookup::default(),
            env: Rc::new(simulator),
        }
    }
}

/// Fixture for a `CachingRequestGate` over `ControlledEligibilityGateFixture`'s
/// gate. Each `create_impl` is a new cache.
pub struct ControlledCachingEligibilityGateFixture {
    inner: ControlledEligibilityGateFixture,
}

impl ConformanceFixture for ControlledCachingEligibilityGateFixture {
    type Env = Simulator;
    type Impl = Rc<CachingRequestGate<EligibilityGate<Simulator>, Simulator>>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.create_with_config(
            NonZeroUsize::new(2).expect("capacity should be nonzero"),
            Duration::from_secs(300),
        )
    }
}

impl EligibilityCacheTestFixture for ControlledCachingEligibilityGateFixture {
    fn lookup_controller(&self) -> ControlledEligibilityLookup {
        self.inner.lookup.clone()
    }

    fn create_with_config(&self, capacity: NonZeroUsize, ttl: Duration) -> Self::Impl {
        Rc::new(CachingRequestGate::new(
            self.inner.gate(),
            self.inner.env.clone(),
            capacity,
            ttl,
        ))
    }
}

impl SimulatorFixture for ControlledCachingEligibilityGateFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: ControlledEligibilityGateFixture::new(simulator),
        }
    }
}
