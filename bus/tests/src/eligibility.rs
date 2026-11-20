/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Eligibility decisions and the reference gate for eligibility gate scenarios.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::rc::Rc;

use agentbus_api::AuthorizationError;
use agentbus_api::RequestContext;
use agentbus_api::RequestGate;
use async_trait::async_trait;

/// A service identity that test eligibility gates exempt from the check.
pub const EXEMPT_SERVICE: &str = "test.exempt.service";

/// The answer `InMemoryEligibilityLookup` gives for one unixname.
#[derive(Clone, Copy, Debug)]
pub enum Eligibility {
    Eligible,
    Ineligible,
    Unavailable,
}

/// The error `InMemoryEligibilityLookup` reports for an unavailable decision.
#[derive(Debug, thiserror::Error)]
#[error("eligibility service unavailable")]
pub struct LookupUnavailable;

/// Table-driven eligibility lookup that answers immediately and counts
/// lookups. Unknown unixnames are ineligible. Clones share the table and the
/// counts.
#[derive(Clone, Default)]
pub struct InMemoryEligibilityLookup {
    decisions: Rc<RefCell<HashMap<String, Eligibility>>>,
    lookups: Rc<RefCell<BTreeMap<String, usize>>>,
}

impl InMemoryEligibilityLookup {
    pub fn set(&self, unixname: &str, eligibility: Eligibility) {
        self.decisions
            .borrow_mut()
            .insert(unixname.to_owned(), eligibility);
    }

    /// How many times each unixname has been looked up.
    pub fn lookups(&self) -> BTreeMap<String, usize> {
        self.lookups.borrow().clone()
    }
}

impl InMemoryEligibilityLookup {
    pub async fn check(&self, unixname: &str) -> anyhow::Result<bool> {
        *self
            .lookups
            .borrow_mut()
            .entry(unixname.to_owned())
            .or_default() += 1;
        let eligibility = self.decisions.borrow().get(unixname).copied();
        match eligibility.unwrap_or(Eligibility::Ineligible) {
            Eligibility::Eligible => Ok(true),
            Eligibility::Ineligible => Ok(false),
            Eligibility::Unavailable => Err(LookupUnavailable.into()),
        }
    }
}

/// Observes a gate's decision for one valid test user, retaining lookup failures.
pub async fn gate_decision(gate: &impl RequestGate, unixname: &str) -> anyhow::Result<bool> {
    match gate
        .admit(
            &RequestContext::default().with_user(unixname),
            "probe",
            "test-bus",
        )
        .await
    {
        Ok(()) => Ok(true),
        Err(AuthorizationError::Denied(_)) => Ok(false),
        Err(AuthorizationError::Unavailable(error)) => Err(error),
    }
}

/// Reference `RequestGate`: the eligibility admission rules applied directly
/// to the in-memory decision table, with no network, retries, cache, or
/// warnings. Clones share the table and the lookup counts.
#[derive(Clone)]
pub struct InMemoryRequestGate {
    eligibility: InMemoryEligibilityLookup,
    exempt_services: BTreeSet<String>,
}

impl InMemoryRequestGate {
    pub fn new(
        eligibility: InMemoryEligibilityLookup,
        exempt_services: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            eligibility,
            exempt_services: exempt_services.into_iter().collect(),
        }
    }

    /// The unixname whose eligibility decides admission, or `None` for an
    /// exempt caller.
    fn caller_unixname<'a>(
        &self,
        ctx: &'a RequestContext,
    ) -> Result<Option<&'a str>, AuthorizationError> {
        if ctx
            .services
            .iter()
            .any(|service| self.exempt_services.contains(service))
        {
            return Ok(None);
        }

        let unixnames: Vec<&str> = ctx
            .users
            .iter()
            .map(String::as_str)
            .filter(|user| !user.is_empty() && !user.bytes().all(|byte| byte.is_ascii_digit()))
            .collect();
        let [unixname] = unixnames.as_slice() else {
            return Err(AuthorizationError::Denied(anyhow::anyhow!(
                "caller needs exactly one unixname, has {unixnames:?}"
            )));
        };
        Ok(Some(unixname))
    }
}

#[async_trait(?Send)]
impl RequestGate for InMemoryRequestGate {
    async fn admit(
        &self,
        ctx: &RequestContext,
        _method: &'static str,
        _agent_bus_id: &str,
    ) -> Result<(), AuthorizationError> {
        let Some(unixname) = self.caller_unixname(ctx)? else {
            return Ok(());
        };

        *self
            .eligibility
            .lookups
            .borrow_mut()
            .entry(unixname.to_owned())
            .or_default() += 1;
        let decision = self.eligibility.decisions.borrow().get(unixname).copied();
        match decision.unwrap_or(Eligibility::Ineligible) {
            Eligibility::Eligible => Ok(()),
            Eligibility::Ineligible => Err(AuthorizationError::Denied(anyhow::anyhow!(
                "`{unixname}` is ineligible"
            ))),
            Eligibility::Unavailable => {
                Err(AuthorizationError::Unavailable(LookupUnavailable.into()))
            }
        }
    }
}
