/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Call accounting shared by observable decorators.
//!
//! A decorator times one call to its inner implementation with
//! [`ObservedCall::observe`], which records the standard
//! `<operation>.<component>.{num_calls,latency_us,num_errors}` metrics, and
//! then adds its own counters and row fields from the returned [`Observed`].
//! Inside a decorator method, [`observed_call!`](crate::observed_call) names
//! the operation and component after the method and the decorator.

use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use crate::AgentBusMetrics;
use crate::Clock;
use crate::Environment;
use crate::LogRow;

/// One operation of an observable decorator: `operation` and `component` form
/// its metric keys, `<operation>.<component>.<metric>`, and every metric it
/// records carries `tags`.
pub struct ObservedCall<'a> {
    metrics: &'a dyn AgentBusMetrics,
    prefix: String,
    tags: &'a [(&'a str, &'a str)],
}

impl<'a> ObservedCall<'a> {
    pub fn new(metrics: &'a dyn AgentBusMetrics, operation: &str, component: &str) -> Self {
        Self {
            metrics,
            prefix: format!("{operation}.{component}"),
            tags: &[],
        }
    }

    pub fn with_tags(mut self, tags: &'a [(&'a str, &'a str)]) -> Self {
        self.tags = tags;
        self
    }

    /// The key of `metric` for this operation, e.g. `get.storage.num_misses`.
    pub fn key(&self, metric: &str) -> String {
        format!("{}.{metric}", self.prefix)
    }

    /// Adds `value` to this operation's `metric` counter.
    pub fn record_counter(&self, metric: &str, value: i64) {
        self.metrics
            .record_counter(&self.key(metric), value, self.tags);
    }

    /// Records `value` in this operation's `metric` histogram.
    pub fn record_histogram(&self, metric: &str, value: i64) {
        self.metrics
            .record_histogram(&self.key(metric), value, self.tags);
    }

    /// Awaits `call`, timing it on `env`'s monotonic clock. Records
    /// `num_calls` before the call, `latency_us` after it, and `num_errors` if
    /// it fails, then returns its result unchanged with the elapsed time.
    pub async fn observe<T, E>(
        &self,
        env: &impl Environment,
        call: impl Future<Output = Result<T, E>>,
    ) -> Observed<T, E> {
        self.record_counter("num_calls", 1);
        let start = env.with_clock(|clock| clock.monotonic_time());
        let result = call.await;
        let elapsed = env
            .with_clock(|clock| clock.monotonic_time())
            .duration_since(start);
        self.record_histogram("latency_us", elapsed.as_micros() as i64);
        if result.is_err() {
            self.record_counter("num_errors", 1);
        }
        Observed { result, elapsed }
    }
}

/// An [`ObservedCall`] named after the enclosing method of an
/// `Observable<Component>` decorator: the operation is the method's name and
/// the component is the decorator's type name without `Observable`, in snake
/// case. In `ObservableStorage::put_if_newer`, `observed_call!(metrics)` records
/// `put_if_newer.storage.*`.
#[macro_export]
macro_rules! observed_call {
    ($metrics:expr) => {{
        fn here() {}
        $crate::ObservedCall::new(
            $metrics,
            $crate::observe::operation_name(::std::any::type_name_of_val(&here)),
            &$crate::observe::component_name(::std::any::type_name::<Self>()),
        )
    }};
}

/// The name of the method enclosing `here`, a function item's type name such
/// as `crate::Type::method::{{closure}}::here`.
#[doc(hidden)]
pub fn operation_name(here: &'static str) -> &'static str {
    let mut path = here.strip_suffix("::here").unwrap_or(here);
    while let Some(outer) = path.strip_suffix("::{{closure}}") {
        path = outer;
    }
    path.rsplit("::").next().unwrap_or(path)
}

/// `type_name`'s last path segment without generics or an `Observable` prefix,
/// in snake case: `crate::ObservableAgentBus<T, E>` is `agent_bus`.
#[doc(hidden)]
pub fn component_name(type_name: &str) -> String {
    let path = type_name.split('<').next().unwrap_or(type_name);
    let name = path.rsplit("::").next().unwrap_or(path);
    let chars: Vec<char> = name
        .strip_prefix("Observable")
        .unwrap_or(name)
        .chars()
        .collect();
    chars
        .iter()
        .enumerate()
        .fold(String::new(), |mut snake, (i, &c)| {
            let starts_word = i > 0
                && c.is_uppercase()
                && (!chars[i - 1].is_uppercase()
                    || chars.get(i + 1).is_some_and(|next| next.is_lowercase()));
            if starts_word {
                snake.push('_');
            }
            snake.extend(c.to_lowercase());
            snake
        })
}

/// The result of an observed call and how long it took.
pub struct Observed<T, E> {
    pub result: Result<T, E>,
    pub elapsed: Duration,
}

impl<T, E: Display> Observed<T, E> {
    /// A row with `api_call_name`, the call's latency, and its error message
    /// if it failed. Decorators add their own fields.
    pub fn row(&self, api_call_name: &str) -> LogRow {
        let row = LogRow::default()
            .api_call_name(api_call_name)
            .latency_ms(self.elapsed.as_millis() as u64);
        match &self.result {
            Ok(_) => row,
            Err(error) => row.error_message(format!("{error:#}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use anyhow::anyhow;
    use async_trait::async_trait;

    use super::*;
    use crate::InMemoryMetrics;
    use crate::RealEnvironment;

    #[tokio::test]
    async fn observe_records_calls_latency_and_errors() {
        let metrics = InMemoryMetrics::new();
        let env = RealEnvironment::new();
        let call = ObservedCall::new(&metrics, "get", "storage").with_tags(&[("tier", "rc")]);

        let ok = call
            .observe(&env, async { Ok::<_, anyhow::Error>(7) })
            .await;
        let err = call
            .observe(&env, async { Err::<i32, _>(anyhow!("backend down")) })
            .await;
        call.record_counter("num_misses", 2);

        assert_eq!(ok.result.unwrap(), 7);
        assert_eq!(err.result.as_ref().unwrap_err().to_string(), "backend down");
        assert_eq!(metrics.counter("get.storage.num_calls"), 2);
        assert_eq!(metrics.counter("get.storage.num_errors"), 1);
        assert_eq!(metrics.counter("get.storage.num_misses"), 2);
        assert_eq!(metrics.histogram("get.storage.latency_us").len(), 2);
        assert_eq!(err.row("get").api_call_name, "get");
        assert_eq!(
            err.row("get").error_message.as_deref(),
            Some("backend down")
        );
    }

    #[tokio::test]
    async fn row_has_no_error_message_on_success() {
        let metrics = InMemoryMetrics::new();
        let observed = ObservedCall::new(&metrics, "admit", "request_gate")
            .observe(&RealEnvironment::new(), async {
                Ok::<_, anyhow::Error>(())
            })
            .await;

        let row = observed.row("append");

        assert_eq!(row.api_call_name, "append");
        assert_eq!(row.error_message, None);
        assert_eq!(metrics.counter("admit.request_gate.num_errors"), 0);
    }

    struct ObservableWriteOnceSpace;

    impl ObservableWriteOnceSpace {
        async fn put_if_newer(&self, metrics: &InMemoryMetrics) {
            let _ = observed_call!(metrics)
                .observe(&RealEnvironment::new(), async {
                    Ok::<_, anyhow::Error>(())
                })
                .await;
        }
    }

    #[async_trait(?Send)]
    trait Gate {
        async fn admit(&self, metrics: &InMemoryMetrics);
    }

    struct ObservableACLGate<T>(PhantomData<T>);

    #[async_trait(?Send)]
    impl<T> Gate for ObservableACLGate<T> {
        async fn admit(&self, metrics: &InMemoryMetrics) {
            observed_call!(metrics).record_counter("num_denials", 1);
        }
    }

    #[tokio::test]
    async fn observed_call_is_named_after_the_method_and_decorator() {
        let metrics = InMemoryMetrics::new();

        ObservableWriteOnceSpace.put_if_newer(&metrics).await;
        ObservableACLGate::<Vec<String>>(PhantomData)
            .admit(&metrics)
            .await;

        assert_eq!(
            metrics.counter("put_if_newer.write_once_space.num_calls"),
            1,
            "an inherent async method"
        );
        assert_eq!(
            metrics.counter("admit.acl_gate.num_denials"),
            1,
            "an async_trait method on a generic decorator, acronym kept as one word"
        );
    }

    #[test]
    fn component_name_strips_the_path_generics_and_prefix() {
        assert_eq!(
            component_name("a::b::ObservableAgentBus<c::D, e::F<G>>"),
            "agent_bus"
        );
        assert_eq!(component_name("Storage"), "storage");
        assert_eq!(
            component_name("ObservableHTTPClient2Pool"),
            "http_client2_pool"
        );
    }
}
