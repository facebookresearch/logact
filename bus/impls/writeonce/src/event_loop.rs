/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A loop that owns some state and handles the requests sent to it in order.

use std::future::Future;
use std::rc::Rc;

use agentbus_api::environment::Environment;
use futures::channel::mpsc;
use futures::channel::oneshot;

/// Sends requests to a loop, which runs until its `EventLoop` is dropped.
pub(crate) struct EventLoop<Request> {
    requests: mpsc::UnboundedSender<Request>,
}

impl<Request: 'static> EventLoop<Request> {
    /// Spawns `run(state, requests, environment)` on `environment`'s local
    /// executor, where `requests` receives the requests sent to the returned
    /// `EventLoop`.
    pub(crate) fn spawn<S, E: Environment, F: Future<Output = ()> + 'static>(
        environment: Rc<E>,
        state: S,
        run: impl FnOnce(S, mpsc::UnboundedReceiver<Request>, Rc<E>) -> F,
    ) -> Self {
        let (requests, receiver) = mpsc::unbounded();
        environment.spawn_local(run(state, receiver, environment.clone()));
        Self { requests }
    }

    pub(crate) fn send(&self, request: Request) {
        // The loop runs until this is dropped, so this cannot fail.
        let _ = self.requests.unbounded_send(request);
    }

    /// Sends the request built around a reply channel and waits for the reply.
    pub(crate) async fn ask<T>(&self, request: impl FnOnce(oneshot::Sender<T>) -> Request) -> T {
        let (reply, response) = oneshot::channel();
        self.send(request(reply));
        response.await.expect("the loop runs until it is dropped")
    }
}
