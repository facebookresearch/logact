/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A sequencer that allows a window of writes in flight, run as an event loop.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::Poll;

use agentbus_api::environment::Environment;
use futures::FutureExt;
use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;

use crate::event_loop::EventLoop;
use crate::sequencer::Claim;
use crate::sequencer::Sequencer;

// Appends to a bus may have up to this many writes in flight.
const WINDOW_SIZE: u64 = 8;

/// Assigns append positions for a bus, with up to `WINDOW_SIZE` writes in flight.
/// A loop owns the state and handles requests in order; the methods send them.
pub struct WindowedSequencer {
    event_loop: EventLoop<Request>,
}

enum Request {
    Claim(ClaimRequest),
    Advance(u64),
}

/// A claim below an optional bound, and where to reply with it.
struct ClaimRequest {
    bound: Option<u64>,
    reply: oneshot::Sender<Option<WindowedClaim>>,
}

/// The loop's state.
struct State {
    window: Window,
    // Claims not yet answered, in arrival order.
    waiting_claims: VecDeque<ClaimRequest>,
    // Each in-flight position and whether its claim was set written, or `false`
    // if it was dropped.
    in_flight: FuturesUnordered<LocalBoxFuture<'static, (u64, bool)>>,
}

/// The claimed and written positions from `base` on; the others are unwritten.
#[derive(Default)]
struct Window {
    // Every position before `base` is written.
    base: u64,
    slots: BTreeMap<u64, Slot>,
}

enum Slot {
    InFlight,
    Written,
}

/// A position claimed from the loop. Setting it sends the loop whether it was
/// written; dropping it unsent closes the channel, which the loop reads as unwritten.
pub struct WindowedClaim {
    position: u64,
    written: oneshot::Sender<bool>,
}

impl<E: Environment + 'static> Sequencer<E> for WindowedSequencer {
    type Claim<'a> = WindowedClaim;

    const TAIL_WINDOW_SIZE: u64 = WINDOW_SIZE;

    fn new(next: u64, environment: Rc<E>) -> Self {
        Self {
            event_loop: EventLoop::spawn(environment, State::new(next), State::run),
        }
    }

    async fn claim(&self, bound: Option<u64>) -> Option<WindowedClaim> {
        self.event_loop
            .ask(|reply| Request::Claim(ClaimRequest { bound, reply }))
            .await
    }

    /// Moves past `tail`, below which every position is written, including any
    /// claimed there.
    fn advance(&self, tail: u64) {
        self.event_loop.send(Request::Advance(tail));
    }
}

impl State {
    fn new(base: u64) -> Self {
        Self {
            window: Window {
                base,
                ..Default::default()
            },
            waiting_claims: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
        }
    }

    /// Handles requests and claims being set until the sequencer is dropped. A
    /// claim that cannot be answered yet waits, and is retried after every change.
    async fn run<E: Environment>(
        mut self,
        mut new_requests: mpsc::UnboundedReceiver<Request>,
        _environment: Rc<E>,
    ) {
        loop {
            futures::select_biased! {
                (position, written) = self.in_flight.select_next_some() => {
                    self.window.complete_write(position, written);
                }
                request = new_requests.next() => match request {
                    Some(Request::Claim(claim)) => self.waiting_claims.push_back(claim),
                    Some(Request::Advance(tail)) => self.window.advance(tail),
                    None => return,
                },
            }
            // In arrival order, so a claim never takes a position from one that came before it.
            for claim in std::mem::take(&mut self.waiting_claims) {
                self.handle(claim);
            }
        }
    }

    /// Answers `claim`, or queues it to retry if it must wait. A claim whose
    /// caller has gone away is dropped, so it is unwritten.
    fn handle(&mut self, claim: ClaimRequest) {
        match self.window.claim_first_unwritten(claim.bound) {
            Poll::Ready(position) => {
                let _ = claim
                    .reply
                    .send(position.map(|position| self.start_write(position)));
            }
            Poll::Pending => self.waiting_claims.push_back(claim),
        }
    }

    /// Keeps `position` in flight until its claim is set or dropped.
    fn start_write(&mut self, position: u64) -> WindowedClaim {
        let (set, written) = oneshot::channel();
        self.in_flight
            .push(async move { (position, written.await.unwrap_or(false)) }.boxed_local());
        WindowedClaim {
            position,
            written: set,
        }
    }
}

impl Claim for WindowedClaim {
    fn position(&self) -> u64 {
        self.position
    }

    fn set(self, written: bool) {
        let _ = self.written.send(written);
    }
}

impl Window {
    /// Claims the first unwritten position in the window and below `bound`, is
    /// ready with `None` if every position before `bound` is written, and is
    /// pending otherwise.
    fn claim_first_unwritten(&mut self, bound: Option<u64>) -> Poll<Option<u64>> {
        let bound = bound.unwrap_or(u64::MAX);
        if self.base >= bound {
            return Poll::Ready(None);
        }
        match (self.base..bound.min(self.base + WINDOW_SIZE))
            .find(|position| !self.slots.contains_key(position))
        {
            Some(position) => {
                self.slots.insert(position, Slot::InFlight);
                Poll::Ready(Some(position))
            }
            None => Poll::Pending,
        }
    }

    /// Ends the write of a claimed `position`, unless `advance` has moved past it.
    fn complete_write(&mut self, position: u64, written: bool) {
        if position < self.base {
            return;
        }
        if written {
            self.slots.insert(position, Slot::Written);
        } else {
            self.slots.remove(&position);
        }
        self.advance(self.base);
    }

    /// Moves past `tail` and past the written positions from there.
    fn advance(&mut self, tail: u64) {
        self.base = self.base.max(tail);
        while matches!(self.slots.get(&self.base), Some(Slot::Written)) {
            self.base += 1;
        }
        self.slots = self.slots.split_off(&self.base);
    }
}

#[cfg(test)]
mod tests {
    use agentbus_api::RealEnvironment;
    use futures::FutureExt;

    use super::*;

    async fn claim(sequencer: &WindowedSequencer, bound: Option<u64>) -> Option<WindowedClaim> {
        Sequencer::<RealEnvironment>::claim(sequencer, bound).await
    }

    /// Runs `test` on a new sequencer, whose loop is a local task.
    async fn with_sequencer(test: impl AsyncFnOnce(&WindowedSequencer)) {
        tokio::task::LocalSet::new()
            .run_until(async {
                test(&WindowedSequencer::new(0, Rc::new(RealEnvironment::new()))).await
            })
            .await;
    }

    /// Checks that a claim bounded by 1 takes position 0, and returns `None` once
    /// it is written.
    async fn assert_bounded_claim_takes_position_0(sequencer: &WindowedSequencer) {
        let bounded = claim(sequencer, Some(1))
            .await
            .expect("position 0 is unwritten");
        assert_eq!(bounded.position(), 0);
        bounded.set(true);
        assert!(
            claim(sequencer, Some(1)).await.is_none(),
            "position 0 is written"
        );
    }

    #[tokio::test]
    async fn dropped_claim_leaves_its_position_to_a_bounded_claim() {
        with_sequencer(async |sequencer| {
            drop(claim(sequencer, None).await);
            assert_bounded_claim_takes_position_0(sequencer).await;
        })
        .await;
    }

    #[tokio::test]
    async fn claim_dropped_before_its_reply_leaves_its_position_to_a_bounded_claim() {
        with_sequencer(async |sequencer| {
            assert!(claim(sequencer, None).now_or_never().is_none());
            assert_bounded_claim_takes_position_0(sequencer).await;
        })
        .await;
    }

    #[tokio::test]
    async fn advance_moves_past_a_claimed_position() {
        with_sequencer(async |sequencer| {
            let first = claim(sequencer, None).await.expect("the window has room");
            Sequencer::<RealEnvironment>::advance(sequencer, 2);
            drop(first);
            assert!(
                claim(sequencer, Some(2)).await.is_none(),
                "positions before 2 are written"
            );
        })
        .await;
    }
}
