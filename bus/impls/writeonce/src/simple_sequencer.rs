/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A sequencer that allows one write at a time.

use std::cell::Cell;
use std::rc::Rc;

use agentbus_api::environment::Environment;
use futures_intrusive::sync::LocalSemaphore;
use futures_intrusive::sync::LocalSemaphoreReleaser;

use crate::sequencer::Claim;
use crate::sequencer::Sequencer;

/// Assigns append positions for a bus, one write at a time.
pub struct SimpleSequencer {
    // Every position before `next` is known to be written.
    next: Cell<u64>,
    // Held by a claim until it is dropped; appends take FIFO turns.
    turn: LocalSemaphore,
}

/// A position claimed under the bus's turn, which dropping the claim gives back.
pub struct SimpleClaim<'a> {
    position: u64,
    next: &'a Cell<u64>,
    _turn: LocalSemaphoreReleaser<'a>,
}

impl<E: Environment> Sequencer<E> for SimpleSequencer {
    type Claim<'a> = SimpleClaim<'a>;

    fn new(next: u64, _environment: Rc<E>) -> Self {
        Self {
            next: Cell::new(next),
            turn: LocalSemaphore::new(true, 1),
        }
    }

    // A window of 1 appends after the highest surviving entry, including after TTL expiry.
    const TAIL_WINDOW_SIZE: u64 = 1;

    async fn claim(&self) -> SimpleClaim<'_> {
        let turn = self.turn.acquire(1).await;
        SimpleClaim {
            position: self.next.get(),
            next: &self.next,
            _turn: turn,
        }
    }

    fn advance(&self, tail: u64) {
        self.next.set(self.next.get().max(tail));
    }
}

impl Claim for SimpleClaim<'_> {
    fn position(&self) -> u64 {
        self.position
    }

    fn set(self, written: bool) {
        if written {
            self.next.set(self.next.get().max(self.position + 1));
        }
    }
}

#[cfg(test)]
mod tests {
    use agentbus_api::RealEnvironment;
    use futures::FutureExt;

    use super::*;

    fn claim(sequencer: &SimpleSequencer) -> Option<SimpleClaim<'_>> {
        Sequencer::<RealEnvironment>::claim(sequencer).now_or_never()
    }

    #[test]
    fn dropped_claim_gives_back_the_turn_and_its_position() {
        let sequencer = SimpleSequencer::new(0, Rc::new(RealEnvironment::new()));
        let first = claim(&sequencer).expect("the turn is free");
        assert!(
            claim(&sequencer).is_none(),
            "the first claim holds the turn"
        );
        drop(first);
        let second = claim(&sequencer).expect("dropping the first claim gave back the turn");
        assert_eq!(second.position(), 0);
        second.set(true);
        assert_eq!(claim(&sequencer).expect("the turn is free").position(), 1);
    }
}
