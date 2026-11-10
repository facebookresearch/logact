/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

thread_local! {
    static LIVE_BYTES: Cell<isize> = const { Cell::new(0) };
}

/// Counts allocations and frees on the current thread. Simulator scenarios must
/// allocate and free their measured state on that same thread.
pub struct ThreadHeapCounter;

fn track(delta: isize) {
    let _ = LIVE_BYTES.try_with(|live| live.set(live.get() + delta));
}

fn thread_live_bytes() -> isize {
    LIVE_BYTES.with(Cell::get)
}

// SAFETY: allocation operations forward to `System` unchanged. Counting only
// touches a const-initialized thread-local and cannot re-enter the allocator.
unsafe impl GlobalAlloc for ThreadHeapCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's allocation contract is forwarded unchanged.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            track(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's allocation contract is forwarded unchanged.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            track(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's allocation contract is forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) };
        track(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's allocation contract is forwarded unchanged.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            track(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

#[conformance_macros::scenarios(commit_service_heap_growth_list)]
mod defs {
    use std::hint::black_box;
    use std::ops::Range;
    use std::rc::Rc;

    use agentbus_api::AgentBus;
    use agentbus_api::BusId;
    use agentbus_api::PollRequest;
    use agentbus_simulator::Simulator;
    use anyhow::Result;
    use logact_commit_service_api::CommitSvc;

    use super::thread_live_bytes;
    use crate::fixtures::CommitServiceTestFixture;

    fn check_heap_counter() -> Result<()> {
        let before = thread_live_bytes();
        let probe = black_box(vec![0u8; 1 << 20]);
        anyhow::ensure!(
            thread_live_bytes() - before == 1 << 20,
            "ThreadHeapCounter must observe dependency allocations"
        );
        drop(probe);
        anyhow::ensure!(
            thread_live_bytes() == before,
            "ThreadHeapCounter must observe dependency frees"
        );
        Ok(())
    }

    async fn poll_empty_buses(
        bus: &impl AgentBus,
        ids: Range<usize>,
        reuse_id: bool,
    ) -> Result<()> {
        for id in ids {
            let id = if reuse_id { 0 } else { id };
            let response = bus
                .poll(PollRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: format!("empty-{id:08}"),
                    }),
                    max_entries: 1,
                    ..Default::default()
                })
                .await?;
            anyhow::ensure!(response.entries.is_empty() && response.complete);
        }
        Ok(())
    }

    async fn run_poll_batch<S: CommitSvc + 'static>(
        env: &Simulator,
        service: &Rc<S>,
        ids: Range<usize>,
        reuse_id: bool,
    ) -> Result<()> {
        let service = Rc::clone(service);
        let handle =
            env.spawn(async move { poll_empty_buses(service.agent_bus(), ids, reuse_id).await });
        // Drive a bounded batch to quiescence before sampling. Each run retains
        // the simulator's normal guard while all batches share the same service.
        env.run();
        handle.await?
    }

    async fn check_empty_bus_polls<F: CommitServiceTestFixture<Env = Simulator, Impl: 'static>>(
        fixture: &F,
        reuse_id: bool,
    ) -> Result<()> {
        const MAX_POLLS: usize = 2_000_000;
        const BATCH: usize = 8192;
        const FLAT_BATCHES: usize = 32;
        check_heap_counter()?;
        let env = fixture.get_env();
        let service = Rc::new(fixture.create_impl());
        let mut polls = 0;
        let mut baseline = thread_live_bytes();
        let mut flat_batches = 0;
        // Discover a settled baseline from the workload, without knowing backend limits.
        while polls < MAX_POLLS {
            let end = (polls + BATCH).min(MAX_POLLS);
            run_poll_batch(&env, &service, polls..end, reuse_id).await?;
            polls = end;
            let current = thread_live_bytes();
            flat_batches = if current == baseline {
                flat_batches + 1
            } else {
                0
            };
            baseline = current;
            if flat_batches == FLAT_BATCHES {
                break;
            }
        }
        anyhow::ensure!(
            flat_batches == FLAT_BATCHES && MAX_POLLS - polls >= 4 * BATCH,
            "retained heap did not converge with room for four validation batches within {MAX_POLLS} empty polls (reuse_id={reuse_id}), last sample {baseline} bytes at poll {polls}",
        );
        let warmup = polls;
        let mut samples = [0; 4];
        for (batch, start) in (polls..MAX_POLLS).step_by(BATCH).enumerate() {
            let end = (start + BATCH).min(MAX_POLLS);
            run_poll_batch(&env, &service, start..end, reuse_id).await?;
            let current = thread_live_bytes();
            samples[batch % samples.len()] = current;
            anyhow::ensure!(
                current <= baseline,
                "after {} empty polls beyond warmup (reuse_id={reuse_id}), retained heap is {current} bytes, baseline {baseline}, growth {} bytes",
                end - warmup,
                current - baseline,
            );
        }
        eprintln!(
            "retained heap (reuse_id={reuse_id}): {MAX_POLLS} polls, warmup {warmup}, baseline {baseline}, final samples {samples:?}"
        );
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_empty_bus_polls_do_not_grow_retained_heap<
        F: CommitServiceTestFixture<Env = Simulator, Impl: 'static>,
    >(
        fixture: &F,
    ) -> Result<()> {
        check_empty_bus_polls(fixture, false).await
    }

    #[scenario(sim_only)]
    pub async fn run_test_repeated_empty_bus_polls_do_not_grow_retained_heap<
        F: CommitServiceTestFixture<Env = Simulator, Impl: 'static>,
    >(
        fixture: &F,
    ) -> Result<()> {
        check_empty_bus_polls(fixture, true).await
    }
}

pub use defs::*;
