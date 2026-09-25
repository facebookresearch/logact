/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TailableSpace API
//!
//! Defines a trait for address spaces that support a contiguous tail query.
//! Both `WriteOnceSpace` and `ConditionalWriteSpace` are supertraits of
//! `TailableSpace`, so the contract and error type live in one place.

use thiserror::Error;

/// Error type for tail operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum TailError {
    #[error("backend unavailable: {0}")]
    BackendUnavailable(String),
}

/// Result type alias for tail operations.
pub type TailResult<T> = std::result::Result<T, TailError>;

/// An address space that supports a contiguous tail query.
///
/// Returns the contiguous tail: the first unwritten address before which all
/// addresses are written. The `window_size` parameter bounds the maximum
/// distance between the contiguous tail and the non-contiguous tail (the first
/// unwritten address after which all addresses are unwritten). This gives the
/// implementation license to only scan the last `window_size` addresses rather
/// than the entire address space.
///
/// This is not strongly consistent / linearizable; it only has to correspond
/// to an unordered, non-atomic scan of the space that occurs within the
/// linearization span.
pub trait TailableSpace {
    fn tail(
        &self,
        space_id: &str,
        window_size: u64,
    ) -> impl std::future::Future<Output = TailResult<u64>>;
}
