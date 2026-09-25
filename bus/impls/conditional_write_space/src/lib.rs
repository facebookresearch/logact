/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Conditional write space implementations.

mod in_memory_conditional_write_space;
mod write_once_space_adapter;

pub use in_memory_conditional_write_space::InMemoryConditionalWriteSpace;
pub use write_once_space_adapter::WriteOnceSpaceAdapter;
