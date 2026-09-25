/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// The visible output and implementation-defined ordering evidence for an operation.
#[derive(Clone, Debug)]
pub struct OperationResult<Output, Tag> {
    pub output: Output,
    pub tag: Tag,
}
