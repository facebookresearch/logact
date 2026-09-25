/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Channel-backed wrapper for the LogAct commit service.

pub mod channeled;

pub use channeled::ChanneledCommitService;
pub use channeled::ChanneledCommitServiceHandle;
