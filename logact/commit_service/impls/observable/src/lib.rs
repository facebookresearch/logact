/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Observability decorator for the Rust commit-service interface.

mod observable_commit_svc;

pub use observable_commit_svc::ObservableCommitSvc;
