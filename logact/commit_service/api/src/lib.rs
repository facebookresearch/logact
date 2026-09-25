/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Trait API for the LogAct commit service.

pub mod policy;
pub mod traits;

pub use policy::PolicyProvider;
pub use policy::PolicyState;
pub use policy::VersionedPolicyState;
pub use traits::CommitError;
pub use traits::CommitIntentionCommand;
pub use traits::CommitIntentionOutcome;
pub use traits::CommitResult;
pub use traits::CommitSvc;
