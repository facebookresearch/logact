/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Integration conformance tests for `PolicyRegister` over real backends.

use logact_commit_service_policy_register_tests::policy_register_integration_suite;

policy_register_integration_suite!();
