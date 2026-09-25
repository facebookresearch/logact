/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! OSS integration tests (eg. DynamoDB integration tests)

mod conditional_write_space;

use agentbus_tests::agentbus_emit_integration;
use agentbus_tests::agentbus_scenarios;
use agentbus_tests::conditional_write_space_emit_integration;
use agentbus_tests::conditional_write_space_scenarios;
use agentbus_tests::fixtures::IntegrationFixture;
use agentbus_tests::fixtures::WriteOnceAgentBusGenericFixture;
use agentbus_tests::tailable_space::fixtures::CwsTailableFixture;
use agentbus_tests::tailable_space::fixtures::WosTailableFixture;
use agentbus_tests::tailable_space_emit_integration;
use agentbus_tests::tailable_space_scenarios;
use agentbus_tests::write_once_space::fixtures::WriteOnceAdapterFixture;
use agentbus_tests::write_once_space_emit_integration;
use agentbus_tests::write_once_space_scenarios;
use conditional_write_space::fixtures::DynamoConditionalWriteSpaceFixture;

#[rustfmt::skip]
mod tests {
use super::*;

// =============================================================================
// AgentBus tests
// =============================================================================

agentbus_scenarios!(agentbus_emit_integration, dynamo, WriteOnceAgentBusGenericFixture<WriteOnceAdapterFixture<DynamoConditionalWriteSpaceFixture>>);

// =============================================================================
// WriteOnceSpace tests
// =============================================================================

write_once_space_scenarios!(write_once_space_emit_integration, dynamo, WriteOnceAdapterFixture<DynamoConditionalWriteSpaceFixture>);

// =============================================================================
// ConditionalWriteSpace tests
// =============================================================================

conditional_write_space_scenarios!(conditional_write_space_emit_integration, cws_dynamo, DynamoConditionalWriteSpaceFixture);

// =============================================================================
// TailableSpace tests
// =============================================================================

tailable_space_scenarios!(tailable_space_emit_integration, wos_dynamo, WosTailableFixture<WriteOnceAdapterFixture<DynamoConditionalWriteSpaceFixture>>);
tailable_space_scenarios!(tailable_space_emit_integration, cws_dynamo, CwsTailableFixture<DynamoConditionalWriteSpaceFixture>);
}
