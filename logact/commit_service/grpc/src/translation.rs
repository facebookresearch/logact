/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Conversion helpers between the CommitService core API and protobuf types.

/// Translation helpers for the `commit_intention` RPC.
pub mod commit_intention {
    use agent_bus_proto_rust::agent_bus::BusId;
    use agent_bus_proto_rust::agent_bus::Intention;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitIntentionOutcome;
    use logact_commit_service_grpc_proto_rust::logact_commit_service::CommitIntentionRequest;
    use logact_commit_service_grpc_proto_rust::logact_commit_service::CommitIntentionResponse;
    use tonic::Status;

    /// Convert a native request into the protobuf request shape.
    ///
    /// Typically used by a gRPC client before sending the request.
    pub fn proto_from_native_request(command: CommitIntentionCommand) -> CommitIntentionRequest {
        let CommitIntentionCommand { bus_id, intention } = command;
        CommitIntentionRequest {
            bus_id: bus_id.agent_bus_id.clone(),
            intention: Some(Intention {
                intention: Some(intention),
                ..Default::default()
            }),
            typed_bus_id: Some(bus_id),
        }
    }

    /// Convert a protobuf request into the native request shape.
    ///
    /// Typically used by a gRPC server after receiving the request.
    pub fn native_from_proto_request(
        request: CommitIntentionRequest,
    ) -> Result<CommitIntentionCommand, Status> {
        let CommitIntentionRequest {
            bus_id,
            typed_bus_id,
            intention,
        } = request;
        let Intention {
            intention,
            policy_version_constraint,
        } = intention.ok_or_else(|| Status::invalid_argument("intention is required"))?;
        if policy_version_constraint.is_some() {
            return Err(Status::invalid_argument(
                "policy version constraints are managed by CommitService",
            ));
        }

        Ok(CommitIntentionCommand {
            bus_id: typed_bus_id.unwrap_or(BusId {
                agent_bus_id: bus_id,
            }),
            intention: intention
                .ok_or_else(|| Status::invalid_argument("intention variant is required"))?,
        })
    }

    /// Convert a native response into the protobuf response shape.
    ///
    /// Typically used by a gRPC server before sending the response.
    pub fn proto_from_native_response(outcome: CommitIntentionOutcome) -> CommitIntentionResponse {
        CommitIntentionResponse {
            approved: outcome.approved,
            reason: outcome.reason,
            log_position: outcome.log_position,
        }
    }

    /// Convert a protobuf response into the native response shape.
    ///
    /// Typically used by a gRPC client after receiving the response.
    pub fn native_from_proto_response(response: CommitIntentionResponse) -> CommitIntentionOutcome {
        CommitIntentionOutcome {
            approved: response.approved,
            reason: response.reason,
            log_position: response.log_position,
        }
    }

    #[cfg(test)]
    mod tests {
        use agent_bus_proto_rust::agent_bus::intention;
        use tonic::Code;

        use super::*;

        fn command() -> CommitIntentionCommand {
            CommitIntentionCommand {
                bus_id: BusId {
                    agent_bus_id: "test-bus".to_string(),
                },
                intention: intention::Intention::StringIntention("run tool".to_string()),
            }
        }

        #[test]
        fn request_round_trips_through_proto() {
            let command = command();
            let round_tripped =
                native_from_proto_request(proto_from_native_request(command.clone()))
                    .expect("generated request should be valid");

            assert_eq!(round_tripped, command);
        }

        #[test]
        fn request_prefers_typed_bus_id() {
            let command = native_from_proto_request(CommitIntentionRequest {
                bus_id: "ignored-legacy".to_string(),
                typed_bus_id: Some(BusId {
                    agent_bus_id: "typed".to_string(),
                }),
                intention: Some(Intention {
                    intention: Some(intention::Intention::StringIntention(
                        "run tool".to_string(),
                    )),
                    ..Default::default()
                }),
            })
            .expect("request should be valid");

            assert_eq!(command.bus_id.agent_bus_id, "typed");
        }

        #[test]
        fn request_accepts_legacy_bus_id() {
            let command = native_from_proto_request(CommitIntentionRequest {
                bus_id: "legacy".to_string(),
                typed_bus_id: None,
                intention: Some(Intention {
                    intention: Some(intention::Intention::StringIntention(
                        "run tool".to_string(),
                    )),
                    ..Default::default()
                }),
            })
            .expect("request should be valid");

            assert_eq!(command.bus_id.agent_bus_id, "legacy");
        }

        #[test]
        fn request_requires_intention() {
            let error = native_from_proto_request(CommitIntentionRequest {
                bus_id: "test-bus".to_string(),
                intention: None,
                typed_bus_id: Some(BusId {
                    agent_bus_id: "test-bus".to_string(),
                }),
            })
            .expect_err("missing intention should be rejected");

            assert_eq!(error.code(), Code::InvalidArgument);
        }

        #[test]
        fn request_requires_intention_variant() {
            let error = native_from_proto_request(CommitIntentionRequest {
                bus_id: "test-bus".to_string(),
                intention: Some(Intention::default()),
                typed_bus_id: Some(BusId {
                    agent_bus_id: "test-bus".to_string(),
                }),
            })
            .expect_err("missing intention variant should be rejected");

            assert_eq!(error.code(), Code::InvalidArgument);
        }

        #[test]
        fn request_rejects_policy_version_constraint() {
            let error = native_from_proto_request(CommitIntentionRequest {
                bus_id: "test-bus".to_string(),
                intention: Some(Intention {
                    intention: Some(intention::Intention::StringIntention(
                        "run tool".to_string(),
                    )),
                    policy_version_constraint: Some(
                        intention::PolicyVersionConstraint::RequiredPolicyVersion(1),
                    ),
                }),
                typed_bus_id: Some(BusId {
                    agent_bus_id: "test-bus".to_string(),
                }),
            })
            .expect_err("policy version constraint should be rejected");

            assert_eq!(error.code(), Code::InvalidArgument);
        }

        #[test]
        fn response_round_trips_through_proto() {
            let outcome = CommitIntentionOutcome {
                approved: false,
                reason: "blocked by policy".to_string(),
                log_position: 42,
            };
            let round_tripped =
                native_from_proto_response(proto_from_native_response(outcome.clone()));

            assert_eq!(round_tripped, outcome);
        }
    }
}
