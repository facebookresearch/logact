/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AgentBus API - Core traits and interfaces
//!
//! This crate defines the fundamental traits and types for AgentBus:
//! - `AgentBus` trait: The main interface for AgentBus implementations
//! - `Environment` trait: Clock and randomness abstraction for testing

pub mod any_helpers;
pub mod conditional_write_space;
pub mod environment;
pub mod helpers;
pub mod logger;
pub mod metrics;
pub mod payload_types;
pub mod retry;
pub mod tailable_space;
pub mod traits;
pub mod validation;
pub mod voter;
pub mod write_once_space;

// Re-export commonly used items
// Re-export proto types for convenience
pub use agent_bus_proto_rust::agent_bus::*;
pub use any_helpers::pack_any;
pub use any_helpers::unpack_any;
pub use conditional_write_space::ConditionalWriteError;
pub use conditional_write_space::ConditionalWriteResult;
pub use conditional_write_space::ConditionalWriteSpace;
pub use conditional_write_space::Version;
pub use conditional_write_space::VersionedValue;
pub use environment::Clock;
pub use environment::Environment;
pub use environment::MonotonicInstant;
pub use environment::RealEnvironment;
pub use environment::UnsafeWallTime;
pub use helpers::ParsedEntry;
pub use helpers::get_payload_type;
pub use helpers::parse_entry;
pub use helpers::payload_matches_filter;
pub use helpers::read_range;
pub use helpers::resolve_bus_id;
pub use logger::AgentbusLogger;
pub use logger::InMemoryLogger;
pub use logger::LogFieldValue;
pub use logger::LogRow;
pub use logger::NoopLogger;
pub use metrics::AgentBusMetrics;
pub use metrics::InMemoryMetrics;
pub use metrics::NoopMetrics;
pub use metrics::PrefixedMetrics;
pub use payload_types::PayloadType;
pub use retry::RetryConfig;
pub use retry::RetryDecision;
pub use retry::RetryFailure;
pub use retry::retry;
pub use tailable_space::TailError;
pub use tailable_space::TailResult;
pub use tailable_space::TailableSpace;
pub use traits::AgentBus;
pub use traits::AgentBusError;
pub use traits::BusResult;
pub use validation::validate_bus_id;
pub use voter::Voter;
pub use voter::VoterContext;
pub use write_once_space::WriteOnceError;
pub use write_once_space::WriteOnceResult;
pub use write_once_space::WriteOnceSpace;
