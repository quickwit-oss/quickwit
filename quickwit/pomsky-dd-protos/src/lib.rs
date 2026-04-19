// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Vendored Datadog-external protobuf definitions used by Pomsky.
//!
//! Datadog's agent-side repos publish Go and Java bindings alongside their
//! `.proto` files but do not publish Rust crates. Per the agent-payload
//! README:
//!
//! > Other consumers may copy the `.proto` files into their repository
//! > and generate their own bindings.
//!
//! This crate does that for the subset of Datadog-external protos Pomsky
//! needs, generating Rust types at build time via `prost-build`.
//!
//! Currently vendored (see `README.md` for provenance and update procedure):
//!
//! * `process` — process-agent `CollectorConnections` and dependencies (from
//!   `github.com/DataDog/agent-payload`).

// Silence lints from generated code only. Matches the pattern used by
// quickwit-proto. Specific lints rather than `clippy::all` so any new
// clippy check that starts firing on the generated output is explicit.
#![allow(clippy::disallowed_methods)] // prost-generated code uses Option::map_or

/// DD Agent process-agent payload types (CollectorConnections and friends).
pub mod process {
    include!(concat!(env!("OUT_DIR"), "/datadog.process_agent.rs"));
}
