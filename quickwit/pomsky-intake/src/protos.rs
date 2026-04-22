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

//! Vendored Datadog-defined protobuf types used by this crate.
//!
//! The `.proto` sources live under `proto/` (with provenance in
//! `proto/README.md`). `build.rs` runs `prost-build` to produce the Rust
//! types, which are checked into `src/codegen/` so they're visible to
//! reviewers and greppable locally. After editing any `.proto`, re-running
//! `cargo build -p pomsky-intake` regenerates `src/codegen/*.rs` in place;
//! commit the regenerated files alongside the `.proto` edit.
//!
//! Currently vendored:
//!
//! * [`process`] — process-agent `CollectorConnections` and dependencies (from
//!   `github.com/DataDog/agent-payload`).
//! * [`conn`] — `MessageHeader` envelope wrapping each agent payload over the wire (from
//!   `github.com/DataDog/dd-go/process/protobuf/conn`).
//! * [`sketch`] — `ddsketch_full` wire format used by the agent to embed DDSketch bin data inside
//!   `bytes` fields (from `github.com/DataDog/sketches-go`, vendored via Vector's copy).

// Silence lints from generated code only. Specific lints rather than
// `clippy::all` so any new clippy check that starts firing on the
// generated output is explicit.
#![allow(clippy::disallowed_methods)] // prost-generated code uses Option::map_or

/// DD Agent process-agent payload types (CollectorConnections and friends).
pub mod process {
    include!("codegen/datadog.process_agent.rs");
}

/// DD Agent message envelope: `MessageHeader` carried at the front of each
/// payload version (V1..=V8). Used by the `connections` source to drive
/// timestamp + encoding selection.
pub mod conn {
    include!("codegen/conn.rs");
}

/// `ddsketch_full` wire format embedded inside agent-payload `bytes`
/// fields (e.g. `HTTPStats.Latencies`).
pub mod sketch {
    include!("codegen/ddsketch_full.rs");
}
