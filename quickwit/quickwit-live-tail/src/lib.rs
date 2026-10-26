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

#![deny(clippy::disallowed_methods)]

//! Live tail of the documents persisted by the ingesters.
//!
//! - The [`LiveTailHub`] receives the documents persisted by the ingester of this node, right
//!   after they are written to its write-ahead log, and fans them out to the live tail sessions.
//! - The [`LiveTailServiceImpl`] opens the live tail sessions, which evaluate a query on the
//!   documents with the exact same semantics as a search and stream the matching ones.

mod filter;
mod hub;
mod metrics;
mod rate_limiter;
mod service;
mod session;

pub use hub::{LiveTailHub, TailBatch, TailDoc};
pub use service::LiveTailServiceImpl;
