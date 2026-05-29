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

//! Shared invariant definitions — the single source of truth.
//!
//! This module contains pure-Rust functions and types that express the
//! invariants verified across all layers of the verification pyramid:
//! TLA+ specs, stateright models, DST tests, and production code.
//!
//! No external dependencies — only `std`.

mod check;
pub mod merge_pipeline;
pub mod merge_policy;
pub mod recorder;
pub mod registry;
pub mod sort;
pub mod window;

pub use recorder::{record_invariant_check, set_invariant_recorder};
pub use registry::InvariantId;
