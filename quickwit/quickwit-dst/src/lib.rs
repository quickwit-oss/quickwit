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

//! Deterministic simulation testing and shared invariants for Quickwit.
//!
//! # Invariants (always available)
//!
//! The [`invariants`] module contains pure-Rust definitions shared across the
//! entire verification pyramid: TLA+ specs, stateright models, DST tests, and
//! production `debug_assert!` checks. Zero external dependencies.
//!
//! # Models (feature = "model-checking")
//!
//! The [`models`] module contains exhaustive model-checking models that mirror
//! the TLA+ specs in `docs/internals/specs/tla/`. Each model verifies the same
//! invariants as the corresponding TLA+ spec, but runs as a Rust test via
//! [stateright](https://docs.rs/stateright). Requires the `model-checking`
//! feature.
//!
//! ## Models
//!
//! - `models::sort_schema` — SS-1..SS-5 (ADR-002, `SortSchema.tla`)
//! - `models::time_windowed_compaction` — TW-1..TW-3, CS-1..CS-3, MC-1..MC-4 (ADR-003,
//!   `TimeWindowedCompaction.tla`)
//! - `models::parquet_data_model` — DM-1..DM-5 (ADR-001, `ParquetDataModel.tla`)
//! - `models::merge_pipeline` — MP-1, MP-4..MP-11 (`MergePipelineShutdown.tla`); shares state and
//!   predicates literally with [`invariants::merge_pipeline`].

pub mod events;
pub mod invariants;

#[cfg(feature = "model-checking")]
pub mod models;
