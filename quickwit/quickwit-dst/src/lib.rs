// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Deterministic simulation testing and shared invariants for Quickhouse-Pomsky.
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
//! - `models::time_windowed_compaction` — TW-1..TW-3, CS-1..CS-3, MC-1..MC-4
//!   (ADR-003, `TimeWindowedCompaction.tla`)
//! - `models::parquet_data_model` — DM-1..DM-5 (ADR-001,
//!   `ParquetDataModel.tla`)

pub mod invariants;

#[cfg(feature = "model-checking")]
pub mod models;
