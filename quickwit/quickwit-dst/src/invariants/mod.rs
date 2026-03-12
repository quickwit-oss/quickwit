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

//! Shared invariant definitions — the single source of truth.
//!
//! This module contains pure-Rust functions and types that express the
//! invariants verified across all layers of the verification pyramid:
//! TLA+ specs, stateright models, DST tests, and production code.
//!
//! No external dependencies — only `std`.

mod check;
pub mod registry;
pub mod sort;
pub mod window;

pub use registry::InvariantId;
