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

//! Invariant ID catalog — single source of truth for all invariant definitions.
//!
//! Each variant corresponds to a named invariant verified across the TLA+ specs,
//! stateright models, and production code. See `docs/internals/specs/tla/` for
//! the formal definitions.

use std::fmt;

/// Unique identifier for each verified invariant.
///
/// The naming convention is `<SPEC_PREFIX><NUMBER>`:
/// - SS: SortSchema.tla (ADR-002)
/// - TW: TimeWindowedCompaction.tla time-window invariants (ADR-003)
/// - CS: TimeWindowedCompaction.tla compaction-scope invariants (ADR-003)
/// - MC: TimeWindowedCompaction.tla merge-correctness invariants (ADR-003)
/// - DM: ParquetDataModel.tla (ADR-001)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InvariantId {
    /// SS-1: all rows within a split are sorted according to the split's schema
    SS1,
    /// SS-2: null values sort correctly per direction (nulls last asc, first desc)
    SS2,
    /// SS-3: missing sort columns are treated as NULL
    SS3,
    /// SS-4: a split's sort schema never changes after write
    SS4,
    /// SS-5: three copies of sort schema are identical per split
    SS5,

    /// TW-1: every split belongs to exactly one time window
    TW1,
    /// TW-2: window_duration evenly divides one hour (3600 seconds)
    TW2,
    /// TW-3: data is never merged across window boundaries
    TW3,

    /// CS-1: only splits sharing all six scope components may be merged
    CS1,
    /// CS-2: within a scope, only same window_start splits merge
    CS2,
    /// CS-3: splits before compaction_start_time are never compacted
    CS3,

    /// MC-1: row multiset preserved through compaction (no add/remove/duplicate)
    MC1,
    /// MC-2: row contents unchanged through compaction
    MC2,
    /// MC-3: output is sorted according to sort schema
    MC3,
    /// MC-4: column set is the union of input column sets
    MC4,

    /// DM-1: each row has all required fields populated
    DM1,
    /// DM-2: no last-write-wins; duplicate ingests both survive
    DM2,
    /// DM-3: storage only contains ingested points (no interpolation)
    DM3,
    /// DM-4: same tags produce same timeseries_id (deterministic TSID)
    DM4,
    /// DM-5: timeseries_id persists through compaction without recomputation
    DM5,
}

impl InvariantId {
    /// Short identifier string (e.g. `"SS-1"`).
    ///
    /// Returns a `&'static str` to avoid allocation on the hot path.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SS1 => "SS-1",
            Self::SS2 => "SS-2",
            Self::SS3 => "SS-3",
            Self::SS4 => "SS-4",
            Self::SS5 => "SS-5",

            Self::TW1 => "TW-1",
            Self::TW2 => "TW-2",
            Self::TW3 => "TW-3",

            Self::CS1 => "CS-1",
            Self::CS2 => "CS-2",
            Self::CS3 => "CS-3",

            Self::MC1 => "MC-1",
            Self::MC2 => "MC-2",
            Self::MC3 => "MC-3",
            Self::MC4 => "MC-4",

            Self::DM1 => "DM-1",
            Self::DM2 => "DM-2",
            Self::DM3 => "DM-3",
            Self::DM4 => "DM-4",
            Self::DM5 => "DM-5",
        }
    }

    /// Human-readable description of this invariant.
    pub fn description(self) -> &'static str {
        match self {
            Self::SS1 => "rows sorted by split schema",
            Self::SS2 => "null ordering correct per direction",
            Self::SS3 => "missing sort columns treated as NULL",
            Self::SS4 => "sort schema immutable after write",
            Self::SS5 => "three copies of sort schema identical",

            Self::TW1 => "one window per split",
            Self::TW2 => "window_duration divides 3600",
            Self::TW3 => "no cross-window merge",

            Self::CS1 => "scope compatibility for merge",
            Self::CS2 => "same window_start for merge",
            Self::CS3 => "compaction start time respected",

            Self::MC1 => "row set preserved through compaction",
            Self::MC2 => "row contents unchanged through compaction",
            Self::MC3 => "sort order preserved after compaction",
            Self::MC4 => "column union after compaction",

            Self::DM1 => "point per row — all fields populated",
            Self::DM2 => "no last-write-wins",
            Self::DM3 => "no interpolation — only ingested points",
            Self::DM4 => "deterministic TSID from tags",
            Self::DM5 => "TSID persists through compaction",
        }
    }
}

impl fmt::Display for InvariantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_format() {
        assert_eq!(InvariantId::SS1.to_string(), "SS-1");
        assert_eq!(InvariantId::TW2.to_string(), "TW-2");
        assert_eq!(InvariantId::CS3.to_string(), "CS-3");
        assert_eq!(InvariantId::MC4.to_string(), "MC-4");
        assert_eq!(InvariantId::DM5.to_string(), "DM-5");
    }

    #[test]
    fn descriptions_non_empty() {
        let all = [
            InvariantId::SS1, InvariantId::SS2, InvariantId::SS3,
            InvariantId::SS4, InvariantId::SS5,
            InvariantId::TW1, InvariantId::TW2, InvariantId::TW3,
            InvariantId::CS1, InvariantId::CS2, InvariantId::CS3,
            InvariantId::MC1, InvariantId::MC2, InvariantId::MC3, InvariantId::MC4,
            InvariantId::DM1, InvariantId::DM2, InvariantId::DM3,
            InvariantId::DM4, InvariantId::DM5,
        ];
        for id in all {
            assert!(
                !id.description().is_empty(),
                "{} has empty description",
                id
            );
        }
    }
}
