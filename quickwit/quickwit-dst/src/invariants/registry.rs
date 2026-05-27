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
    /// SS-2: null values always sort after non-null (nulls last, regardless of direction)
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

    /// MP-1: all splits in a merge operation have the same num_merge_ops level
    MP1,
    /// MP-2: every merge operation has at least 2 input splits
    MP2,
    /// MP-3: all splits in a merge operation share the same compaction scope
    MP3,
    /// MP-4: total rows in published splits equals cumulative rows ever ingested
    /// (the strong "no data loss, no duplication" invariant — survives crash/restart)
    MP4,
    /// MP-5: every published split has merge_ops <= MaxMergeOps (bounded write amp)
    MP5,
    /// MP-6: every input of an in-flight merge is still in published_splits
    /// (inputs durable in metastore until publish step completes)
    MP6,
    /// MP-7: no split appears in two concurrent in-flight merges
    MP7,
    /// MP-8: no split is simultaneously visible to the planner AND locked in an
    /// in-flight merge (planner can't re-merge a locked split)
    MP8,
    /// MP-9: while planner is alive AND connected, every immature published split
    /// is reachable from cold_windows or in_flight_merges
    MP9,
    /// MP-10: orphan_outputs (uploaded but never published) and published_splits
    /// are disjoint — orphan blobs are object-store leaks, not durable data loss
    MP10,
    /// MP-11: post-Restart action property — every immature published split is
    /// in cold_windows after fetch_immature_splits re-seeds the planner
    MP11,
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

            Self::MP1 => "MP-1",
            Self::MP2 => "MP-2",
            Self::MP3 => "MP-3",
            Self::MP4 => "MP-4",
            Self::MP5 => "MP-5",
            Self::MP6 => "MP-6",
            Self::MP7 => "MP-7",
            Self::MP8 => "MP-8",
            Self::MP9 => "MP-9",
            Self::MP10 => "MP-10",
            Self::MP11 => "MP-11",
        }
    }

    /// Human-readable description of this invariant.
    pub fn description(self) -> &'static str {
        match self {
            Self::SS1 => "rows sorted by split schema",
            Self::SS2 => "nulls always sort after non-null",
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

            Self::MP1 => "merge op splits share num_merge_ops level",
            Self::MP2 => "merge op has at least 2 splits",
            Self::MP3 => "merge op splits share compaction scope",
            Self::MP4 => "total ingested rows preserved in published splits",
            Self::MP5 => "every published split has merge_ops <= MaxMergeOps",
            Self::MP6 => "in-flight merge inputs remain in published_splits",
            Self::MP7 => "no split in multiple concurrent in-flight merges",
            Self::MP8 => "planner and in-flight sets are disjoint",
            Self::MP9 => "every immature published split tracked when planner connected",
            Self::MP10 => "orphan_outputs disjoint from published_splits",
            Self::MP11 => "Restart re-seeds planner with all immature splits",
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
        assert_eq!(InvariantId::MP1.to_string(), "MP-1");
    }

    #[test]
    fn descriptions_non_empty() {
        let all = [
            InvariantId::SS1,
            InvariantId::SS2,
            InvariantId::SS3,
            InvariantId::SS4,
            InvariantId::SS5,
            InvariantId::TW1,
            InvariantId::TW2,
            InvariantId::TW3,
            InvariantId::CS1,
            InvariantId::CS2,
            InvariantId::CS3,
            InvariantId::MC1,
            InvariantId::MC2,
            InvariantId::MC3,
            InvariantId::MC4,
            InvariantId::DM1,
            InvariantId::DM2,
            InvariantId::DM3,
            InvariantId::DM4,
            InvariantId::DM5,
            InvariantId::MP1,
            InvariantId::MP2,
            InvariantId::MP3,
            InvariantId::MP4,
            InvariantId::MP5,
            InvariantId::MP6,
            InvariantId::MP7,
            InvariantId::MP8,
            InvariantId::MP9,
            InvariantId::MP10,
            InvariantId::MP11,
        ];
        for id in all {
            assert!(!id.description().is_empty(), "{} has empty description", id);
        }
    }
}
