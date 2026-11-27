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

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::BundleFileRanges;

/// The type of index data a downloaded split read belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SplitComponent {
    /// Term dictionaries (`.term`).
    TermDict = 0,
    /// Posting lists (`.idx`).
    Postings = 1,
    /// Term positions (`.pos`).
    Positions = 2,
    /// Fast fields / columnar data (`.fast`).
    FastFields = 3,
    /// Field norms (`.fieldnorm`).
    FieldNorms = 4,
    /// Split footer: bundle metadata, hotcache and trailer.
    Footer = 5,
    /// Everything else. Expected to stay at zero on the leaf search path.
    Other = 6,
}

impl SplitComponent {
    /// Number of variants.
    pub const COUNT: usize = 7;

    /// Classifies a bundled file from its tantivy component extension.
    pub fn from_bundle_file(path: &Path) -> SplitComponent {
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("term") => SplitComponent::TermDict,
            Some("idx") => SplitComponent::Postings,
            Some("pos") => SplitComponent::Positions,
            Some("fast") => SplitComponent::FastFields,
            Some("fieldnorm") => SplitComponent::FieldNorms,
            _ => SplitComponent::Other,
        }
    }
}

/// Maps byte ranges of a single `.split` object to [`SplitComponent`]s.
///
/// The footer range is part of the split metadata, so it is known before the split is opened. Body
/// ranges become available once [`SplitComponentRanges::set_bundle_file_ranges`] is called with the
/// parsed bundle metadata.
#[derive(Debug)]
pub(crate) struct SplitComponentRanges {
    split_file: PathBuf,
    footer_range: Range<u64>,
    bundle_ranges: OnceLock<Vec<(Range<u64>, SplitComponent)>>,
}

impl SplitComponentRanges {
    pub fn new(split_file: PathBuf, footer_range: Range<u64>) -> Self {
        Self {
            split_file,
            footer_range,
            bundle_ranges: OnceLock::new(),
        }
    }

    /// Registers the bundle_ranges and sorts by start offset.
    pub fn set_bundle_file_ranges(&self, bundle_file_ranges: &BundleFileRanges) {
        self.bundle_ranges.get_or_init(|| {
            let mut bundle_ranges: Vec<(Range<u64>, SplitComponent)> = bundle_file_ranges
                .files
                .iter()
                .map(|(path, range)| (range.clone(), SplitComponent::from_bundle_file(path)))
                .collect();
            bundle_ranges.sort_by_key(|(range, _)| range.start);
            bundle_ranges
        });
    }

    /// Classifies a read of `num_bytes` bytes at offset `start` of `path`.
    ///
    /// A read is attributed as a whole to the single range that contains it. On the leaf search
    /// path every read reaching the counting layer stems from exactly one bundled file read, so
    /// reads spanning several ranges are not expected and are reported as
    /// [`SplitComponent::Other`].
    pub fn classify(&self, path: &Path, start: u64, num_bytes: u64) -> SplitComponent {
        if path != self.split_file {
            return SplitComponent::Other;
        }
        let end = start + num_bytes;
        if self.footer_range.start <= start && end <= self.footer_range.end {
            return SplitComponent::Footer;
        }
        let Some(bundle_ranges) = self.bundle_ranges.get() else {
            return SplitComponent::Other;
        };
        // Index of the last range starting at or before `start`.
        let num_ranges_starting_before =
            bundle_ranges.partition_point(|(range, _)| range.start <= start);
        let Some(candidate_idx) = num_ranges_starting_before.checked_sub(1) else {
            return SplitComponent::Other;
        };
        let (range, component) = &bundle_ranges[candidate_idx];
        if end <= range.end {
            *component
        } else {
            SplitComponent::Other
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_component_ranges_classify() {
        let ranges = SplitComponentRanges::new(PathBuf::from("split.split"), 100..150);
        let split_path = Path::new("split.split");
        assert_eq!(ranges.classify(split_path, 100, 50), SplitComponent::Footer);
        assert_eq!(ranges.classify(split_path, 0, 5), SplitComponent::Other);

        let files = [
            ("seg.term", 0..10),
            ("seg.idx", 10..30),
            ("seg.pos", 30..40),
            ("seg.fast", 40..60),
            ("seg.fieldnorm", 60..65),
            ("seg.store", 65..80),
            ("meta.json", 80..90),
        ]
        .into_iter()
        .map(|(path, range)| (PathBuf::from(path), range))
        .collect();
        ranges.set_bundle_file_ranges(&BundleFileRanges { files });

        for (start, num_bytes, expected) in [
            (0, 10, SplitComponent::TermDict),
            (10, 20, SplitComponent::Postings),
            (35, 5, SplitComponent::Positions),
            (40, 1, SplitComponent::FastFields),
            (60, 5, SplitComponent::FieldNorms),
            (65, 15, SplitComponent::Other),
            (80, 10, SplitComponent::Other),
            (120, 10, SplitComponent::Footer),
            // Spans two files.
            (5, 10, SplitComponent::Other),
            // Gap between the bundle body and the footer.
            (92, 4, SplitComponent::Other),
            // Past the end of the footer.
            (140, 20, SplitComponent::Other),
        ] {
            assert_eq!(
                ranges.classify(split_path, start, num_bytes),
                expected,
                "read {start}+{num_bytes}"
            );
        }
        assert_eq!(
            ranges.classify(Path::new("other.split"), 0, 10),
            SplitComponent::Other
        );
    }
}
