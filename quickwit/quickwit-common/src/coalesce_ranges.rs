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

const MAX_GAP: usize = 2 * 1024 * 1024;

/// Combines requested byte ranges into download ranges for the same object.
///
/// Merges overlaps and gaps up to 2 MiB. This limits each gap, not the total download size
/// or the total number of gap bytes downloaded.
/// Returns sorted, disjoint ranges and ignores empty ranges.
///
/// # Panics
///
/// Panics if a range's start exceeds its end.
pub fn coalesce_ranges(mut ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    if ranges.len() < 2 {
        ranges.retain(|range| {
            assert!(range.start <= range.end, "byte range starts after its end");
            !range.is_empty()
        });
        return ranges;
    }
    ranges.sort_unstable_by_key(|range| range.start);
    let mut downloads: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        assert!(range.start <= range.end, "byte range starts after its end");
        if range.is_empty() {
            continue;
        }
        if let Some(previous) = downloads.last_mut()
            && range.start.saturating_sub(previous.end) <= MAX_GAP
        {
            previous.end = previous.end.max(range.end);
        } else {
            downloads.push(range);
        }
    }
    downloads
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;

    #[test]
    fn test_coalesce_ranges_empty_single_overlap_and_order() {
        assert!(coalesce_ranges(Vec::new()).is_empty());
        let range = 10..20;
        assert_eq!(coalesce_ranges(vec![range.clone()]), vec![range]);
        let empty_range = 7..7;
        assert!(coalesce_ranges(vec![empty_range]).is_empty());
        assert!(coalesce_ranges(vec![7..7, 0..0]).is_empty());
        let ranges = vec![
            4 * MIB..5 * MIB,
            0..MIB,
            0..MIB,
            10..20,
            MIB..2 * MIB,
            2 * MIB + 512 * KIB..3 * MIB,
        ];
        assert_eq!(coalesce_ranges(ranges), vec![0..5 * MIB]);
    }

    #[test]
    fn test_coalesce_ranges_uses_fixed_gap_threshold() {
        let ranges = vec![0..20 * MIB, 22 * MIB..42 * MIB];
        assert_eq!(coalesce_ranges(ranges), vec![0..42 * MIB]);
        // Even large downloads cannot bridge a gap greater than 2 MiB.
        let ranges = vec![0..20 * MIB, 22 * MIB + 1..42 * MIB + 1];
        assert_eq!(coalesce_ranges(ranges.clone()), ranges);
    }

    #[test]
    fn test_coalesce_ranges_near_address_limit() {
        let ranges = vec![usize::MAX - 10..usize::MAX, 0..10];
        assert_eq!(
            coalesce_ranges(ranges),
            vec![0..10, usize::MAX - 10..usize::MAX]
        );
        assert_eq!(
            coalesce_ranges(vec![0..usize::MAX, 1..2]),
            vec![0..usize::MAX]
        );
    }

    #[derive(serde::Deserialize)]
    struct QueryRanges {
        query: String,
        ranges: Vec<[usize; 2]>,
    }

    struct ExpectedReadPlan {
        pattern: &'static str,
        range_count: usize,
        requested_bytes: usize,
        download_count: usize,
        download_bytes: usize,
    }

    #[test]
    fn test_coalesce_ranges_from_postings_inline_8() {
        let queries: Vec<QueryRanges> = serde_json::from_str(include_str!(
            "../test-data/coalescing/requested_ranges.json"
        ))
        .unwrap();

        // Reference plans independently calculated with a 2 MiB gap threshold.
        let expected = [
            ExpectedReadPlan {
                pattern: "*ab*",
                range_count: 203,
                requested_bytes: 288_249,
                download_count: 14,
                download_bytes: 24_776_998,
            },
            ExpectedReadPlan {
                pattern: "*aba*",
                range_count: 16,
                requested_bytes: 143_930,
                download_count: 4,
                download_bytes: 2_015_570,
            },
            ExpectedReadPlan {
                pattern: "*traaa*",
                range_count: 0,
                requested_bytes: 0,
                download_count: 0,
                download_bytes: 0,
            },
            ExpectedReadPlan {
                pattern: "traaa*",
                range_count: 0,
                requested_bytes: 0,
                download_count: 0,
                download_bytes: 0,
            },
            ExpectedReadPlan {
                pattern: "*player*",
                range_count: 21,
                requested_bytes: 4_221_761,
                download_count: 9,
                download_bytes: 7_309_330,
            },
            ExpectedReadPlan {
                pattern: "*ms*",
                range_count: 27,
                requested_bytes: 825_018,
                download_count: 13,
                download_bytes: 4_170_450,
            },
        ];
        assert_postings_read_plans(queries, expected);
    }

    #[test]
    fn test_coalesce_ranges_from_postings_no_inline() {
        let queries: Vec<QueryRanges> = serde_json::from_str(include_str!(
            "../test-data/coalescing/requested_ranges_no_inline.json"
        ))
        .unwrap();
        // Original postings offsets, keeping the first 16,384 matching lists per query.
        let expected = [
            ExpectedReadPlan {
                pattern: "*ab*",
                range_count: 16_384,
                requested_bytes: 82_276,
                download_count: 8,
                download_bytes: 92_019_280,
            },
            ExpectedReadPlan {
                pattern: "*aba*",
                range_count: 2_817,
                requested_bytes: 157_561,
                download_count: 17,
                download_bytes: 201_491_089,
            },
            ExpectedReadPlan {
                pattern: "*traaa*",
                range_count: 2,
                requested_bytes: 10,
                download_count: 2,
                download_bytes: 10,
            },
            ExpectedReadPlan {
                pattern: "traaa*",
                range_count: 0,
                requested_bytes: 0,
                download_count: 0,
                download_bytes: 0,
            },
            ExpectedReadPlan {
                pattern: "*player*",
                range_count: 21,
                requested_bytes: 4_221_761,
                download_count: 9,
                download_bytes: 7_751_160,
            },
            ExpectedReadPlan {
                pattern: "*ms*",
                range_count: 16_384,
                requested_bytes: 200_211,
                download_count: 8,
                download_bytes: 143_174_338,
            },
        ];
        assert_postings_read_plans(queries, expected);
    }

    fn assert_postings_read_plans(queries: Vec<QueryRanges>, expected: [ExpectedReadPlan; 6]) {
        assert_eq!(queries.len(), expected.len());
        for (query, expected) in queries.into_iter().zip(expected) {
            let ExpectedReadPlan {
                pattern,
                range_count,
                requested_bytes,
                download_count,
                download_bytes,
            } = expected;
            assert_eq!(query.query, pattern);
            let ranges: Vec<Range<usize>> = query
                .ranges
                .into_iter()
                .map(|[start, end]| start..end)
                .collect();
            assert_eq!(ranges.len(), range_count, "{pattern}");
            assert_eq!(
                ranges.iter().map(|range| range.len()).sum::<usize>(),
                requested_bytes,
                "{pattern}"
            );
            let downloads = coalesce_ranges(ranges.clone());
            let fetched_bytes: usize = downloads.iter().map(|range| range.len()).sum();
            println!(
                "{pattern}: {range_count} requested ranges, {requested_bytes} requested bytes -> \
                 {} downloads, {fetched_bytes} fetched bytes",
                downloads.len()
            );
            assert_eq!(downloads.len(), download_count, "{pattern}");
            assert_eq!(fetched_bytes, download_bytes, "{pattern}");
            assert!(downloads.windows(2).all(|pair| pair[0].end < pair[1].start));
            for range in ranges {
                assert!(downloads.iter().any(|download| {
                    download.start <= range.start && range.end <= download.end
                }));
            }
        }
    }

    #[test]
    #[should_panic(expected = "byte range starts after its end")]
    fn test_coalesce_ranges_rejects_reversed_range() {
        coalesce_ranges(vec![Range { start: 2, end: 1 }]);
    }
}
