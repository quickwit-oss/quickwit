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

use proptest::prelude::*;

use super::*;

fn build(docs: &[u32]) -> HitSet {
    let mut builder = HitSetBuilder::new();
    for &doc in docs {
        builder.insert(doc);
    }
    builder.build()
}

fn check(docs: &[u32], targets: &[u32]) {
    let hits = build(docs);
    let mut scan = HitSet::from_buffer(hits.clone().into_buffer());
    assert_eq!(scan.size_hint() as usize, docs.len());
    for &doc in docs {
        assert_eq!(scan.doc(), doc);
        scan.advance();
    }
    for _ in 0..BLOCK_LEN * 2 {
        assert_eq!(scan.doc(), TERMINATED);
        assert_eq!(scan.advance(), TERMINATED);
    }
    assert_eq!(scan.count_including_deleted() as usize, docs.len());
    let mut sought = hits.clone();
    let mut dangerous = hits;
    for &target in targets {
        let position = docs.partition_point(|&doc| doc < target);
        let expected = docs.get(position).copied().unwrap_or(TERMINATED);
        let result = dangerous.seek_danger(target);
        if expected == target && target != TERMINATED {
            assert_eq!(result, SeekDangerResult::Found);
            assert_eq!(dangerous.doc(), target);
            assert_eq!(
                dangerous.clone().advance(),
                docs.get(position + 1).copied().unwrap_or(TERMINATED)
            );
        } else {
            assert_eq!(result, SeekDangerResult::SeekLowerBound(expected));
        }
        let target = target.max(sought.doc());
        assert_eq!(
            sought.seek(target),
            docs.iter()
                .copied()
                .find(|&doc| doc >= target)
                .unwrap_or(TERMINATED)
        );
    }
}

#[test]
fn test_boundaries() {
    for count in [0, 1, 31, 32, 127, 128, 129, 255, 256, 257, 8193] {
        for start in [0, TERMINATED - 4 * 8193] {
            let docs: Vec<_> = (0..count).map(|doc| start + doc * 3).collect();
            let mut targets: Vec<_> = (0..count * 3 + 2).map(|doc| start + doc).collect();
            targets.push(TERMINATED);
            check(&docs, &targets);
        }
    }
}

#[test]
fn test_seek_jumps_to_block() {
    let docs: Vec<_> = (0..128 * 1024).map(|doc| doc * 10).collect();
    let mut hits = build(&docs);
    assert_eq!(hits.seek(docs[128 * 1000] + 1), docs[128 * 1000 + 1]);
    assert_eq!(hits.block, 1000);
    assert_eq!(hits.advance(), docs[128 * 1000 + 2]);
    assert_eq!(hits.seek(docs[128 * 1023]), docs[128 * 1023]);
    assert_eq!(hits.block, 1023);
    assert_eq!(hits.seek(TERMINATED), TERMINATED);
    assert_eq!(hits.advance(), TERMINATED);
}

#[test]
fn test_compression() {
    let dense = build(&(0..128 * 100).collect::<Vec<_>>());
    assert_eq!(dense.buffer.len(), 4 + 100 * (8 + 1));
    let sparse = build(&(0..128 * 100).map(|doc| doc * 1000).collect::<Vec<_>>());
    assert!(sparse.buffer.len() < 128 * 100 * 2);
    // Partial blocks store only their actual IDs.
    let singleton = build(&[TERMINATED - 1]);
    assert_eq!(singleton.buffer.len(), 4 + 8 + 1 + 4);
    check(
        &[0, TERMINATED - 1],
        &[0, 1, TERMINATED - 2, TERMINATED - 1, TERMINATED],
    );
}

#[test]
fn test_leading_gap_does_not_increase_bit_width() {
    let docs: Vec<_> = (0..BLOCK_LEN as u32)
        .chain(100_000..100_000 + BLOCK_LEN as u32)
        .chain(TERMINATED - BLOCK_LEN as u32..TERMINATED)
        .collect();
    let hits = build(&docs);
    // Each consecutive block has zero-bit deltas, regardless of its first ID.
    assert_eq!(hits.buffer.len(), 4 + 3 * (8 + 1));
    check(
        &docs,
        &[
            0,
            127,
            128,
            99_999,
            100_000,
            100_001,
            100_128,
            TERMINATED - 1,
            TERMINATED,
        ],
    );
    let high = build(&docs[BLOCK_LEN * 2..]);
    assert_eq!(high.buffer.len(), 4 + 8 + 1);
    check(
        &docs[BLOCK_LEN * 2..],
        &[0, TERMINATED - 128, TERMINATED - 1, TERMINATED],
    );
}

#[test]
fn test_selective_intersection() {
    use tantivy::collector::Count;
    use tantivy::query::{BooleanQuery, Query};
    use tantivy::{Index, TantivyDocument};

    use super::super::{CacheEntry, CacheHitQuery};

    let index = Index::create_in_ram(tantivy::schema::Schema::builder().build());
    let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
    for _ in 0..8193 {
        writer.add_document(TantivyDocument::default()).unwrap();
    }
    writer.commit().unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let segment_id = searcher.segment_reader(0).segment_id();
    let query = |docs: Vec<u32>| -> Box<dyn Query> {
        Box::new(CacheHitQuery {
            cache_entry: CacheEntry {
                segment_id,
                hits: build(&docs),
            },
        })
    };
    let intersection = BooleanQuery::intersection(vec![
        query((0..8193).step_by(2).collect()),
        query(vec![8001, 8002, 8191, 8192]),
    ]);
    assert_eq!(searcher.search(&intersection, &Count).unwrap(), 2);
}

#[test]
fn test_seek_danger_miss_leaves_cursor() {
    let mut hits = build(&[1, 4, 7]);
    assert_eq!(hits.seek_danger(2), SeekDangerResult::SeekLowerBound(4));
    assert_eq!(hits.block_pos, 0);
    assert_eq!(hits.seek_danger(4), SeekDangerResult::Found);
    assert_eq!(hits.advance(), 7);
}

proptest! {
    #[test]
    fn test_roundtrip_and_seek(
        docs in prop::collection::btree_set(0..TERMINATED, 0..2048),
        targets in prop::collection::btree_set(0..=TERMINATED, 0..256),
    ) {
        let docs: Vec<_> = docs.into_iter().collect();
        let targets: Vec<_> = targets.into_iter().collect();
        check(&docs, &targets);
    }
}
