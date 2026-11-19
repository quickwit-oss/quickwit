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

use bitpacking::{BitPacker, BitPacker4x};
use tantivy::directory::OwnedBytes;
use tantivy::{DocId, DocSet, Score, TERMINATED};

const BLOCK_LEN: usize = BitPacker4x::BLOCK_LEN;

/// Compressed, sorted document IDs with independently addressable blocks.
///
/// This process-local format stores a u32 count, then (first ID, payload offset)
/// u32 pairs for each 128-document block, then the payloads. Offsets are relative
/// to the payload section. Full block payloads start with their delta bit width
/// and use the indexed first ID as their delta base, so gaps between blocks never
/// affect their bit width. A partial final block stores raw IDs without a header.
/// Its length is inferred from the total document count.
#[derive(Debug, Clone)]
pub struct HitSet {
    buffer: OwnedBytes,
    block: usize,
    current_block: [u32; BLOCK_LEN],
    block_pos: usize,
    pub(super) boost: Score,
}

impl HitSet {
    pub fn empty() -> Self {
        Self::from_buffer(OwnedBytes::new(vec![0; 4]))
    }

    pub fn is_empty(&self) -> bool {
        self.size_hint() == 0
    }

    /// The buffer must have been produced by `HitSet::into_buffer`.
    pub fn from_buffer(buffer: OwnedBytes) -> Self {
        let mut hits = Self {
            buffer,
            block: 0,
            current_block: [TERMINATED; BLOCK_LEN],
            block_pos: 0,
            boost: 1.0,
        };
        hits.load_block(0);
        hits
    }

    /// Returns the serialized hitset, without preserving the iterator position.
    pub fn into_buffer(self) -> OwnedBytes {
        self.buffer
    }

    fn read_u32(&self, offset: usize) -> u32 {
        u32::from_ne_bytes(self.buffer[offset..offset + 4].try_into().unwrap())
    }

    fn num_blocks(&self) -> usize {
        (self.size_hint() as usize).div_ceil(BLOCK_LEN)
    }

    fn block_index(&self) -> &[[u8; 8]] {
        self.buffer[4..4 + self.num_blocks() * 8].as_chunks::<8>().0
    }

    fn first_doc_in_block(entry: &[u8; 8]) -> DocId {
        u32::from_ne_bytes(entry[..4].try_into().unwrap())
    }

    fn block_payload_offset(&self, block: usize) -> usize {
        let entry = &self.block_index()[block];
        let relative_offset = u32::from_ne_bytes(entry[4..].try_into().unwrap()) as usize;
        4 + self.num_blocks() * 8 + relative_offset
    }

    fn load_block(&mut self, block: usize) {
        self.block = block;
        self.block_pos = 0;
        if block >= self.num_blocks() {
            self.block = self.num_blocks();
            self.current_block.fill(TERMINATED);
            return;
        }
        let offset = self.block_payload_offset(block);
        let payload = &self.buffer[offset..];
        let len = (self.size_hint() as usize - block * BLOCK_LEN).min(BLOCK_LEN);
        if len < BLOCK_LEN {
            for (doc, bytes) in self.current_block[..len]
                .iter_mut()
                .zip(payload.as_chunks::<4>().0)
            {
                *doc = u32::from_ne_bytes(*bytes);
            }
        } else {
            BitPacker4x::new().decompress_strictly_sorted(
                Self::first_doc_in_block(&self.block_index()[block]).checked_sub(1),
                &payload[1..],
                &mut self.current_block,
                payload[0],
            );
        }
        self.current_block[len..].fill(TERMINATED);
    }

    /// Decode the block starting at or before the target. If it ends before the
    /// target, decode the next block, whose first ID is already a lower bound.
    fn seek_block(&mut self, target: DocId) {
        if self.current_block[BLOCK_LEN - 1] >= target {
            return;
        }
        let block = self.block
            + self.block_index()[self.block + 1..]
                .partition_point(|entry| Self::first_doc_in_block(entry) <= target);
        if block > self.block {
            self.load_block(block);
        }
        if self.current_block[BLOCK_LEN - 1] < target {
            self.load_block(self.block + 1);
        }
    }
}

impl DocSet for HitSet {
    #[inline(always)]
    fn advance(&mut self) -> DocId {
        self.block_pos += 1;
        if let Some(&doc) = self.current_block.get(self.block_pos) {
            return doc;
        }
        self.load_block(self.block + 1);
        self.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(self.doc() <= target);
        self.seek_block(target);
        self.block_pos += self.current_block[self.block_pos..].partition_point(|&doc| doc < target);
        self.doc()
    }

    #[inline(always)]
    fn doc(&self) -> DocId {
        self.current_block[self.block_pos]
    }

    fn size_hint(&self) -> u32 {
        self.read_u32(0)
    }

    // Weight::count uses a fresh scorer. Return the stored total without consuming it.
    fn count_including_deleted(&mut self) -> u32 {
        self.size_hint()
    }
}

impl tantivy::query::Scorer for HitSet {
    fn score(&mut self) -> Score {
        self.boost
    }
}

pub(super) struct HitSetBuilder {
    count: u32,
    current_block: [u32; BLOCK_LEN],
    index: Vec<(u32, u32)>,
    payload: Vec<u8>,
}

impl HitSetBuilder {
    pub(super) fn new() -> Self {
        Self {
            count: 0,
            current_block: [0; BLOCK_LEN],
            index: Vec::new(),
            payload: Vec::new(),
        }
    }

    fn flush_block(&mut self, len: usize) {
        let first_doc = self.current_block[0];
        self.index.push((
            first_doc,
            u32::try_from(self.payload.len()).expect("hitset exceeds 4 GiB"),
        ));
        if len < BLOCK_LEN {
            for doc in &self.current_block[..len] {
                self.payload.extend_from_slice(&doc.to_ne_bytes());
            }
        } else {
            // Strict deltas subtract one from each gap. This base makes the
            // first gap zero, including when the first ID itself is zero.
            let base = first_doc.checked_sub(1);
            let packer = BitPacker4x::new();
            let num_bits = packer.num_bits_strictly_sorted(base, &self.current_block);
            self.payload.push(num_bits);
            let offset = self.payload.len();
            self.payload
                .resize(offset + BLOCK_LEN * num_bits as usize / 8, 0);
            packer.compress_strictly_sorted(
                base,
                &self.current_block,
                &mut self.payload[offset..],
                num_bits,
            );
        }
    }

    /// IDs must be strictly increasing and must not include TERMINATED.
    pub(super) fn insert(&mut self, doc: DocId) {
        let position = self.count as usize % BLOCK_LEN;
        debug_assert!(doc != TERMINATED);
        debug_assert!(
            self.count == 0
                || doc
                    > if position == 0 {
                        self.current_block[BLOCK_LEN - 1]
                    } else {
                        self.current_block[position - 1]
                    }
        );
        self.current_block[position] = doc;
        self.count += 1;
        if position + 1 == BLOCK_LEN {
            self.flush_block(BLOCK_LEN);
        }
    }

    pub(super) fn build(mut self) -> HitSet {
        let tail_len = self.count as usize % BLOCK_LEN;
        if tail_len != 0 {
            self.flush_block(tail_len);
        }
        let mut buffer = Vec::with_capacity(4 + self.index.len() * 8 + self.payload.len());
        buffer.extend_from_slice(&self.count.to_ne_bytes());
        for (first_doc, offset) in self.index {
            buffer.extend_from_slice(&first_doc.to_ne_bytes());
            buffer.extend_from_slice(&offset.to_ne_bytes());
        }
        buffer.extend_from_slice(&self.payload);
        HitSet::from_buffer(OwnedBytes::new(buffer))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tantivy::SeekDangerResult;

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
        let first_doc = scan.doc();
        assert_eq!(scan.count_including_deleted() as usize, docs.len());
        assert_eq!(scan.doc(), first_doc);
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
        assert_eq!(singleton.buffer.len(), 4 + 8 + 4);
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
}
