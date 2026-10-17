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
use tantivy::query::BitSetDocSet;
use tantivy::{BitSet, DocId, DocSet, Score, SeekDangerResult, TERMINATED};

const BLOCK_LEN: usize = BitPacker4x::BLOCK_LEN;
const RAW_BLOCK: u8 = 255;

#[cfg(test)]
mod tests;

/// Compressed, sorted document IDs with independently addressable blocks.
///
/// This process-local format stores a u32 count, then (first ID, payload offset)
/// u32 pairs for each 128-document block, then the payloads. Offsets are relative
/// to the payload section. Each payload starts with its delta bit width; 255
/// denotes raw IDs. Full blocks use the indexed first ID as their delta base,
/// so gaps between blocks never affect their bit width. A partial final block
/// stores raw IDs.
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

    /// Compresses the document IDs in a bitset; run this on a CPU executor.
    pub fn from_bitset(hits: BitSet) -> Self {
        if hits.len() == 0 {
            return Self::empty();
        }
        let mut docs = BitSetDocSet::from(hits);
        let mut builder = HitSetBuilder::new();
        while docs.doc() != TERMINATED {
            builder.insert(docs.doc());
            docs.advance();
        }
        builder.build()
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

    fn block_first(&self, block: usize) -> u32 {
        self.read_u32(4 + block * 8)
    }

    fn load_block(&mut self, block: usize) {
        self.block = block;
        self.block_pos = 0;
        if block >= self.num_blocks() {
            self.block = self.num_blocks();
            self.current_block.fill(TERMINATED);
            return;
        }
        let offset = 4 + self.num_blocks() * 8 + self.read_u32(8 + block * 8) as usize;
        let num_bits = self.buffer[offset];
        let payload = &self.buffer[offset + 1..];
        let len = (self.size_hint() as usize - block * BLOCK_LEN).min(BLOCK_LEN);
        if num_bits == RAW_BLOCK {
            for (doc, bytes) in self.current_block[..len]
                .iter_mut()
                .zip(payload.chunks_exact(4))
            {
                *doc = u32::from_ne_bytes(bytes.try_into().unwrap());
            }
        } else {
            BitPacker4x::new().decompress_strictly_sorted(
                self.block_first(block).checked_sub(1),
                payload,
                &mut self.current_block,
                num_bits,
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
        let mut start = self.block + 1;
        let mut end = self.num_blocks();
        while start < end {
            let mid = start + (end - start) / 2;
            if self.block_first(mid) <= target {
                start = mid + 1;
            } else {
                end = mid;
            }
        }
        if start - 1 > self.block {
            self.load_block(start - 1);
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

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        if target == TERMINATED {
            return SeekDangerResult::SeekLowerBound(TERMINATED);
        }
        self.seek_block(target);
        match self.current_block[self.block_pos..].binary_search(&target) {
            Ok(position) => {
                self.block_pos += position;
                SeekDangerResult::Found
            }
            Err(position) => {
                SeekDangerResult::SeekLowerBound(self.current_block[self.block_pos + position])
            }
        }
    }

    #[inline(always)]
    fn doc(&self) -> DocId {
        self.current_block[self.block_pos]
    }

    fn size_hint(&self) -> u32 {
        self.read_u32(0)
    }

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
            self.payload.push(RAW_BLOCK);
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
