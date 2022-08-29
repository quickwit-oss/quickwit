// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::HashMap;
use std::ops::Range;

#[derive(Clone, Copy, Debug)]
struct RecordMeta {
    start_offset: usize,
    position: u64,
}

#[derive(Default)]
struct MemQueue {
    // Concatenated records
    concatenated_records: Vec<u8>,
    record_metas: Vec<RecordMeta>,
}

impl MemQueue {
    fn first_retained_position(&self) -> Option<u64> {
        Some(self.record_metas.first()?.position)
    }

    fn add_record(&mut self, position: u64, payload: &[u8]) {
        let record_meta = RecordMeta {
            start_offset: self.concatenated_records.len(),
            position,
        };
        self.record_metas.push(record_meta);
        self.concatenated_records.extend_from_slice(payload);
    }

    fn position_to_idx(&self, position: u64) -> Result<usize, usize> {
        self.record_metas
            .binary_search_by_key(&position, |record_meta| record_meta.position)
    }

    /// Returns the first record with position greater of equal to position.
    fn get_after(&self, position: u64) -> Option<(u64, &[u8])> {
        let idx = self
            .position_to_idx(position)
            .unwrap_or_else(|first_element_after| first_element_after);
        if idx > self.record_metas.len() {
            return None;
        }
        let record_meta = self.record_metas.get(idx)?;
        let start_offset = record_meta.start_offset;
        if let Some(next_record_meta) = self.record_metas.get(idx + 1) {
            let end_offset = next_record_meta.start_offset;
            Some((
                record_meta.position,
                &self.concatenated_records[start_offset..end_offset],
            ))
        } else {
            Some((
                record_meta.position,
                &self.concatenated_records[start_offset..],
            ))
        }
    }

    /// Removes all records coming before position,
    /// and including the record at "position".
    ///
    /// If a truncation occurs,
    /// this function returns the previous lowest position held.
    fn truncate(&mut self, truncate_up_to_pos: u64) -> Option<u64> {
        if self.record_metas[0].position > truncate_up_to_pos {
            return None;
        }
        let first_position = self.record_metas.first()?.position;
        if first_position > truncate_up_to_pos {
            return None;
        }
        let first_idx_to_keep = self
            .position_to_idx(truncate_up_to_pos)
            .map(|idx| idx + 1)
            .unwrap_or_else(|op| op);
        let start_offset_to_keep: usize = self
            .record_metas
            .get(first_idx_to_keep)
            .map(|record_meta| record_meta.start_offset)
            .unwrap_or(self.concatenated_records.len());
        self.record_metas.drain(..first_idx_to_keep);
        for record_meta in &mut self.record_metas {
            record_meta.start_offset -= start_offset_to_keep;
        }
        self.concatenated_records.drain(..start_offset_to_keep);
        Some(first_position)
    }
}

#[derive(Default)]
pub struct MemQueues {
    queues: HashMap<String, MemQueue>,
    // Range of records currently being held in all queues.
    retained_range: Range<u64>,
}

impl MemQueues {
    fn get_or_create_queue(&mut self, queue_id: &str) -> &mut MemQueue {
        // TODO fix me... It seems we explicitly create queues today.
        //
        // We do not rely on `entry` in order to avoid
        // the allocation.
        if !self.queues.contains_key(queue_id) {
            self.queues.insert(queue_id.to_string(), Default::default());
        }
        self.queues.get_mut(queue_id).unwrap()
    }

    pub fn queue_exists(&self, queue: &str) -> bool {
        self.queues.contains_key(queue)
    }

    pub fn list_queues(&self) -> Vec<String> {
        self.queues
            .keys()
            .cloned()
            .collect()
    }

    pub fn create_queue(&mut self, queue: &str) -> bool {
        if self.queues.contains_key(queue) {
            return false;
        }
        self.queues.insert(queue.to_string(), Default::default());
        true
    }

    pub fn remove_queue(&mut self, queue: &str) -> bool {
        self.queues.remove_entry(queue).is_some()
    }

    /// Appends a new record.
    ///
    /// # Panics
    ///
    /// Panics if the new position is not greater than the last seen position.
    pub fn add_record(&mut self, queue_id: &str, position: u64, record: &[u8]) {
        assert!(position >= self.retained_range.end);
        self.retained_range.end = position;
        self.get_or_create_queue(queue_id)
            .add_record(position, record);
    }

    /// Returns the first record with position greater of equal to position.
    pub fn get_after(&self, queue_id: &str, after_position: u64) -> Option<(u64, &[u8])> {
        let (position, payload) = self.queues.get(queue_id)?.get_after(after_position)?;
        assert!(position >= after_position);
        Some((position, payload))
    }

    /// Removes records up to the supplied `position`,
    /// including the position itself.
    ///
    /// If the queue `queue_id` does not exist, it
    /// will be created, and the first record appended will be `position + 1`.
    ///
    /// If there are no records `<= position`, the method will
    /// not do anything.
    ///
    /// Returns a position up to which including it is safe to truncate files as well.
    pub fn truncate(&mut self, queue_id: &str, position: u64) -> Option<u64> {
        let previous_lowest_retained_position: u64 =
            self.get_or_create_queue(queue_id).truncate(position)?;
        // Optimization here.
        if self.retained_range.start != previous_lowest_retained_position {
            return None;
        }
        if let Some(new_lowest) = self
            .queues
            .values_mut()
            .flat_map(|queue| queue.first_retained_position())
            .min()
        {
            self.retained_range.start = new_lowest;
        } else {
            self.retained_range.start = self.retained_range.end;
        }
        if self.retained_range.start > 0 {
            Some(self.retained_range.start - 1)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_queues() {
        let mut mem_queues = MemQueues::default();
        mem_queues.add_record("droopy", 0, b"hello");
        mem_queues.add_record("droopy", 1, b"happy");
        mem_queues.add_record("fable", 2, b"maitre");
        mem_queues.add_record("fable", 3, b"corbeau");
        mem_queues.add_record("droopy", 4, b"tax");
        mem_queues.add_record("droopy", 5, b"payer");
        assert_eq!(mem_queues.get_after("droopy", 0), Some((0, &b"hello"[..])));
        assert_eq!(mem_queues.get_after("droopy", 1), Some((1, &b"happy"[..])));
        assert_eq!(mem_queues.get_after("droopy", 2), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 3), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 4), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 5), Some((5, &b"payer"[..])));
        assert_eq!(mem_queues.get_after("droopy", 6), None);
        assert_eq!(mem_queues.get_after("fable", 0), Some((2, &b"maitre"[..])));
        assert_eq!(mem_queues.get_after("fable", 1), Some((2, &b"maitre"[..])));
        assert_eq!(mem_queues.get_after("fable", 2), Some((2, &b"maitre"[..])));
        assert_eq!(mem_queues.get_after("fable", 3), Some((3, &b"corbeau"[..])));
        assert_eq!(mem_queues.get_after("fable", 4), None);
    }

    #[test]
    fn test_mem_queues_truncate() {
        let mut mem_queues = MemQueues::default();
        mem_queues.add_record("droopy", 0, b"hello");
        mem_queues.add_record("droopy", 1, b"happy");
        mem_queues.add_record("droopy", 4, b"tax");
        mem_queues.add_record("droopy", 5, b"payer");
        mem_queues.truncate("droopy", 3);
        assert_eq!(mem_queues.get_after("droopy", 0), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 1), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 2), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 3), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 4), Some((4, &b"tax"[..])));
        assert_eq!(mem_queues.get_after("droopy", 5), Some((5, &b"payer"[..])));
        assert_eq!(mem_queues.get_after("droopy", 6), None);
    }
}
