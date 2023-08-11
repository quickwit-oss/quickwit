// Copyright (C) 2023 Quickwit, Inc.
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
use std::sync::atomic::{AtomicUsize, Ordering};

use quickwit_proto::ingest::Shard;
use quickwit_proto::types::SourceId;
use quickwit_proto::IndexId;

/// A set of open shards for a given index and source.
#[derive(Debug, Default)]
pub(crate) struct ShardTableEntry {
    shards: Vec<Shard>,
    round_robbin_state: AtomicUsize,
}

impl ShardTableEntry {
    /// Creates a new entry and ensures that the shards are open and unique.
    ///
    /// A shard table entry may not be empty.
    pub fn new(mut shards: Vec<Shard>) -> Self {
        assert!(!shards.is_empty());
        shards.retain(|shard| shard.is_open());
        shards.sort_unstable_by_key(|shard| shard.shard_id);
        shards.dedup_by_key(|shard| shard.shard_id);
        Self {
            shards,
            round_robbin_state: AtomicUsize::default(),
        }
    }

    /// Returns a shard to send documents to using round robbin.
    pub fn next_round_robbin(&self) -> &Shard {
        let idx = self.round_robbin_state.fetch_add(1, Ordering::Relaxed);
        &self.shards[idx % self.shards.len()]
    }
}

/// A table of shard entries indexed by index UID and source ID.
#[derive(Debug, Default)]
pub(crate) struct ShardTable {
    table: HashMap<(IndexId, SourceId), ShardTableEntry>,
}

impl ShardTable {
    pub fn contains_entry(
        &self,
        index_id: impl Into<IndexId>,
        source_id: impl Into<SourceId>,
    ) -> bool {
        let key = (index_id.into(), source_id.into());
        self.table.contains_key(&key)
    }

    pub fn select_shard_with_round_robbin(
        &self,
        index_id: impl Into<IndexId>,
        source_id: impl Into<SourceId>,
    ) -> Option<&Shard> {
        let key = (index_id.into(), source_id.into());
        let shard_table_entry = self.table.get(&key)?;
        Some(shard_table_entry.next_round_robbin())
    }

    pub fn update_entry(
        &mut self,
        index_id: impl Into<IndexId>,
        source_id: impl Into<SourceId>,
        shards: Vec<Shard>,
    ) {
        let key = (index_id.into(), source_id.into());
        self.table.insert(key, ShardTableEntry::new(shards));
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.table.len()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;

    use super::*;

    #[test]
    fn test_shard_table() {
        let mut table = ShardTable::default();
        assert!(!table.contains_entry("test-index", "test-source"));

        table.update_entry(
            "test-index",
            "test-source",
            vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 0,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 1,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 0,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 2,
                    shard_state: ShardState::Closed as i32,
                    ..Default::default()
                },
            ],
        );
        assert!(table.contains_entry("test-index", "test-source"));

        let entry = table.find_entry("test-index", "test-source").unwrap();
        assert_eq!(entry.len(), 2);
        assert_eq!(entry.shards()[0].shard_id, 0);
        assert_eq!(entry.shards()[1].shard_id, 1);
    }
}
