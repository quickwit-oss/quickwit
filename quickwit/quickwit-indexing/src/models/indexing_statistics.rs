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

use std::sync::atomic::Ordering;

use crate::actors::{DocProcessorCounters, IndexerCounters, PublisherCounters, UploaderCounters};

/// A Struct that holds all statistical data about indexing
#[derive(Clone, Debug, Default)]
pub struct IndexingStatistics {
    /// Number of document processed (valid or not)
    pub num_docs: u64,
    /// Number of document parse error, or missing timestamps
    pub num_invalid_docs: u64,
    /// Number of created split
    pub num_local_splits: u64,
    /// Number of staged splits
    pub num_staged_splits: u64,
    /// Number of uploaded splits
    pub num_uploaded_splits: u64,
    /// Number of published splits
    pub num_published_splits: u64,
    /// Size in byte of document processed
    pub total_bytes_processed: u64,
    /// Size in bytes of resulting split
    pub total_size_splits: u64,
    /// Pipeline generation.
    pub generation: usize,
    /// Number of successive pipeline spawn attempts.
    pub num_spawn_attempts: usize,
}

impl IndexingStatistics {
    pub fn add_actor_counters(
        mut self,
        doc_processor_counters: &DocProcessorCounters,
        indexer_counters: &IndexerCounters,
        uploader_counters: &UploaderCounters,
        publisher_counters: &PublisherCounters,
    ) -> Self {
        self.num_docs += doc_processor_counters.num_processed_docs();
        self.num_invalid_docs += doc_processor_counters.num_invalid_docs();
        self.num_local_splits += indexer_counters.num_splits_emitted;
        self.total_bytes_processed += doc_processor_counters.overall_num_bytes;
        self.num_staged_splits += uploader_counters.num_staged_splits.load(Ordering::SeqCst);
        self.num_uploaded_splits += uploader_counters.num_uploaded_splits.load(Ordering::SeqCst);
        self.num_published_splits += publisher_counters.num_published_splits;
        self
    }

    pub fn set_num_spawn_attempts(mut self, num_spawn_attempts: usize) -> Self {
        self.num_spawn_attempts = num_spawn_attempts;
        self
    }

    pub fn set_generation(mut self, generation: usize) -> Self {
        self.generation = generation;
        self
    }
}
