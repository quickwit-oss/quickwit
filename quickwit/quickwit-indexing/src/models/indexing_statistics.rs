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

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use quickwit_proto::indexing::PipelineMetrics;
use quickwit_proto::types::ShardId;
use serde::Serialize;

use crate::actors::{DocProcessorCounters, IndexerCounters, PublisherCounters, UploaderCounters};

/// A Struct that holds all statistical data about indexing
#[derive(Clone, Debug, Default, Serialize, utoipa::ToSchema)]
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
    /// Number of empty batches
    pub num_empty_splits: u64,
    /// Size in byte of document processed
    pub total_bytes_processed: u64,
    /// Size in bytes of resulting split
    pub total_size_splits: u64,
    /// Pipeline generation.
    pub generation: usize,
    /// Number of successive pipeline spawn attempts.
    pub num_spawn_attempts: usize,
    // Pipeline metrics.
    pub pipeline_metrics_opt: Option<PipelineMetrics>,
    // List of shard ids.
    #[schema(value_type = Vec<u64>)]
    pub shard_ids: BTreeSet<ShardId>,
    pub params_fingerprint: u64,
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
        self.total_bytes_processed += doc_processor_counters
            .num_bytes_total
            .load(Ordering::Relaxed);
        self.num_staged_splits += uploader_counters.num_staged_splits.load(Ordering::Relaxed);
        self.num_uploaded_splits += uploader_counters
            .num_uploaded_splits
            .load(Ordering::Relaxed);
        self.num_published_splits += publisher_counters.num_published_splits;
        self.num_empty_splits += publisher_counters.num_empty_splits;
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
