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

use std::fmt;
use std::path::Path;

use quickwit_common::io::IoControls;
use quickwit_common::metrics::index_label;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_metrics::{GaugeGuard, label_values};
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::types::{DocMappingUid, IndexUid, SplitId};
use tantivy::IndexBuilder;
use tantivy::directory::{MmapDirectory, RamDirectory};
use tracing::{Span, error, instrument};

use crate::controlled_directory::ControlledDirectory;
use crate::docs_clustering::{ChunkedDocsClusterer, DocIdClusterer, Fingerprint};
use crate::merge_policy::MergeTask;
use crate::metrics::INDEX_SOURCE;
use crate::models::{PublishLock, SplitAttrs};

/// Buffered `TantivyDocument`s use about 1.6x their source JSON size (measured on OTel logs:
/// 4M buffered docs of 1.2 KB added 7.7 GB of RSS). Round up so the heap limit stays conservative.
const PENDING_DOC_MEM_FACTOR: usize = 2;

/// How an [`IndexedSplitBuilder`] orders documents for docs clustering.
pub enum SplitClustering {
    /// No clustering: documents are indexed in arrival order.
    Disabled,
    /// Split-wide clustering: fingerprints are recorded per doc id and the segment is rewritten
    /// in clustered order at finalization (requires `IndexSettings::manual_doc_id_mapping`).
    ReorderAtFinalize(DocIdClusterer),
    /// Chunked clustering: documents are buffered and added to the index writer in clustered
    /// order, one chunk at a time. The segment is finalized without a doc id mapping.
    Chunked {
        clusterer: ChunkedDocsClusterer<tantivy::TantivyDocument>,
        /// Sum of the source sizes of the buffered documents. Their memory usage, not yet
        /// accounted for by tantivy, is estimated as `PENDING_DOC_MEM_FACTOR` times this.
        pending_num_bytes: usize,
    },
}

impl SplitClustering {
    pub fn chunked(chunk_num_docs: usize) -> Self {
        SplitClustering::Chunked {
            clusterer: ChunkedDocsClusterer::new(chunk_num_docs),
            pending_num_bytes: 0,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            SplitClustering::Disabled => "disabled",
            SplitClustering::ReorderAtFinalize(_) => "reorder_at_finalize",
            SplitClustering::Chunked { .. } => "chunked",
        }
    }
}

pub struct IndexedSplitBuilder {
    pub split_attrs: SplitAttrs,
    index_writer: tantivy::SingleSegmentIndexWriter,
    pub split_scratch_directory: TempDirectory,
    pub controlled_directory: ControlledDirectory,
    clustering: SplitClustering,
    // Number of documents added to `index_writer` (buffered documents excluded).
    num_docs_in_writer: u32,
    ram_directory_opt: Option<RamDirectory>,
}

pub struct IndexedSplit {
    pub split_attrs: SplitAttrs,
    pub index: tantivy::Index,
    pub split_scratch_directory: TempDirectory,
    pub controlled_directory: ControlledDirectory,
}

impl IndexedSplit {
    pub fn split_id(&self) -> &SplitId {
        &self.split_attrs.split_id
    }
}

impl fmt::Debug for IndexedSplit {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("IndexedSplit")
            .field("split_id", &self.split_attrs.split_id)
            .field("dir", &self.split_scratch_directory.path())
            .field("num_docs", &self.split_attrs.num_docs)
            .finish()
    }
}

impl fmt::Debug for IndexedSplitBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("IndexedSplitBuilder")
            .field("split_id", &self.split_attrs.split_id)
            .field("dir", &self.split_scratch_directory.path())
            .field("num_docs", &self.split_attrs.num_docs)
            .field("clustering", &self.clustering.name())
            .finish()
    }
}

impl IndexedSplitBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new_in_dir(
        pipeline_id: IndexingPipelineId,
        partition_id: u64,
        last_delete_opstamp: u64,
        doc_mapping_uid: DocMappingUid,
        scratch_directory: TempDirectory,
        index_builder: IndexBuilder,
        io_controls: IoControls,
        clustering: SplitClustering,
    ) -> anyhow::Result<Self> {
        // We avoid intermediary merge, and instead merge all segments in the packager.
        // The benefit is that we don't have to wait for potentially existing merges,
        // and avoid possible race conditions.
        let split_id = SplitId::new();
        let split_scratch_directory_prefix = format!("split-{split_id}-");
        let split_scratch_directory =
            scratch_directory.named_temp_child(&split_scratch_directory_prefix)?;
        let use_ram_directory =
            quickwit_common::get_bool_from_env_cached!("QW_ENABLE_IN_MEMORY_INDEXING", false);
        let ram_directory_opt = use_ram_directory.then(RamDirectory::default);
        let mmap_directory = MmapDirectory::open(split_scratch_directory.path())?;
        let controlled_directory = ControlledDirectory::new(Box::new(mmap_directory), io_controls);
        let indexing_directory: Box<dyn tantivy::Directory> =
            if let Some(ram_directory) = &ram_directory_opt {
                Box::new(ram_directory.clone())
            } else {
                Box::new(controlled_directory.clone())
            };

        let index_writer =
            index_builder.single_segment_index_writer(indexing_directory, 15_000_000)?;
        Ok(Self {
            split_attrs: SplitAttrs {
                node_id: pipeline_id.node_id,
                index_uid: pipeline_id.index_uid,
                source_id: pipeline_id.source_id,
                doc_mapping_uid,
                partition_id,
                split_id,
                num_docs: 0,
                replaced_split_ids: Vec::new(),
                uncompressed_docs_size_in_bytes: 0,
                time_range: None,
                delete_opstamp: last_delete_opstamp,
                num_merge_ops: 0,
            },
            index_writer,
            clustering,
            num_docs_in_writer: 0,
            split_scratch_directory,
            controlled_directory,
            ram_directory_opt,
        })
    }

    /// Adds a document to the split. `split_attrs` (num docs, size, time range) is the caller's
    /// responsibility.
    ///
    /// In chunked clustering mode, the document may be buffered and only reach the index writer
    /// when the chunk is full or the split is finalized.
    pub fn add_document(
        &mut self,
        doc: tantivy::TantivyDocument,
        fingerprint_opt: Option<Fingerprint>,
        num_bytes: usize,
    ) -> anyhow::Result<()> {
        match &mut self.clustering {
            SplitClustering::Disabled => {
                self.index_writer.add_document(doc)?;
                self.num_docs_in_writer += 1;
            }
            SplitClustering::ReorderAtFinalize(doc_id_clusterer) => {
                // Tantivy doc IDs are local to the split and follow insertion order.
                doc_id_clusterer.push(fingerprint_opt, self.num_docs_in_writer);
                self.index_writer.add_document(doc)?;
                self.num_docs_in_writer += 1;
            }
            SplitClustering::Chunked {
                clusterer,
                pending_num_bytes,
            } => {
                *pending_num_bytes += num_bytes;
                if clusterer.push(fingerprint_opt, doc) {
                    self.flush_pending_docs()?;
                }
            }
        }
        Ok(())
    }

    /// Chunked clustering: adds the buffered documents to the index writer, in clustered order.
    pub fn flush_pending_docs(&mut self) -> anyhow::Result<()> {
        let SplitClustering::Chunked {
            clusterer,
            pending_num_bytes,
        } = &mut self.clustering
        else {
            return Ok(());
        };
        if clusterer.is_empty() {
            return Ok(());
        }
        for doc in clusterer.drain_in_cluster_order() {
            self.index_writer.add_document(doc)?;
            self.num_docs_in_writer += 1;
        }
        *pending_num_bytes = 0;
        Ok(())
    }

    #[instrument(name="serialize_split",
        skip_all,
        fields(
            node_id=%self.split_attrs.node_id,
            index_uid=%self.split_attrs.index_uid,
            source_id=%self.split_attrs.source_id,
            split_id=%self.split_attrs.split_id,
            partition_id=%self.split_attrs.partition_id,
            num_docs=%self.split_attrs.num_docs,
            uncompressed_docs_size_in_bytes=%self.split_attrs.uncompressed_docs_size_in_bytes,
            delete_opstamp=%self.split_attrs.delete_opstamp,
            num_merge_ops=%self.split_attrs.num_merge_ops,
        )
    )]
    pub fn finalize(mut self) -> anyhow::Result<IndexedSplit> {
        self.flush_pending_docs()?;
        let split_attrs = self.split_attrs;
        let index = if let SplitClustering::ReorderAtFinalize(doc_id_clusterer) = self.clustering {
            // Update metrics for document clustering.
            let index_label = index_label(&split_attrs.index_uid.index_id);
            let labels = label_values!(
                INDEX_SOURCE => index_label.to_string(),
                split_attrs.source_id.to_string()
            );
            doc_id_clusterer.observe_cluster_group_sizes(labels);

            // Finalize the index with the doc id mapping.
            let doc_id_mapping = doc_id_clusterer
                .into_doc_id_mapping()
                .inspect_err(|error| {
                    error!(?error, "failed to create doc id mapping");
                })?;
            self.index_writer
                .finalize_with_doc_id_mapping(&doc_id_mapping)?
        } else {
            self.index_writer.finalize()?
        };
        if let Some(ram_directory) = &self.ram_directory_opt {
            // The packager and uploader consume split files from the scratch directory.
            ram_directory.persist(&self.controlled_directory)?;
        }
        Ok(IndexedSplit {
            split_attrs,
            index,
            split_scratch_directory: self.split_scratch_directory,
            controlled_directory: self.controlled_directory,
        })
    }

    pub fn path(&self) -> &Path {
        self.split_scratch_directory.path()
    }

    pub fn mem_usage(&self) -> usize {
        let pending_num_bytes = match &self.clustering {
            SplitClustering::Chunked {
                pending_num_bytes, ..
            } => *pending_num_bytes * PENDING_DOC_MEM_FACTOR,
            _ => 0,
        };
        self.index_writer.mem_usage()
            + pending_num_bytes
            + self
                .ram_directory_opt
                .as_ref()
                .map(RamDirectory::total_mem_usage)
                .unwrap_or(0)
    }

    pub fn split_id(&self) -> &SplitId {
        &self.split_attrs.split_id
    }
}

#[derive(Debug)]
pub struct IndexedSplitBatch {
    pub splits: Vec<IndexedSplit>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    /// A [`MergeTask`] tracked by either the `MergePlanner` or the `DeleteTaskPlanner`
    /// in the `MergePipeline` or `DeleteTaskPipeline`.
    /// See planners docs to understand the usage.
    /// If `None`, the split batch was built in the `IndexingPipeline`.
    pub merge_task_opt: Option<MergeTask>,
    pub batch_parent_span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitTrigger {
    Drained,
    ForceCommit,
    MemoryLimit,
    NoMoreDocs,
    NumDocsLimit,
    Timeout,
}

#[derive(Debug)]
pub struct IndexedSplitBatchBuilder {
    pub splits: Vec<IndexedSplitBuilder>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub commit_trigger: CommitTrigger,
    pub batch_parent_span: Span,
    pub memory_usage: GaugeGuard,
    pub _split_builders_guard: GaugeGuard,
}

/// Sends notifications to the Publisher that the last batch of splits was empty.
#[derive(Debug)]
pub struct EmptySplit {
    pub index_uid: IndexUid,
    pub checkpoint_delta: IndexCheckpointDelta,
    pub publish_lock: PublishLock,
    pub batch_parent_span: Span,
}
