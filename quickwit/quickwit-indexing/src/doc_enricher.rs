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

use std::sync::Arc;

use quickwit_doc_mapper::DocMapper;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::types::{DocMappingUid, SplitId};
use tantivy::TantivyDocument;
use tantivy::schema::Schema;

/// Describes the destination of a document after partition routing and split selection.
#[derive(Debug)]
#[non_exhaustive]
pub struct DocIndexingContext<'a> {
    /// Identifies the index, source, node, and indexing pipeline.
    pub pipeline_id: &'a IndexingPipelineId,
    /// Identifies the initial split receiving this document, not a later merged split.
    pub split_id: &'a SplitId,
    /// Identifies the destination partition, including the overflow partition when applicable.
    pub partition_id: u64,
    /// Identifies the mapping used to prepare the document.
    pub doc_mapping_uid: DocMappingUid,
    /// The schema of the destination split. Added fields must belong to this schema.
    pub schema: &'a Schema,
}

/// Enriches an already mapped document immediately before it is indexed.
///
/// This is an opt-in Rust extension point, not a document mapping option. The callback runs
/// synchronously on the indexing worker and must not perform blocking I/O. An error fails the
/// indexing attempt; the document is not silently skipped. Callbacks can run again on replay and
/// must not rely on exactly-once execution or on a stable split ID across attempts.
///
/// Enrichment must be additive. It must not change existing values, routing, timestamps, clustering
/// inputs, or Quickwit's reserved fields. Mapping and input validation have already run: the
/// callback must supply schema-compatible values and enforce its own collision/cardinality rules.
/// Source storage, document-size accounting, and concatenate fields are not recomputed. Field
/// presence is updated by the indexer after successful enrichment.
///
/// The callback is never invoked by the merge pipeline. Added fields are ordinary Tantivy fields
/// and are preserved by normal merges. Configure it with [`crate::IndexingPipelineParams`] or
/// [`crate::IndexingService::with_doc_enricher_factory`].
pub type DocEnricher =
    Arc<dyn Fn(&DocIndexingContext<'_>, &mut TantivyDocument) -> anyhow::Result<()> + Send + Sync>;

/// Selects an optional enricher when an indexing pipeline is created.
///
/// Returning `None` leaves that pipeline's indexing path unchanged. The factory can validate the
/// mapping and resolve fields once, capturing them in the returned callback. Errors prevent the
/// pipeline from starting. It runs synchronously and must not perform blocking I/O.
pub type DocEnricherFactory = Arc<
    dyn Fn(&IndexingPipelineId, &DocMapper) -> anyhow::Result<Option<DocEnricher>> + Send + Sync,
>;
