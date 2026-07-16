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

//! Columnar two-phase read primitive backing the Trino `ListSplits` /
//! `SearchSplitBatch` endpoints.
//!
//! Phase 1 ([`plan_columnar_splits`]) enumerates the splits a query touches,
//! returning bounded batches with shared index metadata deduplicated. Phase 2
//! (`run_columnar_batch_search`) scans those splits sequentially using
//! `run_columnar_search`, which opens one split, runs the filter as a
//! doc-collecting tantivy query, and hands the matched doc-ids to
//! [`pomsky_arrow`] to produce Arrow [`RecordBatch`](arrow::array::RecordBatch)es — never touching
//! the row-oriented doc store.
//!
//! Arrow-IPC framing, split-token encode/decode and the gRPC transport are the
//! caller's job (the cloudprem endpoint), not this module's.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::{Stream, StreamExt};
use pomsky_arrow::dictionary_builder::DictionaryBuilders;
use pomsky_arrow::read_segment_columns;
use quickwit_doc_mapper::{DocMapper, FastFieldWarmupInfo, WarmupInfo};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::search::{CountHits, SearchRequest, SplitIdAndFooterOffsets};
use quickwit_proto::types::IndexUid;
use quickwit_query::query_ast::QueryAst;
use quickwit_storage::{ByteRangeCache, Storage};
use tantivy::query::{EnableScoring, Weight};
use tantivy::{
    COLLECT_BLOCK_BUFFER_LEN, DocSet, ReloadPolicy, Searcher, SegmentReader, TERMINATED,
};

use crate::SearchError;
use crate::leaf::{open_index_with_caches, warmup};
use crate::root::IndexMetasForLeafSearch;
use crate::service::SearcherContext;

/// Default number of rows per Arrow batch when the request leaves `batch_size`
/// unset (0).
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Maximum number of splits returned in one phase-1 batch. Bounding batches
/// avoids oversized gRPC messages while substantially reducing phase-2 calls.
/// Number is fairly arbitrary for now, can always be changed later.
const MAX_SPLITS_PER_BATCH: usize = 128;

/// A bounded batch of splits sharing the index-level information phase 2 needs.
#[derive(Debug, Clone)]
pub struct ColumnarSplitBatch {
    /// Index the splits belong to.
    pub index_uid: IndexUid,
    /// Storage URI of the index, used to resolve the splits' object storage.
    pub index_uri: String,
    /// JSON-serialized doc mapper shared by every split in the batch.
    pub doc_mapper_str: String,
    /// Total number of documents in the splits.
    pub total_num_docs: u64,
    /// Total on-disk size of the splits, in bytes.
    pub total_size_bytes: u64,
    /// Split ids and footer offsets to read using the shared index-level information.
    pub splits: Vec<SplitIdAndFooterOffsets>,
}

/// One requested output column: which logical fast-field to read (`name`) and
/// the Arrow type to return it as (`data_type`). The Arrow field is emitted
/// under `name` itself.
#[derive(Debug, Clone)]
pub struct ColumnRequest {
    /// Logical fast-field name / dotted JSON path to read.
    pub name: String,
    /// Arrow type to return the column as.
    pub data_type: DataType,
}

/// Batch size for columnar split reads.
#[derive(Debug, Clone, PartialEq)]
pub enum BatchSize {
    /// Use the server default batch size.
    Default,
    /// Use an explicit batch size.
    Value(u32),
}

impl BatchSize {
    fn to_usize(&self) -> usize {
        match self {
            BatchSize::Default => DEFAULT_BATCH_SIZE,
            BatchSize::Value(batch_size) => *batch_size as usize,
        }
    }
}

/// Phase-2 request for a batch of splits after its opaque details have been
/// decoded by the endpoint.
#[derive(Debug)]
pub struct SearchSplitBatchColumnarRequest {
    /// Storage URI of the index the splits belong to.
    pub index_uri: String,
    /// JSON-serialized doc mapper shared by all splits.
    pub doc_mapper_str: String,
    /// Split ids and footer offsets to scan sequentially.
    pub splits: Vec<SplitIdAndFooterOffsets>,
    /// Row filter applied independently within each split.
    pub query_ast: QueryAst,
    /// Columns to project, with their requested Arrow types.
    pub columns: Vec<ColumnRequest>,
    /// Rows per Arrow batch.
    pub batch_size: BatchSize,
    /// Per-split row cap; `None` means unlimited.
    pub limit: Option<usize>,
}

/// Parameters [`plan_columnar_splits`] needs to plan the splits a columnar
/// query touches — the subset of [`SearchRequest`] that phase 1 planning
/// actually reads. `plan_columnar_splits` fills in the remaining
/// `SearchRequest` fields with defaults appropriate for a split-listing-only
/// query (no hits, no aggregation, no sorting), so callers don't need to
/// track the full request shape.
pub struct ColumnarSplitPlanRequest {
    /// Index id patterns to search, e.g. `["logs-*"]`.
    pub index_id_patterns: Vec<String>,
    /// [`QueryAst`] to filter splits and, in phase 2, docs. A time window, if
    /// any, is expected to be encoded as a range query over the timestamp
    /// field within the AST itself — `plan_splits_for_root_search` already
    /// extracts it from there for split pruning, so no separate out-of-band
    /// bound is needed.
    pub query_ast: QueryAst,
}

/// Phase 1 — enumerate batches of splits a query touches.
///
/// Thin wrapper over the existing root-search split planning: parse → derive
/// time range + tag filter → list relevant splits. Splits are grouped by index
/// so index URI and doc mapper are encoded once, then divided into bounded
/// batches.
pub async fn plan_columnar_splits(
    request: ColumnarSplitPlanRequest,
    metastore: &MetastoreServiceClient,
) -> crate::Result<impl Stream<Item = crate::Result<ColumnarSplitBatch>> + use<>> {
    let query_ast_json = serde_json::to_string(&request.query_ast)
        .map_err(|error| SearchError::Internal(error.to_string()))?;
    let mut search_request = SearchRequest {
        index_id_patterns: request.index_id_patterns,
        query_ast: query_ast_json,
        // Split planning extracts timestamp bounds from the query AST.
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 0,
        start_offset: 0,
        aggregation_request: None,
        snippet_fields: Vec::new(),
        sort_fields: Vec::new(),
        scroll_ttl_secs: None,
        search_after: None,
        count_hits: CountHits::Underestimate.into(),
        ignore_missing_indexes: false,
        skip_aggregation_finalization: false,
        enable_request_batching: false,
    };
    // plan_splits_for_root_search requires a mut client for unknown reasons; refactoring
    // that requirement is out of scope for this PR.
    let mut metastore = metastore.clone();
    let (split_metadatas, indexes_meta) =
        crate::root::plan_splits_for_root_search(&mut search_request, &mut metastore).await?;

    Ok(stream_split_batches(split_metadatas, indexes_meta))
}

/// Groups split metadata by index and yields a batch whenever an index reaches
/// the maximum batch size. Partial batches are yielded after all splits have
/// been processed.
/// In the future this could also consider preferred node to allow for optimal
/// routing during searching, but we'll start simple for now.
fn stream_split_batches(
    split_metadatas: Vec<SplitMetadata>,
    indexes_meta: HashMap<IndexUid, IndexMetasForLeafSearch>,
) -> impl Stream<Item = crate::Result<ColumnarSplitBatch>> {
    async_stream::stream! {
        let mut batches = HashMap::<IndexUid, Vec<SplitMetadata>>::new();
        for split_metadata in split_metadatas {
            let index_uid = split_metadata.index_uid.clone();

            // Add split to batch for that index
            let current_batch = batches
                .entry(index_uid.clone())
                .or_default();
            current_batch.push(split_metadata);
            if current_batch.len() < MAX_SPLITS_PER_BATCH {
                continue;
            }

            // Yield batches that are large enough
            let split_metadata_batch = std::mem::take(current_batch);
            yield build_columnar_split_batch_from_index_uid(
                index_uid,
                &indexes_meta,
                split_metadata_batch,
            );
        }

        // Yield all incomplete batches
        for (index_uid, split_metadata_batch) in batches {
            if split_metadata_batch.is_empty() {
                continue;
            }
            yield build_columnar_split_batch_from_index_uid(
                index_uid,
                &indexes_meta,
                split_metadata_batch,
            );
        }
    }
}

fn build_columnar_split_batch_from_index_uid(
    index_uid: IndexUid,
    indexes_meta: &HashMap<IndexUid, IndexMetasForLeafSearch>,
    split_metadatas: Vec<SplitMetadata>,
) -> crate::Result<ColumnarSplitBatch> {
    let Some(index_meta) = indexes_meta.get(&index_uid) else {
        return Err(SearchError::Internal(format!(
            "missing index metadata for split batch `{index_uid}`"
        )));
    };
    build_columnar_split_batch(index_uid, index_meta, split_metadatas)
}

fn build_columnar_split_batch(
    index_uid: IndexUid,
    index_meta: &IndexMetasForLeafSearch,
    split_metadatas: Vec<SplitMetadata>,
) -> crate::Result<ColumnarSplitBatch> {
    let mut total_num_docs = 0u64;
    let mut total_size_bytes = 0u64;
    let mut splits = Vec::with_capacity(split_metadatas.len());
    for split_metadata in split_metadatas {
        let num_docs = split_metadata.num_docs as u64;
        total_num_docs = total_num_docs.checked_add(num_docs).ok_or_else(|| {
            SearchError::Internal("split batch document count overflow".to_string())
        })?;
        total_size_bytes = total_size_bytes
            .checked_add(split_metadata.footer_offsets.end)
            .ok_or_else(|| SearchError::Internal("split batch byte size overflow".to_string()))?;

        splits.push(SplitIdAndFooterOffsets {
            split_id: split_metadata.split_id.to_string(),
            split_footer_start: split_metadata.footer_offsets.start,
            split_footer_end: split_metadata.footer_offsets.end,
            timestamp_start: split_metadata
                .time_range
                .as_ref()
                .map(|time_range| *time_range.start()),
            timestamp_end: split_metadata
                .time_range
                .as_ref()
                .map(|time_range| *time_range.end()),
            num_docs,
        });
    }
    Ok(ColumnarSplitBatch {
        index_uid,
        index_uri: index_meta.index_uri.to_string(),
        doc_mapper_str: index_meta.doc_mapper_str.clone(),
        total_num_docs,
        total_size_bytes,
        splits,
    })
}

/// Builds the projected Arrow schema for the requested columns.
///
/// Each output field is emitted under its logical fast-field name and carries
/// the requested Arrow type. Fails if the same column name is requested more
/// than once.
fn build_projected_schema(columns: &[ColumnRequest]) -> crate::Result<SchemaRef> {
    let mut seen_names = HashSet::with_capacity(columns.len());
    for column in columns {
        if !seen_names.insert(column.name.as_str()) {
            return Err(SearchError::InvalidArgument(format!(
                "duplicate column requested: `{}`",
                column.name
            )));
        }
    }
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| Field::new(&column.name, column.data_type.clone(), true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

/// Resumable per-segment scan state: everything [`SegmentScanCursor::scan_next_batch`]
/// needs, owned outright so the whole cursor can be moved into and back out of
/// a `run_cpu_intensive` call. `run_cpu_intensive` requires an owned `'static`
/// closure, so there is no way to hand it a `&mut` across that boundary —
/// instead the cursor round-trips by value each call, with the [`DocSet`]
/// advanced in place.
struct SegmentScanCursor {
    segment_reader: SegmentReader,
    projected_schema: SchemaRef,
    segment_ord: u32,
    /// Matching docs, pulled `batch_size` at a time. `DocSet` is `Send`, so it
    /// survives the round-trip through `run_cpu_intensive`.
    doc_set: Box<dyn DocSet>,
    /// Dictionaries built incrementally as string columns are read, shared
    /// across every batch of this segment so dictionary indices stay stable.
    dictionary_builders: DictionaryBuilders,
    /// Remaining docs still allowed for this segment; `None` means unlimited.
    remaining: Option<usize>,
}

impl SegmentScanCursor {
    /// Plain constructor: assembles a cursor from already-computed parts.
    fn new(
        segment_reader: SegmentReader,
        projected_schema: SchemaRef,
        segment_ord: u32,
        doc_set: Box<dyn DocSet>,
        remaining: Option<usize>,
    ) -> Self {
        Self {
            segment_reader,
            projected_schema,
            segment_ord,
            doc_set,
            dictionary_builders: DictionaryBuilders::default(),
            remaining,
        }
    }

    /// Reads the next `batch_size` documents from the segment reader
    fn scan_next_batch(
        mut self,
        batch_size: usize,
    ) -> crate::Result<(Self, Option<RecordBatch>, bool)> {
        let (doc_ids, exhausted) = self.collect_doc_ids(batch_size);
        if doc_ids.is_empty() {
            return Ok((self, None, exhausted));
        }

        let batch = read_segment_columns(
            &self.segment_reader,
            &self.projected_schema,
            &doc_ids,
            self.segment_ord,
            &mut self.dictionary_builders,
        )
        .map_err(|error| SearchError::Internal(format!("failed to read columns: {error}")))?;

        Ok((self, Some(batch), exhausted))
    }

    /// Pulls the next `batch_size` matching doc-ids from the `DocSet` (capped by
    /// any remaining limit)
    fn collect_doc_ids(&mut self, batch_size: usize) -> (Vec<u32>, bool) {
        let doc_count = match self.remaining {
            Some(remaining) => batch_size.min(remaining),
            None => batch_size,
        };
        // In tantivy, there are never any deleted docs, so this count reflects the actual number of
        // docs in the set.
        let doc_count = doc_count.min(self.doc_set.count_including_deleted() as usize);

        // Pull batches of doc ids from the set into a vec
        let mut doc_ids = vec![0u32; doc_count];
        let mut doc_ids_written = 0;
        while doc_ids.len() - doc_ids_written >= COLLECT_BLOCK_BUFFER_LEN {
            let start = doc_ids_written;
            let end = start + COLLECT_BLOCK_BUFFER_LEN;
            let Some(slice) = doc_ids.get_mut(start..end) else {
                // There are not COLLECT_BLOCK_BUFFER_LEN records left to fetch, so we break out of
                // the loop and pull the final few individually
                break;
            };

            let Some(array) = slice.as_mut_array::<COLLECT_BLOCK_BUFFER_LEN>() else {
                unreachable!("slice length was checked to be COLLECT_BLOCK_BUFFER_LEN");
            };

            let filled = self.doc_set.fill_buffer(array);
            doc_ids_written += filled;
            if filled < COLLECT_BLOCK_BUFFER_LEN || self.doc_set.doc() == TERMINATED {
                break;
            }
        }

        // Finish filling in the buffer one doc at a time
        let mut doc_id = self.doc_set.doc();
        while doc_id != TERMINATED && doc_ids_written < doc_ids.len() {
            doc_ids[doc_ids_written] = doc_id;
            doc_ids_written += 1;
            doc_id = self.doc_set.advance();
        }
        // This shouldn't ever actually change the size of the doc_ids as we performed enough checks
        // above, but it is included as a safety mechanism
        doc_ids.truncate(doc_ids_written);

        if let Some(remaining) = self.remaining.as_mut() {
            *remaining -= doc_ids.len();
        }

        let exhausted = doc_id == TERMINATED || self.remaining == Some(0);
        (doc_ids, exhausted)
    }
}

/// Drives [`SegmentScanCursor::scan_next_batch`] on the CPU thread-pool in a
/// loop, yielding each batch as it is produced. Each `run_cpu_intensive` call
/// does bounded work (one batch's worth of column reads) and returns its
/// thread to the pool immediately after — a slow downstream consumer parks
/// this async task, not a CPU-pool thread, since the next call is only
/// scheduled once this stream is polled again.
fn scan_segment_stream(
    weight: &dyn Weight,
    segment_reader: SegmentReader,
    projected_schema: SchemaRef,
    segment_ord: u32,
    batch_size: usize,
    limit: Option<usize>,
) -> crate::Result<impl Stream<Item = crate::Result<RecordBatch>>> {
    let doc_set: Box<dyn DocSet> = weight
        .scorer(&segment_reader, 1.0)
        .map_err(|error| SearchError::Internal(format!("failed to build scorer: {error}")))?;
    Ok(async_stream::try_stream! {
        let mut cursor = SegmentScanCursor::new(
            segment_reader,
            projected_schema,
            segment_ord,
            doc_set,
            limit,
        );
        loop {
            let (new_cursor, batch, exhausted) =
                crate::search_thread_pool()
                    .run_cpu_intensive(move || cursor.scan_next_batch(batch_size))
                    .await
                    .map_err(|_| SearchError::Internal("segment scan panicked".to_string()))??;

            cursor = new_cursor;
            if let Some(batch) = batch {
                yield batch;
            }
            if exhausted {
                break;
            }
        }
    })
}

fn scan_all_segments_stream(
    weight: Box<dyn Weight>,
    segment_readers: Vec<SegmentReader>,
    projected_schema: SchemaRef,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = crate::Result<RecordBatch>> {
    async_stream::try_stream! {
        let mut remaining = limit;
        for (segment_ord, segment_reader) in segment_readers.into_iter().enumerate() {
            if remaining == Some(0) {
                break;
            }

            let segment_stream = scan_segment_stream(
                weight.as_ref(),
                segment_reader,
                projected_schema.clone(),
                segment_ord as u32,
                batch_size,
                remaining,
            )?;
            futures::pin_mut!(segment_stream);
            while let Some(batch) = segment_stream.next().await {
                let batch = batch?;
                if let Some(remaining) = remaining.as_mut() {
                    *remaining = remaining.saturating_sub(batch.num_rows());
                }
                yield batch;
            }
        }
    }
}

fn validate_batch_search_request(request: &SearchSplitBatchColumnarRequest) -> crate::Result<()> {
    if request.batch_size == BatchSize::Value(0) {
        return Err(SearchError::InvalidArgument(
            "batch_size must be greater than 0".to_string(),
        ));
    }
    if request.limit == Some(0) {
        return Err(SearchError::InvalidArgument(
            "limit must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

/// Runs the phase-2 scan of a single split and returns it as a lazy stream of
/// 0..N Arrow record batches. A split with no matches yields zero batches.
// Keeping these per-split inputs explicit is clearer than recreating a request bundle.
#[allow(clippy::too_many_arguments)]
fn run_columnar_search(
    searcher_context: Arc<SearcherContext>,
    index_storage: Arc<dyn Storage>,
    doc_mapper: Arc<DocMapper>,
    split: SplitIdAndFooterOffsets,
    query_ast: QueryAst,
    projected_schema: SchemaRef,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = crate::Result<RecordBatch>> {
    async_stream::try_stream! {
        // 1. Open the split. Use an indepenent byte range cache per split
        let byte_range_cache =
            ByteRangeCache::with_infinite_capacity(&quickwit_storage::metrics::SHORTLIVED_CACHE);
        let (index, _hot_directory) = open_index_with_caches(
            &searcher_context,
            index_storage,
            &split,
            Some(doc_mapper.tokenizer_manager()),
            Some(byte_range_cache),
        )
        .await
        .map_err(|error| SearchError::Internal(format!("failed to open split: {error}")))?;
        let searcher = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?
            .searcher();
        let split_schema = index.schema();

        // 2. Build the filter query and warm up in a single pass: query terms
        // (inverted index) plus the projected columns (fast fields). Tantivy
        // cannot do async IO mid-search, so both must be pre-loaded. The
        // projected fast fields are appended to the query's `WarmupInfo` so the
        // existing `warmup` handles them; `pomsky_arrow` reads only the columns
        // whose name matches the Arrow field name, so the synthetic `_doc_id` /
        // `_segment_ord` columns are skipped here.
        let (query, warmup_info) =
            doc_mapper.query(split_schema, query_ast, false, None)?;
        let provably_empty = warmup_columns(&searcher, &projected_schema, warmup_info).await?;
        if provably_empty {
            // A required term's posting list was empty: this split matches nothing.
            return;
        }

        // 3 + 4. Per segment: collect matching doc-ids and read the projection.
        let weight = query
                .weight(EnableScoring::disabled_from_searcher(&searcher))
                .map_err(|error| {
                    SearchError::Internal(format!("failed to build weight: {error}"))
                })?;
        let segment_readers = searcher.segment_readers().to_vec();

        let stream = scan_all_segments_stream(
            weight,
            segment_readers,
            projected_schema,
            batch_size,
            limit,
        );
        futures::pin_mut!(stream);
        while let Some(batch) = stream.next().await {
            yield batch?;
        }
    }
}

async fn warmup_columns(
    searcher: &Searcher,
    projected_schema: &Schema,
    mut warmup_info: WarmupInfo,
) -> crate::Result<bool> {
    warmup_info.fast_fields.extend(
        projected_schema
            .fields()
            .iter()
            .filter(|field| field.name() != "_doc_id" || field.name() != "_segment_ord")
            .map(|field| FastFieldWarmupInfo {
                name: field.name().clone(),
                with_subfields: false,
            }),
    );
    warmup_info.simplify();

    warmup(searcher, &warmup_info, &|_, _| {})
        .await
        .map_err(|error| SearchError::Internal(format!("warmup failed: {error}")))
}

/// Scans each split sequentially and concatenates their record-batch streams.
/// Nothing runs until the stream is polled, and dropping it stops the current
/// split without opening any remaining splits.
pub(crate) fn run_columnar_batch_search(
    searcher_context: Arc<SearcherContext>,
    index_storage: Arc<dyn Storage>,
    request: SearchSplitBatchColumnarRequest,
    doc_mapper: Arc<DocMapper>,
) -> impl Stream<Item = crate::Result<RecordBatch>> {
    async_stream::try_stream! {
        validate_batch_search_request(&request)?;
        let projected_schema = build_projected_schema(&request.columns)?;
        let batch_size = request.batch_size.to_usize();

        let mut remaining = request.limit;
        for split in request.splits {
            if remaining == Some(0) {
                break;
            }

            let split_stream = run_columnar_search(
                searcher_context.clone(),
                index_storage.clone(),
                doc_mapper.clone(),
                split,
                request.query_ast.clone(),
                projected_schema.clone(),
                batch_size,
                remaining,
            );
            futures::pin_mut!(split_stream);
            while let Some(batch_result) = split_stream.next().await {
                let batch = batch_result?;
                if let Some(remaining) = remaining.as_mut() {
                    *remaining = remaining.saturating_sub(batch.num_rows());
                }
                yield batch;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use arrow::array::AsArray;
    use arrow::datatypes::UInt64Type;
    use tantivy::query::{AllQuery, EnableScoring, Query};
    use tantivy::schema::{FAST, SchemaBuilder, TEXT};
    use tantivy::{Index, IndexWriter, TantivyDocument};

    use super::*;

    fn build_index(num_docs: u64) -> Index {
        let mut builder = SchemaBuilder::new();
        let id_field = builder.add_u64_field("id", FAST);
        let name_field = builder.add_text_field("name", FAST | TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
        for id in 0..num_docs {
            let mut doc = TantivyDocument::default();
            doc.add_u64(id_field, id);
            doc.add_text(name_field, format!("name-{id}"));
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
        index
    }

    fn all_query_weight(searcher: &tantivy::Searcher) -> Box<dyn tantivy::query::Weight> {
        AllQuery
            .weight(EnableScoring::disabled_from_searcher(searcher))
            .unwrap()
    }

    fn split_metadata(index_uid: &IndexUid, split_id: usize) -> SplitMetadata {
        SplitMetadata {
            split_id: format!("split-{split_id}").into(),
            index_uid: index_uid.clone(),
            num_docs: 1,
            footer_offsets: 10..20,
            ..Default::default()
        }
    }

    fn index_meta(index_uri: &str, doc_mapper_str: &str) -> IndexMetasForLeafSearch {
        IndexMetasForLeafSearch {
            index_uri: quickwit_common::uri::Uri::from_str(index_uri).unwrap(),
            doc_mapper_str: doc_mapper_str.to_string(),
        }
    }

    #[tokio::test]
    async fn split_batches_deduplicate_index_fields_and_bound_batch_size() {
        let index_uid_a = IndexUid::for_test("index-a", 1);
        let index_uid_b = IndexUid::for_test("index-b", 1);
        let mut split_metadatas = Vec::new();
        for split_id in 0..MAX_SPLITS_PER_BATCH {
            split_metadatas.push(split_metadata(&index_uid_a, split_id));
        }
        split_metadatas.push(split_metadata(&index_uid_b, MAX_SPLITS_PER_BATCH));
        split_metadatas.push(split_metadata(&index_uid_a, MAX_SPLITS_PER_BATCH + 1));

        let indexes_meta = HashMap::from([
            (
                index_uid_a.clone(),
                index_meta("s3://indexes/a", "mapper-a"),
            ),
            (
                index_uid_b.clone(),
                index_meta("s3://indexes/b", "mapper-b"),
            ),
        ]);
        let batches = stream_split_batches(split_metadatas, indexes_meta);
        futures::pin_mut!(batches);
        let first_batch = batches.next().await.unwrap().unwrap();
        let remaining_batches = batches.collect::<Vec<_>>().await;
        let mut all_batches = vec![first_batch];
        all_batches.extend(remaining_batches.into_iter().map(Result::unwrap));

        assert_eq!(all_batches.len(), 3);
        let full_batch = all_batches
            .iter()
            .find(|batch| {
                batch.index_uid == index_uid_a && batch.splits.len() == MAX_SPLITS_PER_BATCH
            })
            .unwrap();
        assert_eq!(full_batch.total_num_docs, MAX_SPLITS_PER_BATCH as u64);
        assert_eq!(
            full_batch.total_size_bytes,
            (MAX_SPLITS_PER_BATCH * 20) as u64
        );
        assert_eq!(full_batch.doc_mapper_str, "mapper-a");

        let partial_a = all_batches
            .iter()
            .find(|batch| batch.index_uid == index_uid_a && batch.splits.len() == 1)
            .unwrap();
        assert_eq!(partial_a.total_num_docs, 1);
        assert_eq!(partial_a.total_size_bytes, 20);

        let partial_b = all_batches
            .iter()
            .find(|batch| batch.index_uid == index_uid_b)
            .unwrap();
        assert_eq!(partial_b.splits.len(), 1);
        assert_eq!(partial_b.doc_mapper_str, "mapper-b");
    }

    #[test]
    fn build_projected_schema_uses_field_name() {
        let columns = vec![
            ColumnRequest {
                name: "service".to_string(),
                data_type: DataType::Utf8,
            },
            ColumnRequest {
                name: "http.status_code".to_string(),
                data_type: DataType::Int64,
            },
        ];
        let schema = build_projected_schema(&columns).unwrap();
        assert_eq!(schema.field(0).name(), "service");
        assert_eq!(schema.field(1).name(), "http.status_code");
        assert!(schema.field(0).metadata().is_empty());
        assert!(schema.field(1).metadata().is_empty());
    }

    #[test]
    fn build_projected_schema_rejects_duplicate_column_names() {
        let columns = vec![
            ColumnRequest {
                name: "service".to_string(),
                data_type: DataType::Utf8,
            },
            ColumnRequest {
                name: "service".to_string(),
                data_type: DataType::Int64,
            },
        ];
        let error = build_projected_schema(&columns).unwrap_err();
        assert!(matches!(error, SearchError::InvalidArgument(_)));
    }

    /// Drives [`SegmentScanCursor::scan_next_batch`] to completion
    /// synchronously, mirroring what [`scan_segment_stream`] does through the
    /// CPU pool.
    fn scan_all(
        weight: &dyn Weight,
        segment_reader: &SegmentReader,
        projected_schema: &SchemaRef,
        segment_ord: u32,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        let doc_set: Box<dyn DocSet> = weight.scorer(segment_reader, 1.0).unwrap();
        let cursor = SegmentScanCursor::new(
            segment_reader.clone(),
            projected_schema.clone(),
            segment_ord,
            doc_set,
            limit,
        );
        let mut cursor = cursor;
        loop {
            let (new_cursor, batch, exhausted) = cursor.scan_next_batch(batch_size).unwrap();
            cursor = new_cursor;
            batches.extend(batch);
            if exhausted {
                break;
            }
        }
        batches
    }

    #[test]
    fn scan_segment_batches_respect_size_and_limit() {
        let index = build_index(10);
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = &searcher.segment_readers()[0];
        let weight = all_query_weight(&searcher);
        let schema = build_projected_schema(&[ColumnRequest {
            name: "id".to_string(),
            data_type: DataType::UInt64,
        }])
        .unwrap();

        // batch_size 4 over 10 docs -> 4 + 4 + 2.
        let batches = scan_all(weight.as_ref(), segment_reader, &schema, 0, 4, None);
        let rows: Vec<usize> = batches.iter().map(|batch| batch.num_rows()).collect();
        assert_eq!(rows, vec![4, 4, 2]);

        // limit 5 caps the collected docs before batching.
        let limited = scan_all(weight.as_ref(), segment_reader, &schema, 0, 4, Some(5));
        let total: usize = limited.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total, 5);

        // Values are read in doc order.
        let first = &limited[0];
        let ids = first.column(0).as_primitive::<UInt64Type>();
        assert_eq!(ids.values(), &[0, 1, 2, 3]);
    }

    #[test]
    fn scan_next_batch_resumes_across_calls() {
        let index = build_index(10);
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = &searcher.segment_readers()[0];
        let weight = all_query_weight(&searcher);
        let schema = build_projected_schema(&[ColumnRequest {
            name: "id".to_string(),
            data_type: DataType::UInt64,
        }])
        .unwrap();

        let doc_set: Box<dyn DocSet> = weight.scorer(segment_reader, 1.0).unwrap();
        let cursor = SegmentScanCursor::new(segment_reader.clone(), schema, 0, doc_set, None);

        // Each call resumes the shared `DocSet` where the previous left off, so
        // the doc-ids run contiguously without ever rebuilding the doc list.
        let (cursor, first_batch, exhausted) = cursor.scan_next_batch(4).unwrap();
        let first_batch = first_batch.unwrap();
        assert_eq!(first_batch.num_rows(), 4);
        assert_eq!(
            first_batch.column(0).as_primitive::<UInt64Type>().values(),
            &[0, 1, 2, 3]
        );
        assert!(!exhausted);

        let (cursor, second_batch, exhausted) = cursor.scan_next_batch(4).unwrap();
        let second_batch = second_batch.unwrap();
        assert_eq!(second_batch.num_rows(), 4);
        assert_eq!(
            second_batch.column(0).as_primitive::<UInt64Type>().values(),
            &[4, 5, 6, 7]
        );
        assert!(!exhausted);

        let (_cursor, third_batch, exhausted) = cursor.scan_next_batch(4).unwrap();
        let third_batch = third_batch.unwrap();
        assert_eq!(third_batch.num_rows(), 2);
        assert_eq!(
            third_batch.column(0).as_primitive::<UInt64Type>().values(),
            &[8, 9]
        );
        assert!(exhausted);
    }

    #[test]
    fn run_columnar_batch_search_rejects_zero_batch_size_and_limit() {
        assert!(matches!(
            validate_batch_search_request(&SearchSplitBatchColumnarRequest {
                index_uri: String::new(),
                doc_mapper_str: String::new(),
                splits: Vec::new(),
                query_ast: QueryAst::MatchAll,
                columns: Vec::new(),
                batch_size: BatchSize::Value(0),
                limit: None,
            }),
            Err(SearchError::InvalidArgument(message)) if message == "batch_size must be greater than 0"
        ));
        assert!(matches!(
            validate_batch_search_request(&SearchSplitBatchColumnarRequest {
                index_uri: String::new(),
                doc_mapper_str: String::new(),
                splits: Vec::new(),
                query_ast: QueryAst::MatchAll,
                columns: Vec::new(),
                batch_size: BatchSize::Default,
                limit: Some(0),
            }),
            Err(SearchError::InvalidArgument(message)) if message == "limit must be greater than 0"
        ));
    }
}
