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
//! `SearchSplit` endpoints.
//!
//! Phase 1 ([`plan_columnar_splits`]) enumerates the splits a query touches,
//! returning a self-sufficient descriptor per split. Phase 2
//! (`run_columnar_search`) opens exactly one split, runs the filter as a
//! doc-collecting tantivy query, and hands the matched doc-ids to
//! [`pomsky_arrow`] to produce Arrow [`RecordBatch`](arrow::array::RecordBatch)es — never touching
//! the row-oriented doc store.
//!
//! Arrow-IPC framing, split-token encode/decode and the gRPC transport are the
//! caller's job (the cloudprem endpoint), not this module's.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::{Stream, StreamExt};
use pomsky_arrow::{DictCache, DocSelection, read_segment_columns, warm_up_fast_fields};
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::search::{CountHits, SearchRequest, SplitIdAndFooterOffsets};
use quickwit_proto::types::IndexUid;
use quickwit_query::query_ast::QueryAst;
use quickwit_storage::{ByteRangeCache, Storage};
use tantivy::query::{EnableScoring, Weight};
use tantivy::{DocSet, ReloadPolicy, SegmentReader, TERMINATED};

use crate::SearchError;
use crate::leaf::{WarmupOutcome, open_index_with_caches, warmup};
use crate::service::SearcherContext;

/// Default number of rows per Arrow batch when the request leaves `batch_size`
/// unset (0).
const DEFAULT_BATCH_SIZE: usize = 8192;

/// A single split a columnar query touches, with everything phase 2 needs to
/// open and read it directly from object storage — no metastore round-trip.
#[derive(Debug, Clone)]
pub struct ColumnarSplitDescriptor {
    /// Split id and footer offsets needed to open the split directly.
    pub split: SplitIdAndFooterOffsets,
    /// Index the split belongs to.
    pub index_uid: IndexUid,
    /// Storage URI of the index, used to resolve the split's object storage.
    pub index_uri: String,
    /// JSON-serialized doc mapper.
    pub doc_mapper_str: String,
    /// Approximate on-disk size of the split, for client-side cost estimation.
    pub size_bytes: u64,
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
#[derive(Debug, Clone)]
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

/// Phase-2 request, after the opaque split token has been decoded by the
/// endpoint into its native parts.
#[derive(Debug, Clone)]
pub struct SearchSplitColumnarRequest {
    /// Storage URI of the index the split belongs to.
    pub index_uri: String,
    /// JSON-serialized doc mapper, carried in the split token.
    pub doc_mapper_str: String,
    /// Split id and footer offsets to open the split.
    pub split: SplitIdAndFooterOffsets,
    /// Row filter applied within the split. A time window, if any, is
    /// expected to be encoded as a range query over the timestamp field
    /// within the AST itself.
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

/// Phase 1 — enumerate the splits a query touches.
///
/// Thin wrapper over the existing root-search split planning: parse → derive
/// time range + tag filter → list relevant splits. Returns a self-sufficient
/// descriptor per split.
pub async fn plan_columnar_splits(
    request: ColumnarSplitPlanRequest,
    metastore: &MetastoreServiceClient,
) -> crate::Result<Vec<ColumnarSplitDescriptor>> {
    let query_ast_json = serde_json::to_string(&request.query_ast)
        .map_err(|error| SearchError::Internal(error.to_string()))?;
    let mut search_request = SearchRequest {
        index_id_patterns: request.index_id_patterns,
        query_ast: query_ast_json,
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

    let descriptors = split_metadatas
        .iter()
        .map(|split_metadata| {
            let Some(index_meta) = indexes_meta.get(&split_metadata.index_uid) else {
                // Every listed split belongs to one of the planned indexes; a miss
                // here is a real bug, not a skippable absence.
                return Err(SearchError::Internal(format!(
                    "missing index metadata for split `{}`",
                    split_metadata.split_id
                )));
            };
            let split = SplitIdAndFooterOffsets {
                split_id: split_metadata.split_id.clone(),
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
                num_docs: split_metadata.num_docs as u64,
            };
            Ok(ColumnarSplitDescriptor {
                split,
                index_uid: split_metadata.index_uid.clone(),
                index_uri: index_meta.index_uri.to_string(),
                doc_mapper_str: index_meta.doc_mapper_str.clone(),
                size_bytes: split_metadata.footer_offsets.end,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(descriptors)
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

/// Walks the scorer, collecting alive matching doc-ids in doc order (capped by
/// `limit`). The scorer itself is `tantivy`-internal state that cannot cross a
/// `run_cpu_intensive` call boundary (`Box<dyn Scorer>` is not `Send`), so this
/// must run start-to-finish within a single CPU-pool call.
fn collect_alive_doc_ids(
    weight: &dyn Weight,
    segment_reader: &SegmentReader,
    limit: Option<usize>,
) -> crate::Result<Vec<u32>> {
    let mut scorer = weight
        .scorer(segment_reader, 1.0)
        .map_err(|error| SearchError::Internal(format!("failed to build scorer: {error}")))?;
    let alive_bitset_opt = segment_reader.alive_bitset();

    let mut doc_ids: Vec<u32> = Vec::new();
    let mut doc_id = scorer.doc();
    while doc_id != TERMINATED {
        let is_alive = match alive_bitset_opt {
            Some(alive_bitset) => alive_bitset.is_alive(doc_id),
            None => true,
        };
        if is_alive {
            doc_ids.push(doc_id);
            if let Some(limit) = limit
                && doc_ids.len() >= limit
            {
                break;
            }
        }
        doc_id = scorer.advance();
    }
    Ok(doc_ids)
}

/// Collects every matching alive doc-id and builds the dictionary cache for a
/// segment, then assembles the resulting [`SegmentScanCursor`]. This is the
/// heavy, once-per-segment setup step — run it on the CPU thread-pool.
fn collect_segment_scan_cursor(
    weight: &dyn Weight,
    segment_reader: SegmentReader,
    projected_schema: SchemaRef,
    segment_ord: u32,
    limit: Option<usize>,
) -> crate::Result<SegmentScanCursor> {
    let doc_ids = collect_alive_doc_ids(weight, &segment_reader, limit)?;
    let dict_cache = if doc_ids.is_empty() {
        None
    } else {
        let dict_cache = DictCache::build(&segment_reader, &projected_schema).map_err(|error| {
            SearchError::Internal(format!("failed to build dictionary cache: {error}"))
        })?;
        Some(dict_cache)
    };
    Ok(SegmentScanCursor::new(
        segment_reader,
        projected_schema,
        segment_ord,
        doc_ids,
        dict_cache,
    ))
}

/// Resumable per-segment scan state: everything [`SegmentScanCursor::scan_next_batch`]
/// needs, owned outright so the whole cursor can be moved into and back out of
/// a `run_cpu_intensive` call. `run_cpu_intensive` requires an owned `'static`
/// closure, so there is no way to hand it a `&mut` across that boundary —
/// instead the cursor round-trips by value each call, with `offset` advanced.
struct SegmentScanCursor {
    segment_reader: SegmentReader,
    projected_schema: SchemaRef,
    segment_ord: u32,
    /// Alive matching doc-ids, collected once up front.
    doc_ids: Vec<u32>,
    /// `None` when `doc_ids` is empty — no dictionary-typed column can match.
    dict_cache: Option<DictCache>,
    offset: usize,
}

impl SegmentScanCursor {
    /// Plain constructor: assembles a cursor from already-computed parts. See
    /// [`collect_segment_scan_cursor`] for the (CPU-heavy) doc-id and
    /// dictionary-cache collection that feeds this.
    fn new(
        segment_reader: SegmentReader,
        projected_schema: SchemaRef,
        segment_ord: u32,
        doc_ids: Vec<u32>,
        dict_cache: Option<DictCache>,
    ) -> Self {
        Self {
            segment_reader,
            projected_schema,
            segment_ord,
            doc_ids,
            dict_cache,
            offset: 0,
        }
    }

    /// Produces the next `batch_size`-worth of rows, resuming from
    /// `self.offset`. Returns the advanced cursor for the caller to pass into
    /// the next call, the batch (if any), and whether the segment is now
    /// exhausted. Runs on the CPU thread-pool.
    fn scan_next_batch(
        mut self,
        batch_size: usize,
    ) -> crate::Result<(Self, Option<RecordBatch>, bool)> {
        let chunk_end = (self.offset + batch_size).min(self.doc_ids.len());
        let exhausted = chunk_end >= self.doc_ids.len();
        let chunk = &self.doc_ids[self.offset..chunk_end];
        if chunk.is_empty() {
            return Ok((self, None, exhausted));
        }

        let batch = read_segment_columns(
            &self.segment_reader,
            &self.projected_schema,
            DocSelection::Ids(chunk),
            self.segment_ord,
            self.dict_cache.as_ref(),
        )
        .map_err(|error| SearchError::Internal(format!("failed to read columns: {error}")))?;

        self.offset = chunk_end;
        Ok((self, Some(batch), exhausted))
    }
}

/// Drives [`SegmentScanCursor::scan_next_batch`] on the CPU thread-pool in a
/// loop, yielding each batch as it is produced. Each `run_cpu_intensive` call
/// does bounded work (one batch's worth of column reads) and returns its
/// thread to the pool immediately after — a slow downstream consumer parks
/// this async task, not a CPU-pool thread, since the next call is only
/// scheduled once this stream is polled again.
fn scan_segment_stream(
    weight: Arc<Box<dyn Weight>>,
    segment_reader: SegmentReader,
    projected_schema: SchemaRef,
    segment_ord: u32,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = crate::Result<RecordBatch>> {
    async_stream::try_stream! {
        let mut cursor = run_cpu_intensive(move || {
            collect_segment_scan_cursor(
                weight.as_ref().as_ref(),
                segment_reader,
                projected_schema,
                segment_ord,
                limit,
            )
        })
        .await
        .map_err(|_| SearchError::Internal("segment scan panicked".to_string()))??;

        loop {
            let (new_cursor, batch, exhausted) =
                run_cpu_intensive(move || cursor.scan_next_batch(batch_size))
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
    }
}

fn scan_all_segments_stream(
    weight: Arc<Box<dyn Weight>>,
    segment_readers: Vec<SegmentReader>,
    projected_schema: SchemaRef,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = crate::Result<RecordBatch>> {
    async_stream::try_stream! {
        let mut remaining = limit;
        for (segment_ord, segment_reader) in segment_readers.into_iter().enumerate() {
            if let Some(0) = remaining {
                break;
            }

            let segment_limit = remaining;
            let segment_stream = scan_segment_stream(
                weight.clone(),
                segment_reader,
                projected_schema.clone(),
                segment_ord as u32,
                batch_size,
                segment_limit,
            );
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

fn validate_search_request(request: &SearchSplitColumnarRequest) -> crate::Result<()> {
    if let BatchSize::Value(0) = request.batch_size {
        return Err(SearchError::InvalidArgument(
            "batch_size must be greater than 0".to_string(),
        ));
    }
    if let Some(0) = request.limit {
        return Err(SearchError::InvalidArgument(
            "limit must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

/// Runs the phase-2 scan of a single split and returns it as a lazy stream of
/// 0..N Arrow record batches. Nothing runs until the stream is polled, and
/// polling stops scanning as soon as the caller stops driving it — no channel,
/// no sink.
///
/// Callers already know the projected schema: it's derived entirely from the
/// `columns` they requested (see [`build_projected_schema`]), so it isn't
/// echoed back on the stream. A split with no matches yields zero batches.
pub(crate) fn run_columnar_search(
    searcher_context: Arc<SearcherContext>,
    index_storage: Arc<dyn Storage>,
    request: SearchSplitColumnarRequest,
    doc_mapper: Arc<DocMapper>,
) -> impl Stream<Item = crate::Result<RecordBatch>> {
    async_stream::try_stream! {
        validate_search_request(&request)?;

        let batch_size = request.batch_size.to_usize();

        // 1. Open the split.
        let byte_range_cache =
            ByteRangeCache::with_infinite_capacity(&quickwit_storage::metrics::SHORTLIVED_CACHE);
        let (index, _hot_directory) = open_index_with_caches(
            &searcher_context,
            index_storage,
            &request.split,
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

        // 2. Build the projected Arrow schema.
        let projected_schema = build_projected_schema(&request.columns)?;

        // 3. Build the filter query and warm up: query terms (inverted index) then
        // the projected columns (fast fields). Tantivy cannot do async IO mid-search.
        let (query, mut warmup_info) =
            doc_mapper.query(split_schema, request.query_ast, false, None)?;
        warmup_info.simplify();
        let warmup_outcome = warmup(&searcher, &warmup_info)
            .await
            .map_err(|error| SearchError::Internal(format!("warmup failed: {error}")))?;

        if warmup_outcome == WarmupOutcome::ProvablyEmpty {
            // A required term's posting list was empty: this split matches nothing.
            return;
        }

        warm_up_fast_fields(&searcher, &projected_schema)
            .await
            .map_err(|error| SearchError::Internal(format!("fast-field warmup failed: {error}")))?;

        // 4 + 5. Per segment: collect matching doc-ids and read the projection.
        let weight: Arc<Box<dyn Weight>> = Arc::new(
            query
                .weight(EnableScoring::disabled_from_searcher(&searcher))
                .map_err(|error| {
                    SearchError::Internal(format!("failed to build weight: {error}"))
                })?,
        );
        let segment_readers: Vec<SegmentReader> = searcher.segment_readers().to_vec();

        let stream = scan_all_segments_stream(
            weight,
            segment_readers,
            projected_schema,
            batch_size,
            request.limit,
        );
        futures::pin_mut!(stream);
        while let Some(batch) = stream.next().await {
            yield batch?;
        }
    }
}

#[cfg(test)]
mod tests {
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
        let mut cursor = collect_segment_scan_cursor(
            weight,
            segment_reader.clone(),
            projected_schema.clone(),
            segment_ord,
            limit,
        )
        .unwrap();
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
    fn scan_next_batch_resumes_across_calls_without_rebuilding_docs() {
        let index = build_index(10);
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = &searcher.segment_readers()[0];
        let weight = all_query_weight(&searcher);
        let schema = build_projected_schema(&[ColumnRequest {
            name: "id".to_string(),
            data_type: DataType::UInt64,
        }])
        .unwrap();

        let cursor =
            collect_segment_scan_cursor(weight.as_ref(), segment_reader.clone(), schema, 0, None)
                .unwrap();
        let (cursor, first_batch, exhausted) = cursor.scan_next_batch(4).unwrap();
        assert_eq!(first_batch.unwrap().num_rows(), 4);
        assert_eq!(cursor.offset, 4);
        assert!(!exhausted);
        let doc_ids_ptr = cursor.doc_ids.as_ptr();

        let (cursor, second_batch, exhausted) = cursor.scan_next_batch(4).unwrap();
        assert_eq!(second_batch.unwrap().num_rows(), 4);
        assert_eq!(cursor.offset, 8);
        assert!(!exhausted);
        // The doc-id list built once up front is reused, not rebuilt.
        assert_eq!(doc_ids_ptr, cursor.doc_ids.as_ptr());

        let (cursor, third_batch, exhausted) = cursor.scan_next_batch(4).unwrap();
        assert_eq!(third_batch.unwrap().num_rows(), 2);
        assert_eq!(cursor.offset, 10);
        assert!(exhausted);
    }

    #[test]
    fn run_columnar_search_rejects_zero_batch_size_and_limit() {
        assert!(matches!(
            validate_search_request(&SearchSplitColumnarRequest {
                index_uri: String::new(),
                doc_mapper_str: String::new(),
                split: SplitIdAndFooterOffsets {
                    split_id: String::new(),
                    split_footer_start: 0,
                    split_footer_end: 0,
                    timestamp_start: None,
                    timestamp_end: None,
                    num_docs: 0,
                },
                query_ast: QueryAst::MatchAll,
                columns: Vec::new(),
                batch_size: BatchSize::Value(0),
                limit: None,
            }),
            Err(SearchError::InvalidArgument(message)) if message == "batch_size must be greater than 0"
        ));
        assert!(matches!(
            validate_search_request(&SearchSplitColumnarRequest {
                index_uri: String::new(),
                doc_mapper_str: String::new(),
                split: SplitIdAndFooterOffsets {
                    split_id: String::new(),
                    split_footer_start: 0,
                    split_footer_end: 0,
                    timestamp_start: None,
                    timestamp_end: None,
                    num_docs: 0,
                },
                query_ast: QueryAst::MatchAll,
                columns: Vec::new(),
                batch_size: BatchSize::Default,
                limit: Some(0),
            }),
            Err(SearchError::InvalidArgument(message)) if message == "limit must be greater than 0"
        ));
    }
}
