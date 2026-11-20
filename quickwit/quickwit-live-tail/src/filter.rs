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

use std::sync::{Arc, LazyLock};
use std::time::Instant;

use quickwit_doc_mapper::DocMapper;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use quickwit_query::query_ast::QueryAst;
use tantivy::collector::DocSetCollector;
use tantivy::directory::RamDirectory;
use tantivy::jitexpr::compile::ExprCompilationCache;
use tantivy::schema::{Field, Value};
use tantivy::{Index, IndexBuilder, ReloadPolicy, TantivyDocument};

use crate::hub::{TailBatch, TailDoc};
use crate::metrics::{
    LIVE_TAIL_FILTER_DURATION_SECS, LIVE_TAIL_FILTERED_DOCS_MATCHED,
    LIVE_TAIL_FILTERED_DOCS_UNMATCHED,
};

/// Memory budget of the in-RAM index built for each batch. It is not enforced by the
/// single-segment writer: it only sizes the term hash table, which is preallocated, so it is kept
/// small. Larger values do not make filtering faster.
const INDEX_WRITER_MEMORY_BUDGET_BYTES: usize = 3_000_000;

/// Documents matching a query.
#[derive(Debug)]
pub(crate) struct FilteredDocs {
    pub num_matching_docs: usize,
    /// Number of matching documents larger than `max_doc_num_bytes` once serialized.
    pub num_too_large_docs: usize,
    /// The first `max_num_docs` matching documents that are not too large, serialized as JSON, in
    /// processing order.
    pub docs_json: Vec<Vec<u8>>,
}

/// Counts the documents of `tail_batches` matching `query_ast` and serializes the first
/// `max_num_docs` of them, so that no work is spent on documents dropped by the rate limiter.
///
/// Matching documents larger than `max_doc_num_bytes` once serialized cannot be sent: they are
/// counted in `num_too_large_docs` and do not count toward `max_num_docs`.
///
/// The query is evaluated with the same semantics as a search: the documents are indexed into a
/// single-segment in-RAM Tantivy index built with the doc mapper of their shard, and the query
/// is built with `DocMapper::query`, exactly like a leaf search does on a split.
///
/// Precondition: `query_ast` must be resolved, i.e. it must not contain `UserInput` nodes.
///
/// The documents of the batches that no other session filtered yet are parsed here.
///
/// This is CPU-intensive: call it from the live tail thread pool.
pub(crate) fn filter_batches(
    query_ast: &QueryAst,
    tail_batches: &[Arc<TailBatch>],
    max_num_docs: usize,
    max_doc_num_bytes: usize,
) -> anyhow::Result<FilteredDocs> {
    let start = Instant::now();
    let mut num_docs = 0;
    let mut num_matching_docs = 0;
    let mut num_too_large_docs = 0;
    let mut docs_json: Vec<Vec<u8>> = Vec::new();

    // Batches persisted with different doc mappings, e.g. right after an index update, are
    // filtered separately.
    for same_mapping_batches in tail_batches.chunk_by(|left, right| {
        left.doc_mapper.doc_mapping_uid() == right.doc_mapper.doc_mapping_uid()
    }) {
        let doc_mapper = &same_mapping_batches[0].doc_mapper;
        let docs: Vec<&TailDoc> = same_mapping_batches
            .iter()
            .flat_map(|tail_batch| tail_batch.docs().iter())
            .collect();
        num_docs += docs.len();

        let doc_positions = matching_doc_positions(doc_mapper, query_ast, &docs)?;
        num_matching_docs += doc_positions.len();

        for doc_position in doc_positions {
            if docs_json.len() == max_num_docs {
                break;
            }
            let doc_json = serde_json::to_vec(&docs[doc_position].json_obj)?;

            if doc_json.len() > max_doc_num_bytes {
                num_too_large_docs += 1;
                continue;
            }
            docs_json.push(doc_json);
        }
    }
    LIVE_TAIL_FILTER_DURATION_SECS.observe(start.elapsed().as_secs_f64());
    LIVE_TAIL_FILTERED_DOCS_MATCHED.inc_by(num_matching_docs as u64);
    LIVE_TAIL_FILTERED_DOCS_UNMATCHED.inc_by((num_docs - num_matching_docs) as u64);
    Ok(FilteredDocs {
        num_matching_docs,
        num_too_large_docs,
        docs_json,
    })
}

/// Returns the positions of the documents matching the query, in increasing order.
///
/// Documents rejected by the doc mapper are skipped: the indexing pipeline does not index them
/// either. The ingester only lets them through when document validation is disabled for the index.
fn matching_doc_positions(
    doc_mapper: &DocMapper,
    query_ast: &QueryAst,
    docs: &[&TailDoc],
) -> anyhow::Result<Vec<usize>> {
    let timestamp_field_opt: Option<Field> = doc_mapper
        .timestamp_field_name()
        .map(|timestamp_field_name| doc_mapper.schema().get_field(timestamp_field_name))
        .transpose()?;
    // `MatchAll` queries only need to know which documents are indexed by the pipeline.
    let mut index_writer_opt = if matches!(query_ast, QueryAst::MatchAll) {
        None
    } else {
        let index_writer = IndexBuilder::new()
            .schema(doc_mapper.schema())
            .tokenizers(doc_mapper.tokenizer_manager().tantivy_manager().clone())
            .fast_field_tokenizers(
                get_quickwit_fastfield_normalizer_manager()
                    .tantivy_manager()
                    .clone(),
            )
            .single_segment_index_writer::<TantivyDocument>(
                RamDirectory::default(),
                INDEX_WRITER_MEMORY_BUDGET_BYTES,
            )?;
        Some(index_writer)
    };
    // Position in `docs` of each indexed document. With a single segment and no doc ID mapping,
    // doc IDs are assigned in insertion order: the doc ID of a document is its index here.
    let mut doc_positions: Vec<usize> = Vec::with_capacity(docs.len());

    for (doc_position, tail_doc) in docs.iter().enumerate() {
        let Ok((_partition, tantivy_doc)) =
            doc_mapper.doc_from_json_obj(tail_doc.json_obj.clone(), tail_doc.num_bytes)
        else {
            continue;
        };
        // Like the `DocProcessor`, skip the documents without timestamp.
        if let Some(timestamp_field) = timestamp_field_opt
            && tantivy_doc
                .get_first(timestamp_field)
                .and_then(|value| value.as_datetime())
                .is_none()
        {
            continue;
        }
        // Documents are added one at a time, so that their Tantivy representation is not held
        // in memory for the whole batch.
        if let Some(index_writer) = &mut index_writer_opt {
            index_writer.add_document(tantivy_doc)?;
        }
        doc_positions.push(doc_position);
    }
    let Some(index_writer) = index_writer_opt else {
        return Ok(doc_positions);
    };
    let mut index = index_writer.finalize()?;
    set_expr_compilation_cache(&mut index);

    let searcher = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?
        .searcher();
    let (query, _warmup_info) = doc_mapper.query(index.schema(), query_ast.clone(), false, None)?;
    let mut matching_doc_positions: Vec<usize> = searcher
        .search(&query, &DocSetCollector)?
        .into_iter()
        .map(|doc_address| doc_positions[doc_address.doc_id as usize])
        .collect();
    matching_doc_positions.sort_unstable();
    Ok(matching_doc_positions)
}

/// Shares compiled expressions across the short-lived per-batch indexes, like the leaf search
/// does across splits.
fn set_expr_compilation_cache(index: &mut Index) {
    const EXPR_COMPILATION_CACHE_CAPACITY: usize = 256;
    static GLOBAL_EXPR_COMPILATION_CACHE: LazyLock<ExprCompilationCache> =
        LazyLock::new(|| ExprCompilationCache::with_capacity(EXPR_COMPILATION_CACHE_CAPACITY));
    index.set_expr_compilation_cache(GLOBAL_EXPR_COMPILATION_CACHE.clone());
}

#[cfg(test)]
mod tests {
    use quickwit_query::query_ast::{QueryAst, query_ast_from_user_text};
    use serde_json::json;

    use super::*;
    use crate::hub::doc_batch_for_test;

    const DOC_MAPPING_YAML: &str = r#"
        field_mappings:
          - name: timestamp
            type: datetime
            input_formats: [unix_timestamp]
            fast: true
          - name: severity_text
            type: text
            tokenizer: raw
          - name: body
            type: text
            tokenizer: default
            record: position
          - name: attributes
            type: json
            fast: true
        timestamp_field: timestamp
        mode: dynamic
    "#;

    fn tail_batch_with_doc_mapper(
        doc_mapper: DocMapper,
        docs: &[serde_json::Value],
    ) -> Arc<TailBatch> {
        Arc::new(TailBatch::new(
            Arc::new(doc_mapper),
            doc_batch_for_test(docs),
        ))
    }

    fn tail_batch(docs: Vec<serde_json::Value>) -> Arc<TailBatch> {
        let doc_mapper: DocMapper = serde_yaml::from_str(DOC_MAPPING_YAML).unwrap();
        tail_batch_with_doc_mapper(doc_mapper, &docs)
    }

    fn tail_batches() -> Vec<Arc<TailBatch>> {
        vec![
            tail_batch(vec![
                json!({"timestamp": 1_700_000_000, "severity_text": "INFO", "body": "User logged in", "attributes": {"user": "alice"}}),
                json!({"timestamp": 1_700_000_060, "severity_text": "ERROR", "body": "Connection REFUSED by upstream", "attributes": {"user": "bob", "status": 503}}),
            ]),
            tail_batch(vec![
                json!({"timestamp": 1_700_000_120, "severity_text": "ERROR", "body": "Disk full", "attributes": {"status": 507}, "host": "node-1"}),
            ]),
        ]
    }

    /// Returns the body of the matching documents.
    fn matching_bodies(user_query: &str) -> Vec<String> {
        let query_ast = query_ast_from_user_text(user_query, None)
            .parse_user_query(&["body".to_string()])
            .unwrap();
        filter_batches(&query_ast, &tail_batches(), usize::MAX, usize::MAX)
            .unwrap()
            .docs_json
            .into_iter()
            .map(|doc_json| {
                let doc: serde_json::Value = serde_json::from_slice(&doc_json).unwrap();
                doc["body"].as_str().unwrap().to_string()
            })
            .collect()
    }

    #[test]
    fn test_filter_batches_match_all() {
        let filtered_docs =
            filter_batches(&QueryAst::MatchAll, &tail_batches(), 10, usize::MAX).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 3);
        assert_eq!(filtered_docs.docs_json.len(), 3);

        let filtered_docs = filter_batches(&QueryAst::MatchAll, &[], 10, usize::MAX).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 0);
        assert!(filtered_docs.docs_json.is_empty());
    }

    #[test]
    fn test_filter_batches_serializes_at_most_max_num_docs() {
        let query_ast = query_ast_from_user_text("severity_text:ERROR", None)
            .parse_user_query(&[])
            .unwrap();
        let filtered_docs = filter_batches(&query_ast, &tail_batches(), 1, usize::MAX).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 2);
        assert_eq!(filtered_docs.docs_json.len(), 1);
        let doc: serde_json::Value = serde_json::from_slice(&filtered_docs.docs_json[0]).unwrap();
        assert_eq!(doc["body"], "Connection REFUSED by upstream");

        let filtered_docs = filter_batches(&query_ast, &tail_batches(), 0, usize::MAX).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 2);
        assert!(filtered_docs.docs_json.is_empty());
    }

    #[test]
    fn test_filter_batches_skips_too_large_docs() {
        let tail_batches = [tail_batch(vec![
            json!({"timestamp": 1_700_000_000, "body": "x".repeat(1_000)}),
            json!({"timestamp": 1_700_000_000, "body": "small"}),
        ])];
        // The too large document does not count toward `max_num_docs`.
        let filtered_docs = filter_batches(&QueryAst::MatchAll, &tail_batches, 1, 500).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 2);
        assert_eq!(filtered_docs.num_too_large_docs, 1);
        assert_eq!(filtered_docs.docs_json.len(), 1);
        let doc: serde_json::Value = serde_json::from_slice(&filtered_docs.docs_json[0]).unwrap();
        assert_eq!(doc["body"], "small");
    }

    #[test]
    fn test_filter_batches_term_on_raw_tokenizer() {
        assert_eq!(
            matching_bodies("severity_text:ERROR"),
            ["Connection REFUSED by upstream", "Disk full"]
        );
        // The raw tokenizer is case sensitive.
        assert!(matching_bodies("severity_text:error").is_empty());
    }

    #[test]
    fn test_filter_batches_default_field_and_tokenizer() {
        // The default tokenizer lowercases, and `body` is the default search field.
        assert_eq!(
            matching_bodies("refused"),
            ["Connection REFUSED by upstream"]
        );
        assert_eq!(
            matching_bodies("\"connection refused\""),
            ["Connection REFUSED by upstream"]
        );
    }

    #[test]
    fn test_filter_batches_json_and_dynamic_fields() {
        assert_eq!(
            matching_bodies("attributes.user:bob"),
            ["Connection REFUSED by upstream"]
        );
        assert_eq!(
            matching_bodies("attributes.status:[500 TO 505]"),
            ["Connection REFUSED by upstream"]
        );
        assert_eq!(matching_bodies("host:node-1"), ["Disk full"]);
    }

    #[test]
    fn test_filter_batches_boolean_and_timestamp_range() {
        assert_eq!(
            matching_bodies(
                "severity_text:ERROR AND timestamp:[2023-11-14T22:13:20Z TO 2023-11-14T22:14:30Z}"
            ),
            ["Connection REFUSED by upstream"]
        );
        assert_eq!(
            matching_bodies("NOT severity_text:ERROR"),
            ["User logged in"]
        );
    }

    #[test]
    fn test_filter_batches_uses_the_doc_mapper_of_each_batch() {
        let tail_batch = |doc_mapping_uid: &str, tokenizer: &str| {
            let doc_mapper: DocMapper = serde_yaml::from_str(&format!(
                r#"
                doc_mapping_uid: {doc_mapping_uid}
                field_mappings:
                  - name: severity_text
                    type: text
                    tokenizer: {tokenizer}
                "#
            ))
            .unwrap();
            tail_batch_with_doc_mapper(doc_mapper, &[json!({"severity_text": "ERROR"})])
        };
        // The index was updated: the tokenizer changed from `raw` to `default`.
        let tail_batches = [
            tail_batch("00000000000000000000000001", "raw"),
            tail_batch("00000000000000000000000002", "default"),
        ];
        let query_ast = query_ast_from_user_text("severity_text:error", None)
            .parse_user_query(&[])
            .unwrap();
        // Only the document tokenized with the `default` tokenizer, which lowercases, matches.
        let filtered_docs = filter_batches(&query_ast, &tail_batches, 10, usize::MAX).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 1);
    }

    #[test]
    fn test_filter_batches_skips_docs_rejected_by_the_doc_mapper() {
        // Document validation is disabled for the index: the ingester persisted a document
        // without timestamp, which the indexing pipeline does not index.
        let tail_batches = [tail_batch(vec![
            json!({"severity_text": "ERROR", "body": "no timestamp"}),
            json!({"timestamp": 1_700_000_000, "severity_text": "ERROR", "body": "Disk full"}),
        ])];
        let filtered_docs =
            filter_batches(&QueryAst::MatchAll, &tail_batches, 10, usize::MAX).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 1);

        let query_ast = query_ast_from_user_text("severity_text:ERROR", None)
            .parse_user_query(&[])
            .unwrap();
        let filtered_docs = filter_batches(&query_ast, &tail_batches, 10, usize::MAX).unwrap();
        assert_eq!(filtered_docs.num_matching_docs, 1);
        let doc: serde_json::Value = serde_json::from_slice(&filtered_docs.docs_json[0]).unwrap();
        assert_eq!(doc["body"], "Disk full");
    }
}
