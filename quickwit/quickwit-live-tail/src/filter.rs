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
use tantivy::{Index, IndexBuilder, ReloadPolicy, TantivyDocument};

use crate::hub::{TailBatch, TailDoc};
use crate::metrics::{
    LIVE_TAIL_FILTER_DURATION_SECS, LIVE_TAIL_FILTERED_DOCS_MATCHED,
    LIVE_TAIL_FILTERED_DOCS_UNMATCHED,
};

/// Memory budget of the in-RAM index built for each batch. Same as the indexer's split writer.
const INDEX_WRITER_MEMORY_BUDGET_BYTES: usize = 15_000_000;

/// Returns the documents of `tail_batches` matching `query_ast`, serialized as JSON, in processing
/// order.
///
/// The query is evaluated with the same semantics as a search: the documents are indexed into a
/// single-segment in-RAM Tantivy index built with the doc mapper of their pipeline, and the query
/// is built with `DocMapper::query`, exactly like a leaf search does on a split.
///
/// Precondition: `query_ast` must be resolved, i.e. it must not contain `UserInput` nodes.
///
/// This is CPU-intensive: call it from `run_cpu_intensive`.
pub(crate) fn filter_batches(
    query_ast: &QueryAst,
    tail_batches: &[Arc<TailBatch>],
) -> anyhow::Result<Vec<Vec<u8>>> {
    let start = Instant::now();
    let mut num_docs = 0;
    let mut matching_docs_json: Vec<Vec<u8>> = Vec::new();

    // Batches processed with different doc mappings, e.g. right after an index update, are
    // filtered separately.
    for same_mapping_batches in tail_batches.chunk_by(|left, right| {
        left.doc_mapper.doc_mapping_uid() == right.doc_mapper.doc_mapping_uid()
    }) {
        let doc_mapper = &same_mapping_batches[0].doc_mapper;
        let docs: Vec<&TailDoc> = same_mapping_batches
            .iter()
            .flat_map(|tail_batch| tail_batch.docs.iter())
            .collect();
        num_docs += docs.len();

        for doc_position in matching_doc_positions(doc_mapper, query_ast, &docs)? {
            matching_docs_json.push(serde_json::to_vec(&docs[doc_position].json_obj)?);
        }
    }
    LIVE_TAIL_FILTER_DURATION_SECS.observe(start.elapsed().as_secs_f64());
    LIVE_TAIL_FILTERED_DOCS_MATCHED.inc_by(matching_docs_json.len() as u64);
    LIVE_TAIL_FILTERED_DOCS_UNMATCHED.inc_by((num_docs - matching_docs_json.len()) as u64);
    Ok(matching_docs_json)
}

/// Returns the positions of the documents matching the query, in increasing order.
///
/// Precondition: `docs` were successfully mapped by `doc_mapper` in the indexing pipeline.
fn matching_doc_positions(
    doc_mapper: &DocMapper,
    query_ast: &QueryAst,
    docs: &[&TailDoc],
) -> anyhow::Result<Vec<usize>> {
    if matches!(query_ast, QueryAst::MatchAll) {
        return Ok((0..docs.len()).collect());
    }
    let mut index_writer = IndexBuilder::new()
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
    // With a single segment and no doc ID mapping, doc IDs are assigned in insertion order: the
    // doc ID of a document is its position in `docs`.
    for tail_doc in docs {
        let (_partition, doc) =
            doc_mapper.doc_from_json_obj(tail_doc.json_obj.clone(), tail_doc.num_bytes)?;
        index_writer.add_document(doc)?;
    }
    let mut index = index_writer.finalize()?;
    set_expr_compilation_cache(&mut index);

    let searcher = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?
        .searcher();
    let (query, _warmup_info) = doc_mapper.query(index.schema(), query_ast.clone(), false, None)?;
    let mut doc_positions: Vec<usize> = searcher
        .search(&query, &DocSetCollector)?
        .into_iter()
        .map(|doc_address| doc_address.doc_id as usize)
        .collect();
    doc_positions.sort_unstable();
    Ok(doc_positions)
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

    fn tail_batch(docs: Vec<serde_json::Value>) -> Arc<TailBatch> {
        let doc_mapper: DocMapper = serde_yaml::from_str(DOC_MAPPING_YAML).unwrap();
        let docs = docs
            .into_iter()
            .map(|doc| {
                let serde_json::Value::Object(json_obj) = doc else {
                    unreachable!()
                };
                TailDoc {
                    json_obj,
                    num_bytes: 100,
                }
            })
            .collect();
        Arc::new(TailBatch {
            doc_mapper: Arc::new(doc_mapper),
            docs,
        })
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
        filter_batches(&query_ast, &tail_batches())
            .unwrap()
            .into_iter()
            .map(|doc_json| {
                let doc: serde_json::Value = serde_json::from_slice(&doc_json).unwrap();
                doc["body"].as_str().unwrap().to_string()
            })
            .collect()
    }

    #[test]
    fn test_filter_batches_match_all() {
        let matching_docs = filter_batches(&QueryAst::MatchAll, &tail_batches()).unwrap();
        assert_eq!(matching_docs.len(), 3);
        assert!(filter_batches(&QueryAst::MatchAll, &[]).unwrap().is_empty());
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
            let serde_json::Value::Object(json_obj) = json!({"severity_text": "ERROR"}) else {
                unreachable!()
            };
            Arc::new(TailBatch {
                doc_mapper: Arc::new(doc_mapper),
                docs: vec![TailDoc {
                    json_obj,
                    num_bytes: 100,
                }],
            })
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
        let matching_docs = filter_batches(&query_ast, &tail_batches).unwrap();
        assert_eq!(matching_docs.len(), 1);
    }
}
