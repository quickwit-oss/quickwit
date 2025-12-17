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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, Ok};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::search::{
    FetchDocsResponse, PartialHit, SnippetRequest, SplitIdAndFooterOffsets,
};
use quickwit_storage::Storage;
use tantivy::query::Query;
use tantivy::schema::document::CompactDocValue;
use tantivy::schema::{Document as DocumentTrait, Field, TantivyDocument, Value};
use tantivy::snippet::SnippetGenerator;
use tantivy::{ReloadPolicy, Score, Searcher, Term};
use tracing::{Instrument, error};

use crate::leaf::open_index_with_caches;
use crate::service::SearcherContext;
use crate::{GlobalDocAddress, convert_document_to_json_string};

const SNIPPET_MAX_NUM_CHARS: usize = 150;

/// Given a list of global doc address, fetches all the documents and
/// returns them as a hashmap.
async fn fetch_docs_to_map(
    searcher_context: Arc<SearcherContext>,
    mut global_doc_addrs: Vec<GlobalDocAddress>,
    index_storage: Arc<dyn Storage>,
    splits: &[SplitIdAndFooterOffsets],
    doc_mapper: Arc<DocMapper>,
    snippet_request_opt: Option<&SnippetRequest>,
) -> anyhow::Result<HashMap<GlobalDocAddress, Document>> {
    let mut split_fetch_docs_futures = Vec::new();

    let split_offsets_map: HashMap<&str, &SplitIdAndFooterOffsets> = splits
        .iter()
        .map(|split| (split.split_id.as_str(), split))
        .collect();

    // We sort global hit addrs in order to allow for the grouby.
    global_doc_addrs.sort_by(|a, b| a.split.cmp(&b.split));
    for (split_id, global_doc_addrs) in global_doc_addrs
        .iter()
        .chunk_by(|global_doc_addr| global_doc_addr.split.as_str())
        .into_iter()
    {
        let global_doc_addrs: Vec<GlobalDocAddress> =
            global_doc_addrs.into_iter().cloned().collect();
        let split_and_offset = split_offsets_map
            .get(split_id)
            .ok_or_else(|| anyhow::anyhow!("failed to find offset for split {}", split_id))?;
        split_fetch_docs_futures.push(fetch_docs_in_split(
            searcher_context.clone(),
            global_doc_addrs,
            index_storage.clone(),
            split_and_offset,
            doc_mapper.clone(),
            snippet_request_opt,
        ));
    }

    let split_fetch_docs: Vec<Vec<(GlobalDocAddress, Document)>> = futures::future::try_join_all(
        split_fetch_docs_futures,
    )
    .await
    .map_err(|error| {
        let split_ids = splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect_vec();
        error!(split_ids = ?split_ids, error = ?error, "error when fetching docs in splits");
        anyhow::anyhow!(
            "error when fetching docs for splits {:?}: {:?}",
            split_ids,
            error
        )
    })?;

    let global_doc_addr_to_doc_json: HashMap<GlobalDocAddress, Document> = split_fetch_docs
        .into_iter()
        .flat_map(|docs| docs.into_iter())
        .collect();

    Ok(global_doc_addr_to_doc_json)
}

/// `fetch_docs` step of search.
///
/// This function takes a list of partial hits (possibly from different splits)
/// and the storage associated to an index, fetches the document from
/// the split document stores, and returns the full hits.
pub async fn fetch_docs(
    searcher_context: Arc<SearcherContext>,
    partial_hits: Vec<PartialHit>,
    index_storage: Arc<dyn Storage>,
    splits: &[SplitIdAndFooterOffsets],
    doc_mapper: Arc<DocMapper>,
    snippet_request_opt: Option<&SnippetRequest>,
) -> anyhow::Result<FetchDocsResponse> {
    let global_doc_addrs: Vec<GlobalDocAddress> = partial_hits
        .iter()
        .map(GlobalDocAddress::from_partial_hit)
        .collect();

    let mut global_doc_addr_to_doc_json = fetch_docs_to_map(
        searcher_context,
        global_doc_addrs,
        index_storage,
        splits,
        doc_mapper,
        snippet_request_opt,
    )
    .await?;

    let hits: Vec<quickwit_proto::search::LeafHit> = partial_hits
        .into_iter()
        .flat_map(|partial_hit| {
            let global_doc_addr = GlobalDocAddress::from_partial_hit(&partial_hit);
            if let Some((_, document)) = global_doc_addr_to_doc_json.remove_entry(&global_doc_addr)
            {
                Some(quickwit_proto::search::LeafHit {
                    leaf_json: document.content_json,
                    partial_hit: Some(partial_hit),
                    leaf_snippet_json: document.snippet_json,
                })
            } else {
                None
            }
        })
        .collect();
    Ok(FetchDocsResponse { hits })
}

// number of concurrent fetch allowed for a single split.
const NUM_CONCURRENT_REQUESTS: usize = 30;

/// A struct for holding a fetched document's content and snippet.
#[derive(Debug)]
struct Document {
    content_json: String,
    snippet_json: Option<String>,
}

/// Fetching docs from a specific split.
async fn fetch_docs_in_split(
    searcher_context: Arc<SearcherContext>,
    mut global_doc_addrs: Vec<GlobalDocAddress>,
    index_storage: Arc<dyn Storage>,
    split: &SplitIdAndFooterOffsets,
    doc_mapper: Arc<DocMapper>,
    snippet_request_opt: Option<&SnippetRequest>,
) -> anyhow::Result<Vec<(GlobalDocAddress, Document)>> {
    global_doc_addrs.sort_by_key(|doc| doc.doc_addr);
    // Opens the index without the ephemeral unbounded cache, this cache is indeed not useful
    // when fetching docs as we will fetch them only once.
    let (mut index, _) = open_index_with_caches(
        &searcher_context,
        index_storage,
        split,
        Some(doc_mapper.tokenizer_manager()),
        None,
    )
    .await
    .context("open-index-for-split")?;
    // we add an executor here, we could add it in open_index_with_caches, though we should verify
    // the side-effect before
    let tantivy_executor = crate::search_thread_pool()
        .get_underlying_rayon_thread_pool()
        .into();
    index.set_executor(tantivy_executor);
    let index_reader = index
        .reader_builder()
        // the docs are presorted so a cache size of NUM_CONCURRENT_REQUESTS is fine
        .doc_store_cache_num_blocks(NUM_CONCURRENT_REQUESTS)
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = Arc::new(index_reader.searcher());
    let fields_snippet_generator_opt = if let Some(snippet_request) = snippet_request_opt {
        Some(create_fields_snippet_generator(&searcher, doc_mapper.clone(), snippet_request).await?)
    } else {
        None
    };

    let doc_futures = global_doc_addrs.into_iter().map(|global_doc_addr| {
        let moved_searcher = searcher.clone();
        let moved_doc_mapper = doc_mapper.clone();
        let fields_snippet_generator_opt_clone = fields_snippet_generator_opt.clone();
        async move {
            let doc: TantivyDocument = moved_searcher
                .doc_async(global_doc_addr.doc_addr)
                .await
                .context("searcher-doc-async")?;

            let named_field_doc = doc.to_named_doc(moved_searcher.schema());
            let content_json = convert_document_to_json_string(named_field_doc, &moved_doc_mapper)?;
            if fields_snippet_generator_opt_clone.is_none() {
                return Ok((
                    global_doc_addr,
                    Document {
                        content_json,
                        snippet_json: None,
                    },
                ));
            }

            let fields_snippet_generator_clone = fields_snippet_generator_opt_clone.unwrap();
            if fields_snippet_generator_clone.is_empty() {
                return Ok((
                    global_doc_addr,
                    Document {
                        content_json,
                        snippet_json: None,
                    },
                ));
            }

            let mut snippets = HashMap::new();
            for (field, field_values) in doc.get_sorted_field_values() {
                let field_name = moved_searcher.schema().get_field_name(field);
                if let Some(values) = fields_snippet_generator_clone
                    .snippets_from_field_values(field_name, field_values)
                {
                    snippets.insert(field_name, values);
                }
            }
            let snippet_json = serde_json::to_string(&snippets)?;
            Ok((
                global_doc_addr,
                Document {
                    content_json,
                    snippet_json: Some(snippet_json),
                },
            ))
        }
        .in_current_span()
    });

    futures::stream::iter(doc_futures)
        .buffer_unordered(NUM_CONCURRENT_REQUESTS)
        .try_collect::<Vec<_>>()
        .await
}

// A struct to hold the snippet generators associated to
// the snippet fields from a search request.
#[derive(Clone)]
struct FieldsSnippetGenerator {
    field_generators: Arc<HashMap<String, SnippetGenerator>>,
}

impl FieldsSnippetGenerator {
    // Returns the  snippets from fields values.
    fn snippets_from_field_values(
        &self,
        field_name: &str,
        field_values: Vec<CompactDocValue<'_>>,
    ) -> Option<Vec<String>> {
        if let Some(snippet_generator) = self.field_generators.get(field_name) {
            let values = field_values
                .into_iter()
                .filter_map(|value| {
                    value.as_str().and_then(|text| {
                        let snippet = snippet_generator.snippet(text);
                        match snippet.is_empty() {
                            false => Some(snippet.to_html()),
                            _ => None,
                        }
                    })
                })
                .collect();
            Some(values)
        } else {
            None
        }
    }

    fn is_empty(&self) -> bool {
        self.field_generators.is_empty()
    }
}

// Creates FieldsSnippetGenerator.
async fn create_fields_snippet_generator(
    searcher: &Searcher,
    doc_mapper: Arc<DocMapper>,
    snippet_request: &SnippetRequest,
) -> anyhow::Result<FieldsSnippetGenerator> {
    let schema = searcher.schema();
    let query_ast_resolved = serde_json::from_str(&snippet_request.query_ast_resolved)
        .context("failed to deserialize QueryAst")?;
    let (query, _) = doc_mapper.query(schema.clone(), query_ast_resolved, false, None)?;
    let mut snippet_generators = HashMap::new();
    for field_name in &snippet_request.snippet_fields {
        let field = schema.get_field(field_name)?;
        let snippet_generator = create_snippet_generator(searcher, &query, field).await?;
        snippet_generators.insert(field_name.clone(), snippet_generator);
    }

    Ok(FieldsSnippetGenerator {
        field_generators: Arc::new(snippet_generators),
    })
}

// Creates a snippet generator associated to a field.
async fn create_snippet_generator(
    searcher: &Searcher,
    query: &dyn Query,
    field: Field,
) -> anyhow::Result<SnippetGenerator> {
    let mut terms: Vec<&Term> = Vec::new();
    // TODO ok with termset?
    query.query_terms(&mut |term, _need_position| {
        if term.field() == field {
            terms.push(term);
        }
    });
    let mut terms_text: BTreeMap<String, f32> = BTreeMap::default();
    for term in terms {
        let value = term.value();
        let Some(term_str) = value.as_str() else {
            continue;
        };
        let doc_freq = searcher.doc_freq_async(term).await?;
        if doc_freq > 0 {
            let score = 1.0 / (1.0 + doc_freq as Score);
            terms_text.insert(term_str.to_string(), score);
        }
    }
    let tokenizer = searcher.index().tokenizer_for_field(field)?;
    Ok(SnippetGenerator::new(
        terms_text,
        tokenizer,
        field,
        SNIPPET_MAX_NUM_CHARS,
    ))
}
