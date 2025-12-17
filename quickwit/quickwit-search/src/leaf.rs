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

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::Context;
use bytesize::ByteSize;
use futures::future::try_join_all;
use quickwit_common::pretty::PrettySample;
use quickwit_directories::{CachingDirectory, HotDirectory, StorageDirectory};
use quickwit_doc_mapper::{Automaton, DocMapper, FastFieldWarmupInfo, TermRange, WarmupInfo};
use quickwit_proto::search::{
    CountHits, LeafSearchRequest, LeafSearchResponse, PartialHit, ResourceStats, SearchRequest,
    SortOrder, SortValue, SplitIdAndFooterOffsets, SplitSearchError,
};
use quickwit_query::query_ast::{
    BoolQuery, CacheNode, QueryAst, QueryAstTransformer, RangeQuery, TermQuery,
};
use quickwit_query::tokenizers::TokenizerManager;
use quickwit_storage::{
    BundleStorage, ByteRangeCache, MemorySizedCache, OwnedBytes, SplitCache, Storage,
    StorageResolver, TimeoutAndRetryStorage, wrap_storage_with_cache,
};
use tantivy::aggregation::agg_req::{AggregationVariants, Aggregations};
use tantivy::aggregation::{AggContextParams, AggregationLimitsGuard};
use tantivy::collector::Collector;
use tantivy::directory::FileSlice;
use tantivy::fastfield::FastFieldReaders;
use tantivy::schema::Field;
use tantivy::{DateTime, Index, ReloadPolicy, Searcher, TantivyError, Term};
use tokio::task::JoinError;
use tracing::*;

use crate::collector::{IncrementalCollector, make_collector_for_split, make_merge_collector};
use crate::metrics::SplitSearchOutcomeCounters;
use crate::root::is_metadata_count_request_with_ast;
use crate::search_permit_provider::{SearchPermit, compute_initial_memory_allocation};
use crate::service::{SearcherContext, deserialize_doc_mapper};
use crate::{QuickwitAggregations, SearchError};

async fn get_split_footer_from_cache_or_fetch(
    index_storage: Arc<dyn Storage>,
    split_and_footer_offsets: &SplitIdAndFooterOffsets,
    footer_cache: &MemorySizedCache<String>,
) -> anyhow::Result<OwnedBytes> {
    {
        let possible_val = footer_cache.get(&split_and_footer_offsets.split_id);
        if let Some(footer_data) = possible_val {
            return Ok(footer_data);
        }
    }
    let split_file = PathBuf::from(format!("{}.split", split_and_footer_offsets.split_id));
    let footer_data_opt = index_storage
        .get_slice(
            &split_file,
            split_and_footer_offsets.split_footer_start as usize
                ..split_and_footer_offsets.split_footer_end as usize,
        )
        .await
        .with_context(|| {
            format!(
                "failed to fetch hotcache and footer from {} for split `{}`",
                index_storage.uri(),
                split_and_footer_offsets.split_id
            )
        })?;

    footer_cache.put(
        split_and_footer_offsets.split_id.to_owned(),
        footer_data_opt.clone(),
    );

    Ok(footer_data_opt)
}

/// Returns hotcache_bytes and the split directory (`BundleStorage`) with cache layer:
/// - A split footer cache given by `SearcherContext.split_footer_cache`.
pub(crate) async fn open_split_bundle(
    searcher_context: &SearcherContext,
    index_storage: Arc<dyn Storage>,
    split_and_footer_offsets: &SplitIdAndFooterOffsets,
) -> anyhow::Result<(FileSlice, BundleStorage)> {
    let split_file = PathBuf::from(format!("{}.split", split_and_footer_offsets.split_id));
    let footer_data = get_split_footer_from_cache_or_fetch(
        index_storage.clone(),
        split_and_footer_offsets,
        &searcher_context.split_footer_cache,
    )
    .await?;

    // We wrap the top-level storage with the split cache.
    // This is before the bundle storage: at this point, this storage is reading `.split` files.
    let index_storage_with_split_cache =
        if let Some(split_cache) = searcher_context.split_cache_opt.as_ref() {
            SplitCache::wrap_storage(split_cache.clone(), index_storage.clone())
        } else {
            index_storage.clone()
        };

    let (hotcache_bytes, bundle_storage) = BundleStorage::open_from_split_data(
        index_storage_with_split_cache,
        split_file,
        FileSlice::new(Arc::new(footer_data)),
    )?;

    Ok((hotcache_bytes, bundle_storage))
}

/// Add a storage proxy to retry `get_slice` requests if they are taking too long,
/// if configured in the searcher config.
///
/// The goal here is too ensure a low latency.
fn configure_storage_retries(
    searcher_context: &SearcherContext,
    index_storage: Arc<dyn Storage>,
) -> Arc<dyn Storage> {
    if let Some(storage_timeout_policy) = &searcher_context.searcher_config.storage_timeout_policy {
        Arc::new(TimeoutAndRetryStorage::new(
            index_storage,
            storage_timeout_policy.clone(),
        ))
    } else {
        index_storage
    }
}

/// Opens a `tantivy::Index` for the given split with several cache layers:
/// - A split footer cache given by `SearcherContext.split_footer_cache`.
/// - A fast fields cache given by `SearcherContext.storage_long_term_cache`.
/// - An ephemeral unbounded cache directory (whose lifetime is tied to the returned `Index` if no
///   `ByteRangeCache` is provided).
pub(crate) async fn open_index_with_caches(
    searcher_context: &SearcherContext,
    index_storage: Arc<dyn Storage>,
    split_and_footer_offsets: &SplitIdAndFooterOffsets,
    tokenizer_manager: Option<&TokenizerManager>,
    ephemeral_unbounded_cache: Option<ByteRangeCache>,
) -> anyhow::Result<(Index, HotDirectory)> {
    let index_storage_with_retry_on_timeout =
        configure_storage_retries(searcher_context, index_storage);

    let (hotcache_bytes, bundle_storage) = open_split_bundle(
        searcher_context,
        index_storage_with_retry_on_timeout,
        split_and_footer_offsets,
    )
    .await?;

    let bundle_storage_with_cache = wrap_storage_with_cache(
        searcher_context.fast_fields_cache.clone(),
        Arc::new(bundle_storage),
    );

    let directory = StorageDirectory::new(bundle_storage_with_cache);

    let hot_directory = if let Some(cache) = ephemeral_unbounded_cache {
        let caching_directory = CachingDirectory::new(Arc::new(directory), cache);
        HotDirectory::open(caching_directory, hotcache_bytes.read_bytes()?)?
    } else {
        HotDirectory::open(directory, hotcache_bytes.read_bytes()?)?
    };

    let mut index = Index::open(hot_directory.clone())?;
    if let Some(tokenizer_manager) = tokenizer_manager {
        index.set_tokenizers(tokenizer_manager.tantivy_manager().clone());
    }
    index.set_fast_field_tokenizers(
        quickwit_query::get_quickwit_fastfield_normalizer_manager()
            .tantivy_manager()
            .clone(),
    );
    Ok((index, hot_directory))
}

/// Tantivy search does not make it possible to fetch data asynchronously during
/// search.
///
/// It is required to download all required information in advance.
/// This is the role of the `warmup` function.
///
/// The downloaded data depends on the query (which term's posting list is required,
/// are position required too), and the collector.
///
/// * `query` - query is used to extract the terms and their fields which will be loaded from the
/// inverted_index.
///
/// * `term_dict_field_names` - A list of fields, where the whole dictionary needs to be loaded.
/// This is e.g. required for term aggregation, since we don't know in advance which terms are going
/// to be hit.
#[instrument(skip_all)]
pub(crate) async fn warmup(searcher: &Searcher, warmup_info: &WarmupInfo) -> anyhow::Result<()> {
    debug!(warmup_info=?warmup_info);
    let warm_up_terms_future = warm_up_terms(searcher, &warmup_info.terms_grouped_by_field)
        .instrument(debug_span!("warm_up_terms"));
    let warm_up_term_ranges_future =
        warm_up_term_ranges(searcher, &warmup_info.term_ranges_grouped_by_field)
            .instrument(debug_span!("warm_up_term_ranges"));
    let warm_up_term_dict_future =
        warm_up_term_dict_fields(searcher, &warmup_info.term_dict_fields)
            .instrument(debug_span!("warm_up_term_dicts"));
    let warm_up_fastfields_future = warm_up_fastfields(searcher, &warmup_info.fast_fields)
        .instrument(debug_span!("warm_up_fastfields"));
    let warm_up_fieldnorms_future = warm_up_fieldnorms(searcher, warmup_info.field_norms)
        .instrument(debug_span!("warm_up_fieldnorms"));
    // TODO merge warm_up_postings into warm_up_term_dict_fields
    let warm_up_postings_future = warm_up_postings(searcher, &warmup_info.term_dict_fields)
        .instrument(debug_span!("warm_up_postings"));
    let warm_up_automatons_future =
        warm_up_automatons(searcher, &warmup_info.automatons_grouped_by_field)
            .instrument(debug_span!("warm_up_automatons"));

    tokio::try_join!(
        warm_up_terms_future,
        warm_up_term_ranges_future,
        warm_up_fastfields_future,
        warm_up_term_dict_future,
        warm_up_fieldnorms_future,
        warm_up_postings_future,
        warm_up_automatons_future,
    )?;

    Ok(())
}

async fn warm_up_term_dict_fields(
    searcher: &Searcher,
    term_dict_fields: &HashSet<Field>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for field in term_dict_fields {
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(*field)?.clone();
            warm_up_futures.push(async move {
                let dict = inverted_index.terms();
                dict.warm_up_dictionary().await
            });
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_postings(searcher: &Searcher, fields: &HashSet<Field>) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for field in fields {
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(*field)?.clone();
            warm_up_futures.push(async move { inverted_index.warm_postings_full(false).await });
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_fastfield(
    fast_field_reader: &FastFieldReaders,
    fast_field: &FastFieldWarmupInfo,
) -> anyhow::Result<()> {
    let mut columns = fast_field_reader
        .list_dynamic_column_handles(&fast_field.name)
        .await?;
    if fast_field.with_subfields {
        let subpath_columns = fast_field_reader
            .list_subpath_dynamic_column_handles(&fast_field.name)
            .await?;
        columns.extend(subpath_columns);
    }
    futures::future::try_join_all(
        columns
            .into_iter()
            .map(|col| async move { col.file_slice().read_bytes_async().await }),
    )
    .await?;
    Ok(())
}

/// Populates the short-lived cache with the data for
/// all of the fast fields passed as argument.
async fn warm_up_fastfields(
    searcher: &Searcher,
    fast_fields: &HashSet<FastFieldWarmupInfo>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for segment_reader in searcher.segment_readers() {
        let fast_field_reader = segment_reader.fast_fields();
        for fast_field in fast_fields {
            let warm_up_fut = warm_up_fastfield(fast_field_reader, fast_field);
            warm_up_futures.push(Box::pin(warm_up_fut));
        }
    }
    futures::future::try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_terms(
    searcher: &Searcher,
    terms_grouped_by_field: &HashMap<Field, HashMap<Term, bool>>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for (field, terms) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(*field)?;
            for (term, position_needed) in terms.iter() {
                let inv_idx_clone = inv_idx.clone();
                warm_up_futures
                    .push(async move { inv_idx_clone.warm_postings(term, *position_needed).await });
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_term_ranges(
    searcher: &Searcher,
    terms_grouped_by_field: &HashMap<Field, HashMap<TermRange, bool>>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for (field, terms) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(*field)?;
            for (term_range, position_needed) in terms.iter() {
                let inv_idx_clone = inv_idx.clone();
                let range = (term_range.start.as_ref(), term_range.end.as_ref());
                warm_up_futures.push(async move {
                    inv_idx_clone
                        .warm_postings_range(range, term_range.limit, *position_needed)
                        .await
                });
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_automatons(
    searcher: &Searcher,
    terms_grouped_by_field: &HashMap<Field, HashSet<Automaton>>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    let cpu_intensive_executor = |task| async {
        crate::search_thread_pool()
            .run_cpu_intensive(task)
            .await
            .map_err(|_| std::io::Error::other("task panicked"))?
    };
    for (field, automatons) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(*field)?;
            for automaton in automatons {
                let inv_idx_clone = inv_idx.clone();
                warm_up_futures.push(async move {
                    match automaton {
                        Automaton::Regex(path, regex_str) => {
                            let regex = tantivy_fst::Regex::new(regex_str)
                                .context("failed to parse regex during warmup")?;
                            inv_idx_clone
                                .warm_postings_automaton(
                                    quickwit_query::query_ast::JsonPathPrefix {
                                        automaton: regex.into(),
                                        prefix: path.clone().unwrap_or_default(),
                                    },
                                    cpu_intensive_executor,
                                )
                                .await
                                .context("failed to load automaton")
                        }
                    }
                });
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_fieldnorms(searcher: &Searcher, requires_scoring: bool) -> anyhow::Result<()> {
    if !requires_scoring {
        return Ok(());
    }
    let mut warm_up_futures = Vec::new();
    for field in searcher.schema().fields() {
        for segment_reader in searcher.segment_readers() {
            let fieldnorm_readers = segment_reader.fieldnorms_readers();
            let file_handle_opt = fieldnorm_readers.get_inner_file().open_read(field.0);
            if let Some(file_handle) = file_handle_opt {
                warm_up_futures.push(async move { file_handle.read_bytes_async().await })
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

fn get_leaf_resp_from_count(count: u64) -> LeafSearchResponse {
    LeafSearchResponse {
        num_hits: count,
        partial_hits: Vec::new(),
        failed_splits: Vec::new(),
        num_attempted_splits: 1,
        num_successful_splits: 1,
        intermediate_aggregation_result: None,
        resource_stats: None,
    }
}

/// Compute the size of the index, store excluded.
fn compute_index_size(hot_directory: &HotDirectory) -> ByteSize {
    let size_bytes = hot_directory
        .get_file_lengths()
        .iter()
        .filter(|(path, _)| !path.to_string_lossy().ends_with("store"))
        .map(|(_, size)| *size)
        .sum();
    ByteSize(size_bytes)
}

/// Apply a leaf search on a single split.
#[allow(clippy::too_many_arguments)]
async fn leaf_search_single_split(
    mut search_request: SearchRequest,
    ctx: Arc<LeafSearchContext>,
    storage: Arc<dyn Storage>,
    split: SplitIdAndFooterOffsets,
    aggregations_limits: AggregationLimitsGuard,
    search_permit: &mut SearchPermit,
) -> crate::Result<Option<LeafSearchResponse>> {
    let mut leaf_search_state_guard =
        SplitSearchStateGuard::new(ctx.split_outcome_counters.clone());

    rewrite_request(
        &mut search_request,
        &split,
        ctx.doc_mapper.timestamp_field_name(),
    );
    if let Some(cached_answer) = ctx
        .searcher_context
        .leaf_search_cache
        .get(split.clone(), search_request.clone())
    {
        leaf_search_state_guard.set_state(SplitSearchState::CacheHit);
        return Ok(Some(cached_answer));
    }

    let query_ast: QueryAst = serde_json::from_str(search_request.query_ast.as_str())
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;

    // CanSplitDoBetter or rewrite_request may have changed the request to be a count only request
    // This may be the case for AllQuery with a sort by date and time filter, where the current
    // split can't have better results.
    //
    if is_metadata_count_request_with_ast(&query_ast, &search_request) {
        leaf_search_state_guard.set_state(SplitSearchState::PrunedBeforeWarmup);
        return Ok(Some(get_leaf_resp_from_count(split.num_docs)));
    }

    let split_id = split.split_id.to_string();
    let byte_range_cache =
        ByteRangeCache::with_infinite_capacity(&quickwit_storage::STORAGE_METRICS.shortlived_cache);
    let (index, hot_directory) = open_index_with_caches(
        &ctx.searcher_context,
        storage,
        &split,
        Some(ctx.doc_mapper.tokenizer_manager()),
        Some(byte_range_cache.clone()),
    )
    .await?;

    let index_size = compute_index_size(&hot_directory);
    if index_size < search_permit.memory_allocation() {
        search_permit.update_memory_usage(index_size);
    }

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();

    let agg_context_params = AggContextParams {
        limits: aggregations_limits,
        tokenizers: ctx.doc_mapper.tokenizer_manager().tantivy_manager().clone(),
    };
    let mut collector =
        make_collector_for_split(split_id.clone(), &search_request, agg_context_params)?;

    let predicate_cache = if collector.requires_scoring() {
        // at the moment the predicate cache doesn't support scoring
        None
    } else {
        Some((
            ctx.searcher_context.predicate_cache.clone() as _,
            split.split_id.clone(),
        ))
    };
    let split_schema = index.schema();
    let (query, mut warmup_info) = ctx.doc_mapper.query(
        split_schema.clone(),
        query_ast.clone(),
        false,
        predicate_cache,
    )?;

    let collector_warmup_info = collector.warmup_info();
    warmup_info.merge(collector_warmup_info);
    warmup_info.simplify();

    let warmup_start = Instant::now();
    leaf_search_state_guard.set_state(SplitSearchState::WarmUp);
    warmup(&searcher, &warmup_info).await?;
    let warmup_end = Instant::now();
    let warmup_duration: Duration = warmup_end.duration_since(warmup_start);
    let warmup_size = ByteSize(byte_range_cache.get_num_bytes());
    if warmup_size > search_permit.memory_allocation() {
        warn!(
            memory_usage = ?warmup_size,
            memory_allocation = ?search_permit.memory_allocation(),
            "current leaf search is consuming more memory than the initial allocation"
        );
    }
    crate::SEARCH_METRICS
        .leaf_search_single_split_warmup_num_bytes
        .observe(warmup_size.as_u64() as f64);
    search_permit.update_memory_usage(warmup_size);
    search_permit.free_warmup_slot();

    let split_num_docs = split.num_docs;

    let span = info_span!("tantivy_search");

    let split_clone = split.clone();

    let ctx_clone = ctx.clone();
    leaf_search_state_guard.set_state(SplitSearchState::CpuQueue);
    let search_request_and_result: Option<(SearchRequest, LeafSearchResponse)> =
        crate::search_thread_pool()
            .run_cpu_intensive(move || {
                leaf_search_state_guard.set_state(SplitSearchState::Cpu);
                let cpu_start = Instant::now();
                let cpu_thread_pool_wait_microsecs = cpu_start.duration_since(warmup_end);
                let _span_guard = span.enter();
                // Our search execution has been scheduled, let's check if we can improve the
                // request based on the results of the preceding searches
                let Some(simplified_search_request) =
                    simplify_search_request(search_request, &split_clone, &ctx_clone.split_filter)
                else {
                    leaf_search_state_guard.set_state(SplitSearchState::PrunedAfterWarmup);
                    return Ok(None);
                };
                collector.update_search_param(&simplified_search_request);
                let mut leaf_search_response: LeafSearchResponse =
                    if is_metadata_count_request_with_ast(&query_ast, &simplified_search_request) {
                        get_leaf_resp_from_count(searcher.num_docs())
                    } else if collector.is_count_only() {
                        let count = query.count(&searcher)? as u64;
                        get_leaf_resp_from_count(count)
                    } else {
                        searcher.search(&query, &collector)?
                    };
                leaf_search_response.resource_stats = Some(ResourceStats {
                    cpu_microsecs: cpu_start.elapsed().as_micros() as u64,
                    short_lived_cache_num_bytes: warmup_size.as_u64(),
                    split_num_docs,
                    warmup_microsecs: warmup_duration.as_micros() as u64,
                    cpu_thread_pool_wait_microsecs: cpu_thread_pool_wait_microsecs.as_micros()
                        as u64,
                });
                leaf_search_state_guard.set_state(SplitSearchState::Success);
                Result::<_, TantivyError>::Ok(Some((
                    simplified_search_request,
                    leaf_search_response,
                )))
            })
            .await
            .map_err(|_| {
                crate::SearchError::Internal(format!("leaf search panicked. split={split_id}"))
            })??;

    // Let's cache this result in the partial result cache.
    if let Some((leaf_search_req, leaf_search_resp)) = search_request_and_result {
        ctx.searcher_context.leaf_search_cache.put(
            split,
            leaf_search_req,
            leaf_search_resp.clone(),
        );
        Ok(Some(leaf_search_resp))
    } else {
        Ok(None)
    }
}

/// Rewrite a request removing parts which incur additional download or computation with no
/// effect.
///
/// This include things such as sorting result by a field or _score when no document is requested,
/// or applying date range when the range covers the entire split.
fn rewrite_request(
    search_request: &mut SearchRequest,
    split: &SplitIdAndFooterOffsets,
    timestamp_field: Option<&str>,
) {
    if search_request.max_hits == 0 {
        search_request.sort_fields = Vec::new();
    }
    if let Some(timestamp_field) = timestamp_field {
        remove_redundant_timestamp_range(search_request, split, timestamp_field);
    }
    rewrite_aggregation(search_request);
    // we add a top level cache node when search_after is set, this won't help for this query (which
    // is the 2nd in its series), but should speedup every other request that comes after
    if search_request.search_after.is_some() {
        add_top_cache_node(search_request)
    }
}

fn add_top_cache_node(search_request: &mut SearchRequest) {
    let Ok(query_ast) = serde_json::from_str(search_request.query_ast.as_str()) else {
        // an error will get raised a bit after anyway
        return;
    };
    let new_ast: QueryAst = CacheNode::new(query_ast).into();
    search_request.query_ast = serde_json::to_string(&new_ast).unwrap();
}

/// Rewrite aggregation to make them easier to cache
///
/// This is only valid for options which are handled while merging results, which is
/// mostly `extended_bounds`.
fn rewrite_aggregation(search_request: &mut SearchRequest) {
    if let Some(aggregation) = &search_request.aggregation_request {
        let Ok(QuickwitAggregations::TantivyAggregations(mut aggregations)) =
            serde_json::from_str(aggregation)
        else {
            return;
        };
        let modified_something = visit_aggregation_mut(&mut aggregations, &|aggregation_variant| {
            match aggregation_variant {
                // we take() away the extended bounds, and record we did something
                AggregationVariants::Histogram(histogram) => {
                    histogram.extended_bounds.take().is_some()
                }
                AggregationVariants::DateHistogram(histogram) => {
                    histogram.extended_bounds.take().is_some()
                }
                _ => false,
            }
        });
        if modified_something {
            // it's fine to put a (Tantivy)Aggregations and not a QuickwitAggregations because
            // the former is an serde-untagged variant of the later
            search_request.aggregation_request =
                Some(serde_json::to_string(&aggregations).expect("serializing should never fail"));
        }
    }
}

// this is a rather limited visitor, but enough to do the job
fn visit_aggregation_mut(
    aggregations: &mut Aggregations,
    callback: &impl Fn(&mut AggregationVariants) -> bool,
) -> bool {
    let mut modified_something = false;
    for aggregation in aggregations.values_mut() {
        modified_something |= callback(&mut aggregation.agg);
        modified_something |= visit_aggregation_mut(&mut aggregation.sub_aggregation, callback);
    }
    modified_something
}

// equivalent to Bound::map, which is unstable
pub fn map_bound<T, U>(bound: Bound<T>, f: impl FnOnce(T) -> U) -> Bound<U> {
    use Bound::*;
    match bound {
        Unbounded => Unbounded,
        Included(x) => Included(f(x)),
        Excluded(x) => Excluded(f(x)),
    }
}

// returns the max of left and right, that isn't unbounded. Useful for making
// the intersection of lower bound of ranges
fn max_bound<T: Ord + Copy>(left: Bound<T>, right: Bound<T>) -> Bound<T> {
    use Bound::*;
    match (left, right) {
        (Unbounded, right) => right,
        (left, Unbounded) => left,
        (Included(left), Included(right)) => Included(left.max(right)),
        (Excluded(left), Excluded(right)) => Excluded(left.max(right)),
        (excluded_total @ Excluded(excluded), included_total @ Included(included)) => {
            if included > excluded {
                included_total
            } else {
                excluded_total
            }
        }
        (included_total @ Included(included), excluded_total @ Excluded(excluded)) => {
            if included > excluded {
                included_total
            } else {
                excluded_total
            }
        }
    }
}

// returns the min of left and right, that isn't unbounded. Useful for making
// the intersection of upper bound of ranges
fn min_bound<T: Ord + Copy>(left: Bound<T>, right: Bound<T>) -> Bound<T> {
    use Bound::*;
    match (left, right) {
        (Unbounded, right) => right,
        (left, Unbounded) => left,
        (Included(left), Included(right)) => Included(left.min(right)),
        (Excluded(left), Excluded(right)) => Excluded(left.min(right)),
        (excluded_total @ Excluded(excluded), included_total @ Included(included)) => {
            if included < excluded {
                included_total
            } else {
                excluded_total
            }
        }
        (included_total @ Included(included), excluded_total @ Excluded(excluded)) => {
            if included < excluded {
                included_total
            } else {
                excluded_total
            }
        }
    }
}

/// remove timestamp range that would be present both in QueryAst and SearchRequest
///
/// this can save us from doing double the work in some cases, and help with the partial request
/// cache.
fn remove_redundant_timestamp_range(
    search_request: &mut SearchRequest,
    split: &SplitIdAndFooterOffsets,
    timestamp_field: &str,
) {
    let Ok(query_ast) = serde_json::from_str(search_request.query_ast.as_str()) else {
        // an error will get raised a bit after anyway
        return;
    };

    let start_timestamp = search_request
        .start_timestamp
        .map(DateTime::from_timestamp_secs)
        .map(Bound::Included)
        .unwrap_or(Bound::Unbounded);
    let end_timestamp = search_request
        .end_timestamp
        .map(DateTime::from_timestamp_secs)
        .map(Bound::Excluded)
        .unwrap_or(Bound::Unbounded);

    let mut visitor = RemoveTimestampRange {
        timestamp_field,
        start_timestamp,
        end_timestamp,
    };
    let mut new_ast = visitor
        .transform(query_ast)
        .expect("can't fail unwrapping Infallible")
        .unwrap_or(QueryAst::MatchAll);

    let final_start_timestamp = match (
        visitor.start_timestamp,
        split.timestamp_start.map(DateTime::from_timestamp_secs),
    ) {
        (Bound::Included(query_ts), Some(split_ts)) => {
            if query_ts > split_ts {
                Bound::Included(query_ts)
            } else {
                Bound::Unbounded
            }
        }
        (Bound::Excluded(query_ts), Some(split_ts)) => {
            if query_ts >= split_ts {
                Bound::Excluded(query_ts)
            } else {
                Bound::Unbounded
            }
        }
        (Bound::Unbounded, Some(_)) => Bound::Unbounded,
        (timestamp, None) => timestamp,
    };
    let final_end_timestamp = match (
        visitor.end_timestamp,
        split.timestamp_end.map(DateTime::from_timestamp_secs),
    ) {
        (Bound::Included(query_ts), Some(split_ts)) => {
            if query_ts < split_ts {
                Bound::Included(query_ts)
            } else {
                Bound::Unbounded
            }
        }
        (Bound::Excluded(query_ts), Some(split_ts)) => {
            if query_ts <= split_ts {
                Bound::Excluded(query_ts)
            } else {
                Bound::Unbounded
            }
        }
        (Bound::Unbounded, Some(_)) => Bound::Unbounded,
        (timestamp, None) => timestamp,
    };
    if final_start_timestamp != Bound::Unbounded || final_end_timestamp != Bound::Unbounded {
        let range = RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: map_bound(final_start_timestamp, |bound| {
                bound.into_timestamp_nanos().into()
            }),
            upper_bound: map_bound(final_end_timestamp, |bound| {
                bound.into_timestamp_nanos().into()
            }),
        };
        new_ast = if let QueryAst::Bool(mut bool_query) = new_ast {
            if bool_query.must.is_empty()
                && bool_query.filter.is_empty()
                && !bool_query.should.is_empty()
            {
                // we can't simply add a filter if we have some should but no must/filter. We must
                // add a new layer of bool query
                BoolQuery {
                    must: vec![bool_query.into()],
                    filter: vec![range.into()],
                    ..Default::default()
                }
                .into()
            } else {
                bool_query.filter.push(range.into());
                QueryAst::Bool(bool_query)
            }
        } else {
            BoolQuery {
                must: vec![new_ast],
                filter: vec![range.into()],
                ..Default::default()
            }
            .into()
        }
    }

    search_request.query_ast = serde_json::to_string(&new_ast).unwrap();
    search_request.start_timestamp = None;
    search_request.end_timestamp = None;
}

/// Remove all `must` and `filter timestamp ranges, and summarize them
#[derive(Debug, Clone)]
struct RemoveTimestampRange<'a> {
    timestamp_field: &'a str,
    start_timestamp: Bound<DateTime>,
    end_timestamp: Bound<DateTime>,
}

impl RemoveTimestampRange<'_> {
    fn update_start_timestamp(
        &mut self,
        lower_bound: &quickwit_query::JsonLiteral,
        included: bool,
    ) {
        use quickwit_query::InterpretUserInput;
        let Some(lower_bound) = DateTime::interpret_json(lower_bound) else {
            // we shouldn't be able to get here, we would have errored much earlier in root search
            warn!("unparsable time bound in leaf search: {lower_bound:?}");
            return;
        };
        let bound = if included {
            Bound::Included(lower_bound)
        } else {
            Bound::Excluded(lower_bound)
        };

        self.start_timestamp = max_bound(self.start_timestamp, bound);
    }

    fn update_end_timestamp(&mut self, upper_bound: &quickwit_query::JsonLiteral, included: bool) {
        use quickwit_query::InterpretUserInput;
        let Some(upper_bound) = DateTime::interpret_json(upper_bound) else {
            // we shouldn't be able to get here, we would have errored much earlier in root search
            warn!("unparsable time bound in leaf search: {upper_bound:?}");
            return;
        };
        let bound = if included {
            Bound::Included(upper_bound)
        } else {
            Bound::Excluded(upper_bound)
        };

        self.end_timestamp = min_bound(self.end_timestamp, bound);
    }
}

impl QueryAstTransformer for RemoveTimestampRange<'_> {
    type Err = std::convert::Infallible;

    fn transform_bool(&mut self, mut bool_query: BoolQuery) -> Result<Option<QueryAst>, Self::Err> {
        // we only want to visit sub-queries which are strict (positive) requirements
        bool_query.must = bool_query
            .must
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        bool_query.filter = bool_query
            .filter
            .into_iter()
            .filter_map(|query_ast| self.transform(query_ast).transpose())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(QueryAst::Bool(bool_query)))
    }

    fn transform_range(&mut self, range_query: RangeQuery) -> Result<Option<QueryAst>, Self::Err> {
        if range_query.field == self.timestamp_field {
            match range_query.lower_bound {
                Bound::Included(lower_bound) => {
                    self.update_start_timestamp(&lower_bound, true);
                }
                Bound::Excluded(lower_bound) => {
                    self.update_start_timestamp(&lower_bound, false);
                }
                Bound::Unbounded => (),
            };

            match range_query.upper_bound {
                Bound::Included(upper_bound) => {
                    self.update_end_timestamp(&upper_bound, true);
                }
                Bound::Excluded(upper_bound) => {
                    self.update_end_timestamp(&upper_bound, false);
                }
                Bound::Unbounded => (),
            };

            Ok(Some(QueryAst::MatchAll))
        } else {
            Ok(Some(range_query.into()))
        }
    }

    fn transform_term(&mut self, term_query: TermQuery) -> Result<Option<QueryAst>, Self::Err> {
        // TODO we could remove query bounds, this point query surely is more precise, and it
        // doesn't require loading a fastfield
        Ok(Some(QueryAst::Term(term_query)))
    }
}

/// Checks if request is a simple all query.
/// Simple in this case would still including sorting
fn is_simple_all_query(search_request: &SearchRequest) -> bool {
    if search_request.aggregation_request.is_some() {
        return false;
    }

    if search_request.search_after.is_some() {
        return false;
    }

    // TODO: Update the logic to handle start_timestamp end_timestamp ranges
    if search_request.start_timestamp.is_some() || search_request.end_timestamp.is_some() {
        return false;
    }

    let Ok(query_ast) = serde_json::from_str(&search_request.query_ast) else {
        return false;
    };

    matches!(query_ast, QueryAst::MatchAll)
}

#[derive(Debug, Clone)]
enum CanSplitDoBetter {
    Uninformative,
    SplitIdHigher(Option<String>),
    SplitTimestampHigher(Option<i64>),
    SplitTimestampLower(Option<i64>),
    FindTraceIdsAggregation(Option<i64>),
}

impl CanSplitDoBetter {
    /// Create a CanSplitDoBetter from a SearchRequest
    fn from_request(request: &SearchRequest, timestamp_field_name: Option<&str>) -> Self {
        if request.max_hits == 0
            && let Some(aggregation) = &request.aggregation_request
            && let Ok(crate::QuickwitAggregations::FindTraceIdsAggregation(find_trace_aggregation)) =
                serde_json::from_str(aggregation)
            && Some(find_trace_aggregation.span_timestamp_field_name.as_str())
                == timestamp_field_name
        {
            return CanSplitDoBetter::FindTraceIdsAggregation(None);
        }

        if request.sort_fields.is_empty() {
            CanSplitDoBetter::SplitIdHigher(None)
        } else if let Some((sort_by, timestamp_field)) =
            request.sort_fields.first().zip(timestamp_field_name)
        {
            if sort_by.field_name == timestamp_field {
                if sort_by.sort_order() == SortOrder::Desc {
                    CanSplitDoBetter::SplitTimestampHigher(None)
                } else {
                    CanSplitDoBetter::SplitTimestampLower(None)
                }
            } else {
                CanSplitDoBetter::Uninformative
            }
        } else {
            CanSplitDoBetter::Uninformative
        }
    }

    /// Optimize the order in which splits will get processed based on how it can skip the most
    /// splits.
    ///
    /// The leaf search code contains some logic that makes it possible to skip entire splits
    /// when we are confident they won't make it into top K.
    /// To make this optimization as potent as possible, we sort the splits so that the first splits
    /// are the most likely to fill our Top K.
    /// In the future, as split get more metadata per column, we may be able to do this more than
    /// just for timestamp and "unsorted" request.
    fn optimize_split_order(&self, splits: &mut [SplitIdAndFooterOffsets]) {
        match self {
            CanSplitDoBetter::SplitIdHigher(_) => {
                splits.sort_unstable_by(|a, b| b.split_id.cmp(&a.split_id))
            }
            CanSplitDoBetter::SplitTimestampHigher(_)
            | CanSplitDoBetter::FindTraceIdsAggregation(_) => {
                splits.sort_unstable_by_key(|split| std::cmp::Reverse(split.timestamp_end()))
            }
            CanSplitDoBetter::SplitTimestampLower(_) => {
                splits.sort_unstable_by_key(|split| split.timestamp_start())
            }
            CanSplitDoBetter::Uninformative => (),
        }
    }

    /// This function tries to detect upfront which splits contain the top n hits and convert other
    /// split searches to count only searches. It also optimizes split order.
    ///
    /// Returns the search_requests with their split.
    fn optimize(
        &self,
        request: Arc<SearchRequest>,
        mut splits: Vec<SplitIdAndFooterOffsets>,
    ) -> Result<Vec<(SplitIdAndFooterOffsets, SearchRequest)>, SearchError> {
        self.optimize_split_order(&mut splits);

        if !is_simple_all_query(&request) {
            // no optimization opportunity here.
            return Ok(splits
                .into_iter()
                .map(|split| (split, (*request).clone()))
                .collect::<Vec<_>>());
        }

        let num_requested_docs = request.start_offset + request.max_hits;

        // Calculate the number of splits which are guaranteed to deliver enough documents.
        let min_required_splits = splits
            .iter()
            .map(|split| split.num_docs)
            // computing the partial sum
            .scan(0u64, |partial_sum: &mut u64, num_docs_in_split: u64| {
                *partial_sum += num_docs_in_split;
                Some(*partial_sum)
            })
            .take_while(|partial_sum| *partial_sum < num_requested_docs)
            .count()
            + 1;

        // TODO: we maybe want here some deduplication + Cow logic
        let mut split_with_req = splits
            .into_iter()
            .map(|split| (split, (*request).clone()))
            .collect::<Vec<_>>();

        // reuse the detected sort order in split_filter
        // we want to detect cases where we can convert some split queries to count only queries
        match self {
            CanSplitDoBetter::SplitIdHigher(_) => {
                // In this case there is no sort order, we order by split id.
                // If the first split has enough documents, we can convert the other queries to
                // count only queries
                for (_split, request) in split_with_req.iter_mut().skip(min_required_splits) {
                    disable_search_request_hits(request);
                }
            }
            CanSplitDoBetter::Uninformative => {}
            CanSplitDoBetter::SplitTimestampLower(_) => {
                // We order by timestamp asc. split_with_req is sorted by timestamp_start.
                //
                // If we know that some splits will deliver enough documents, we can convert the
                // others to count only queries.
                // Since we only have start and end ranges and don't know the distribution we make
                // sure the splits dont' overlap, since the distribution of two
                // splits could be like this (dot is a timestamp doc on a x axis), for top 2
                // queries.
                // ```
                // [.          .] Split1 has enough docs, but last doc is not in top 2
                //           [..         .] Split2 first doc is in top2
                // ```
                // Let's get the biggest timestamp_end of the first num_splits splits
                let biggest_end_timestamp = split_with_req
                    .iter()
                    .take(min_required_splits)
                    .map(|(split, _)| split.timestamp_end())
                    .max()
                    // if min_required_splits is 0, we choose a value that disables all splits
                    .unwrap_or(i64::MIN);
                for (split, request) in split_with_req.iter_mut().skip(min_required_splits) {
                    if split.timestamp_start() > biggest_end_timestamp {
                        disable_search_request_hits(request);
                    }
                }
            }
            CanSplitDoBetter::SplitTimestampHigher(_) => {
                // We order by timestamp desc. split_with_req is sorted by timestamp_end desc.
                //
                // We have the number of splits we need to search to get enough docs, now we need to
                // find the splits that don't overlap.
                //
                // Let's get the smallest timestamp_start of the first num_splits splits
                let smallest_start_timestamp = split_with_req
                    .iter()
                    .take(min_required_splits)
                    .map(|(split, _)| split.timestamp_start())
                    .min()
                    // if min_required_splits is 0, we choose a value that disables all splits
                    .unwrap_or(i64::MAX);
                for (split, request) in split_with_req.iter_mut().skip(min_required_splits) {
                    if split.timestamp_end() < smallest_start_timestamp {
                        disable_search_request_hits(request);
                    }
                }
            }
            CanSplitDoBetter::FindTraceIdsAggregation(_) => {}
        }

        Ok(split_with_req)
    }

    /// Returns whether the given split can possibly give documents better than the one already
    /// known to match.
    fn can_be_better(&self, split: &SplitIdAndFooterOffsets) -> bool {
        match self {
            CanSplitDoBetter::SplitIdHigher(Some(split_id)) => split.split_id >= *split_id,
            CanSplitDoBetter::SplitTimestampHigher(Some(timestamp))
            | CanSplitDoBetter::FindTraceIdsAggregation(Some(timestamp)) => {
                split.timestamp_end() >= *timestamp
            }
            CanSplitDoBetter::SplitTimestampLower(Some(timestamp)) => {
                split.timestamp_start() <= *timestamp
            }
            _ => true,
        }
    }

    /// Record the new worst-of-the-top document, that is, the document which would first be
    /// evicted from the list of best documents, if a better document was found. Only call this
    /// function if you have at least max_hits documents already.
    fn record_new_worst_hit(&mut self, hit: &PartialHit) {
        match self {
            CanSplitDoBetter::Uninformative => (),
            CanSplitDoBetter::SplitIdHigher(split_id) => *split_id = Some(hit.split_id.clone()),
            CanSplitDoBetter::SplitTimestampHigher(timestamp)
            | CanSplitDoBetter::FindTraceIdsAggregation(timestamp) => {
                if let Some(SortValue::I64(timestamp_ns)) = hit.sort_value() {
                    // if we get a timestamp of, says 1.5s, we need to check up to 2s to make
                    // sure we don't throw away something like 1.2s, so we should round up while
                    // dividing.
                    *timestamp = Some(quickwit_common::div_ceil(timestamp_ns, 1_000_000_000));
                }
            }
            CanSplitDoBetter::SplitTimestampLower(timestamp) => {
                if let Some(SortValue::I64(timestamp_ns)) = hit.sort_value() {
                    // if we get a timestamp of, says 1.5s, we need to check down to 1s to make
                    // sure we don't throw away something like 1.7s, so we should truncate,
                    // which is the default behavior of division
                    let timestamp_s = timestamp_ns / 1_000_000_000;
                    *timestamp = Some(timestamp_s);
                }
            }
        }
    }
}

/// Searches multiple splits, potentially in multiple indexes, sitting on different storages and
/// having different doc mappings.
#[instrument(skip_all, fields(index = ?leaf_search_request.search_request.as_ref().unwrap().index_id_patterns))]
pub async fn multi_index_leaf_search(
    searcher_context: Arc<SearcherContext>,
    leaf_search_request: LeafSearchRequest,
    storage_resolver: &StorageResolver,
) -> Result<LeafSearchResponse, SearchError> {
    let search_request: Arc<SearchRequest> = leaf_search_request
        .search_request
        .ok_or_else(|| SearchError::Internal("no search request".to_string()))?
        .into();

    let doc_mappers: Vec<Arc<DocMapper>> = leaf_search_request
        .doc_mappers
        .iter()
        .map(|doc_mapper| deserialize_doc_mapper(doc_mapper))
        .collect::<crate::Result<_>>()?;
    // Creates a collector which merges responses into one
    let aggregation_limits = searcher_context.get_aggregation_limits();
    // TODO: to avoid lockstep, we should pull up the future creation over the list of split ids
    // and have the semaphore on this level.
    // This will lower resource consumption due to less in-flight futures and avoid contention.
    // It also allows passing early exit conditions between indices.
    //
    // It is a little bit tricky how to handle which is now the incremental_merge_collector, one
    // per index, e.g. when to merge results and how to avoid lock contention.
    let mut leaf_request_tasks = Vec::new();

    for leaf_search_request_ref in leaf_search_request.leaf_requests.into_iter() {
        let index_uri = quickwit_common::uri::Uri::from_str(
            leaf_search_request
                .index_uris
                .get(leaf_search_request_ref.index_uri_ord as usize)
                .ok_or_else(|| {
                    SearchError::Internal(format!(
                        "Received incorrect request, index_uri_ord out of bounds: {}",
                        leaf_search_request_ref.index_uri_ord
                    ))
                })?,
        )?;
        let doc_mapper = doc_mappers
            .get(leaf_search_request_ref.doc_mapper_ord as usize)
            .ok_or_else(|| {
                SearchError::Internal(format!(
                    "Received incorrect request, doc_mapper_ord out of bounds: {}",
                    leaf_search_request_ref.doc_mapper_ord
                ))
            })?
            .clone();

        let leaf_request_future = tokio::spawn({
            let storage_resolver = storage_resolver.clone();
            let searcher_context = searcher_context.clone();
            let search_request = search_request.clone();
            let aggregation_limits = aggregation_limits.clone();
            async move {
                let storage = storage_resolver.resolve(&index_uri).await?;
                single_doc_mapping_leaf_search(
                    searcher_context,
                    search_request,
                    storage,
                    leaf_search_request_ref.split_offsets,
                    doc_mapper,
                    aggregation_limits,
                )
                .await
            }
            .in_current_span()
        });
        leaf_request_tasks.push(leaf_request_future);
    }

    let leaf_responses: Vec<crate::Result<LeafSearchResponse>> = tokio::time::timeout(
        searcher_context.searcher_config.request_timeout(),
        try_join_all(leaf_request_tasks),
    )
    .await??;
    let merge_collector = make_merge_collector(&search_request, aggregation_limits)?;
    let mut incremental_merge_collector = IncrementalCollector::new(merge_collector);
    for result in leaf_responses {
        match result {
            Ok(result) => {
                incremental_merge_collector.add_result(result)?;
            }
            Err(err) => {
                incremental_merge_collector.add_failed_split(SplitSearchError {
                    split_id: "unknown".to_string(),
                    error: format!("{err}"),
                    retryable_error: true,
                });
            }
        }
    }

    crate::search_thread_pool()
        .run_cpu_intensive(|| incremental_merge_collector.finalize().map_err(Into::into))
        .instrument(info_span!("incremental_merge_finalize"))
        .await
        .context("failed to merge split search responses")?
}

/// Optimizes the search_request based on CanSplitDoBetter
/// Returns None if the search request does nothing can be skipped.
#[must_use]
fn simplify_search_request(
    mut search_request: SearchRequest,
    split: &SplitIdAndFooterOffsets,
    split_filter_lock: &Arc<RwLock<CanSplitDoBetter>>,
) -> Option<SearchRequest> {
    let can_be_better: bool;
    let is_trace_req: bool;
    {
        let split_filter_guard = split_filter_lock.read().unwrap();
        can_be_better = split_filter_guard.can_be_better(split);
        // The info is originally from the search_request.aggregation as a string (yes we need to
        // clean this eventually). We don't want to parse it again, so we use the
        // split_filter variant to get that info.
        is_trace_req = matches!(
            &*split_filter_guard,
            &CanSplitDoBetter::FindTraceIdsAggregation(_)
        );
    }
    if !can_be_better {
        disable_search_request_hits(&mut search_request);
    }
    if is_trace_req {
        return Some(search_request);
    }
    if search_request.max_hits > 0 {
        return Some(search_request);
    }
    if search_request.aggregation_request.is_some() {
        return Some(search_request);
    }
    if search_request.count_hits() == CountHits::CountAll {
        return Some(search_request);
    }
    None
}

/// Alter the search request so it does not return any docs.
///
/// This is usually done since it cannot provide better hits results than existing fetched results.
fn disable_search_request_hits(search_request: &mut SearchRequest) {
    search_request.max_hits = 0;
    search_request.start_offset = 0;
    search_request.sort_fields.clear();
    search_request.search_after = None;
}

/// Searches multiple splits for a specific index and a single doc mapping
///
/// The leaf search collects all kind of information, and returns a set of
/// [PartialHit](quickwit_proto::search::PartialHit) candidates. The root will be in
/// charge to consolidate, identify the actual final top hits to display, and
/// fetch the actual documents to convert the partial hits into actual Hits.
pub async fn single_doc_mapping_leaf_search(
    searcher_context: Arc<SearcherContext>,
    request: Arc<SearchRequest>,
    index_storage: Arc<dyn Storage>,
    splits: Vec<SplitIdAndFooterOffsets>,
    doc_mapper: Arc<DocMapper>,
    aggregations_limits: AggregationLimitsGuard,
) -> Result<LeafSearchResponse, SearchError> {
    let num_docs: u64 = splits.iter().map(|split| split.num_docs).sum();
    let num_splits = splits.len();
    info!(num_docs, num_splits, split_offsets = ?PrettySample::new(&splits, 5));

    let split_filter = CanSplitDoBetter::from_request(&request, doc_mapper.timestamp_field_name());
    let split_with_req = split_filter.optimize(request.clone(), splits)?;

    let split_filter = Arc::new(RwLock::new(split_filter));

    let mut leaf_search_single_split_join_handles: Vec<(String, tokio::task::JoinHandle<()>)> =
        Vec::with_capacity(split_with_req.len());

    let merge_collector = make_merge_collector(&request, aggregations_limits.clone())?;
    let incremental_merge_collector = IncrementalCollector::new(merge_collector);
    let incremental_merge_collector = Arc::new(Mutex::new(incremental_merge_collector));

    // We acquire all of the leaf search permits to make sure our single split search tasks
    // do no interleave with other leaf search requests.
    let permit_sizes = split_with_req.iter().map(|(split, _)| {
        compute_initial_memory_allocation(
            split,
            searcher_context
                .searcher_config
                .warmup_single_split_initial_allocation,
        )
    });
    let permit_futures = searcher_context
        .search_permit_provider
        .get_permits(permit_sizes)
        .await;

    let leaf_search_context = Arc::new(LeafSearchContext {
        searcher_context: searcher_context.clone(),
        split_outcome_counters: Arc::new(SplitSearchOutcomeCounters::new_unregistered()),
        incremental_merge_collector: incremental_merge_collector.clone(),
        doc_mapper: doc_mapper.clone(),
        split_filter: split_filter.clone(),
    });

    for ((split, search_request), permit_fut) in
        split_with_req.into_iter().zip(permit_futures.into_iter())
    {
        let leaf_split_search_permit = permit_fut
            .instrument(info_span!("waiting_for_leaf_search_split_semaphore"))
            .await;

        let Some(simplified_search_request) =
            simplify_search_request(search_request, &split, &split_filter)
        else {
            let mut leaf_search_state_guard =
                SplitSearchStateGuard::new(leaf_search_context.split_outcome_counters.clone());
            leaf_search_state_guard.set_state(SplitSearchState::PrunedBeforeWarmup);
            continue;
        };

        leaf_search_single_split_join_handles.push((
            split.split_id.clone(),
            tokio::spawn(
                leaf_search_single_split_wrapper(
                    simplified_search_request,
                    leaf_search_context.clone(),
                    index_storage.clone(),
                    split,
                    leaf_split_search_permit,
                    aggregations_limits.clone(),
                )
                .in_current_span(),
            ),
        ));
    }

    // TODO we could cancel running splits when !run_all_splits and the running split can no
    // longer give better results after some other split answered.
    let mut split_search_join_errors: Vec<(String, JoinError)> = Vec::new();

    // There is no need to use `join_all`, as these are spawned tasks.
    for (split, leaf_search_join_handle) in leaf_search_single_split_join_handles {
        // splits that did not panic were already added to the collector
        if let Err(join_error) = leaf_search_join_handle.await {
            if join_error.is_cancelled() {
                // An explicit task cancellation is not an error.
                continue;
            }
            if join_error.is_panic() {
                error!(split=%split, "leaf search task panicked");
            } else {
                error!(split=%split, "please report: leaf search was not cancelled, and could not extract panic. this should never happen");
            }
            split_search_join_errors.push((split, join_error));
        }
    }

    info!(split_outcome_counters=%leaf_search_context.split_outcome_counters, "leaf split search finished");

    // we can't use unwrap_or_clone because mutexes aren't Clone
    let mut incremental_merge_collector = match Arc::try_unwrap(incremental_merge_collector) {
        Ok(filter_merger) => filter_merger.into_inner().unwrap(),
        Err(filter_merger) => filter_merger.lock().unwrap().clone(),
    };

    for (split_id, split_search_join_error) in split_search_join_errors {
        incremental_merge_collector.add_failed_split(SplitSearchError {
            split_id,
            error: SearchError::from(split_search_join_error).to_string(),
            retryable_error: true,
        });
    }

    let leaf_search_response_reresult: Result<Result<LeafSearchResponse, _>, _> =
        crate::search_thread_pool()
            .run_cpu_intensive(|| incremental_merge_collector.finalize())
            .instrument(info_span!("incremental_merge_intermediate"))
            .await
            .context("failed to merge split search responses");

    Ok(leaf_search_response_reresult??)
}

#[derive(Copy, Clone)]
enum SplitSearchState {
    Start,
    CacheHit,
    PrunedBeforeWarmup,
    WarmUp,
    PrunedAfterWarmup,
    CpuQueue,
    Cpu,
    Success,
}

impl SplitSearchState {
    pub fn inc(self, counters: &SplitSearchOutcomeCounters) {
        match self {
            SplitSearchState::Start => counters.cancel_before_warmup.inc(),
            SplitSearchState::CacheHit => counters.cache_hit.inc(),
            SplitSearchState::PrunedBeforeWarmup => counters.pruned_before_warmup.inc(),
            SplitSearchState::WarmUp => counters.cancel_warmup.inc(),
            SplitSearchState::PrunedAfterWarmup => counters.pruned_after_warmup.inc(),
            SplitSearchState::CpuQueue => counters.cancel_cpu_queue.inc(),
            SplitSearchState::Cpu => counters.cancel_cpu.inc(),
            SplitSearchState::Success => counters.success.inc(),
        }
    }
}

impl Drop for SplitSearchStateGuard {
    fn drop(&mut self) {
        self.state
            .inc(&crate::metrics::SEARCH_METRICS.split_search_outcome_total);
        self.state.inc(&self.local_split_search_outcome_counters);
    }
}

struct SplitSearchStateGuard {
    state: SplitSearchState,
    local_split_search_outcome_counters: Arc<SplitSearchOutcomeCounters>,
}

impl SplitSearchStateGuard {
    pub fn new(local_split_search_outcome_counters: Arc<SplitSearchOutcomeCounters>) -> Self {
        SplitSearchStateGuard {
            state: SplitSearchState::Start,
            local_split_search_outcome_counters: local_split_search_outcome_counters.clone(),
        }
    }

    pub fn set_state(&mut self, state: SplitSearchState) {
        self.state = state;
    }
}

struct LeafSearchContext {
    searcher_context: Arc<SearcherContext>,
    split_outcome_counters: Arc<SplitSearchOutcomeCounters>,
    incremental_merge_collector: Arc<Mutex<IncrementalCollector>>,
    doc_mapper: Arc<DocMapper>,
    split_filter: Arc<RwLock<CanSplitDoBetter>>,
}

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all, fields(split_id = split.split_id, num_docs = split.num_docs))]
async fn leaf_search_single_split_wrapper(
    request: SearchRequest,
    ctx: Arc<LeafSearchContext>,
    index_storage: Arc<dyn Storage>,
    split: SplitIdAndFooterOffsets,
    mut search_permit: SearchPermit,
    aggregations_limits: AggregationLimitsGuard,
) {
    let timer = crate::SEARCH_METRICS
        .leaf_search_split_duration_secs
        .start_timer();
    let leaf_search_single_split_opt_res: crate::Result<Option<LeafSearchResponse>> =
        leaf_search_single_split(
            request,
            ctx.clone(),
            index_storage,
            split.clone(),
            aggregations_limits,
            &mut search_permit,
        )
        .await;

    // Explicitly drop the permit for readability.
    // This should always happen after the ephemeral search cache is dropped.
    std::mem::drop(search_permit);

    if leaf_search_single_split_opt_res.is_ok() {
        timer.observe_duration();
    }

    let mut locked_incremental_merge_collector = ctx.incremental_merge_collector.lock().unwrap();
    match leaf_search_single_split_opt_res {
        Ok(Some(split_search_res)) => {
            if let Err(err) = locked_incremental_merge_collector.add_result(split_search_res) {
                locked_incremental_merge_collector.add_failed_split(SplitSearchError {
                    split_id: split.split_id.clone(),
                    error: format!("Error parsing aggregation result: {err}"),
                    retryable_error: true,
                });
            }
        }
        Ok(None) => {}
        Err(err) => locked_incremental_merge_collector.add_failed_split(SplitSearchError {
            split_id: split.split_id.clone(),
            error: format!("{err}"),
            retryable_error: true,
        }),
    }
    if let Some(last_hit) = locked_incremental_merge_collector.peek_worst_hit() {
        // TODO: we could use the RWLock instead and read the value instead of updating it
        // unconditionally.
        ctx.split_filter
            .write()
            .unwrap()
            .record_new_worst_hit(last_hit.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use bytes::BufMut;
    use quickwit_directories::write_hotcache;
    use rand::{Rng, thread_rng};
    use tantivy::TantivyDocument;
    use tantivy::directory::RamDirectory;
    use tantivy::schema::{
        BytesOptions, FieldEntry, Schema, TextFieldIndexing, TextOptions, Value,
    };

    use super::*;

    fn bool_filter(ast: impl Into<QueryAst>) -> QueryAst {
        BoolQuery {
            must: vec![QueryAst::MatchAll],
            filter: vec![ast.into()],
            ..Default::default()
        }
        .into()
    }

    #[track_caller]
    fn assert_ast_eq(got: &SearchRequest, expected: &QueryAst) {
        let got_ast: QueryAst = serde_json::from_str(&got.query_ast).unwrap();
        assert_eq!(&got_ast, expected);
        assert!(got.start_timestamp.is_none());
        assert!(got.end_timestamp.is_none());
    }

    #[track_caller]
    fn remove_timestamp_test_case(
        request: &SearchRequest,
        split: &SplitIdAndFooterOffsets,
        expected: Option<RangeQuery>,
    ) {
        let timestamp_field = "timestamp";

        // test the query directly
        let mut request_direct = request.clone();
        remove_redundant_timestamp_range(&mut request_direct, split, timestamp_field);
        let expected_direct = expected
            .clone()
            .map(bool_filter)
            .unwrap_or(QueryAst::MatchAll);
        assert_ast_eq(&request_direct, &expected_direct);
    }

    #[test]
    fn test_remove_timestamp_range() {
        const S_TO_NS: i64 = 1_000_000_000;
        let time1 = 1700001000;
        let time2 = 1700002000;
        let time3 = 1700003000;
        let time4 = 1700004000;

        let timestamp_field = "timestamp".to_string();

        // cases where the bounds are larger than the split: no bound is emitted
        let split = SplitIdAndFooterOffsets {
            timestamp_start: Some(time2),
            timestamp_end: Some(time3),
            ..SplitIdAndFooterOffsets::default()
        };

        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Included(time1.into()),
                // *1000 has no impact, we detect timestamp in ms instead of s
                upper_bound: Bound::Included((time4 * 1000).into()),
            }))
            .unwrap(),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(&search_request, &split, None);

        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Included(time1.into()),
                upper_bound: Bound::Included(time3.into()),
            }))
            .unwrap(),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(&search_request, &split, None);

        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::MatchAll).unwrap(),
            start_timestamp: Some(time1),
            end_timestamp: Some(time4),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(&search_request, &split, None);

        // request bound that are exclusive are treated properly
        let expected_upper_exclusive = RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Unbounded,
            upper_bound: Bound::Excluded((time3 * S_TO_NS).into()),
        };
        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Included(time1.into()),
                upper_bound: Bound::Excluded(time3.into()),
            }))
            .unwrap(),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(
            &search_request,
            &split,
            Some(expected_upper_exclusive.clone()),
        );

        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::MatchAll).unwrap(),
            start_timestamp: Some(time1),
            end_timestamp: Some(time3),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(
            &search_request,
            &split,
            Some(expected_upper_exclusive.clone()),
        );

        let expected_lower_exclusive = RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Excluded((time2 * S_TO_NS).into()),
            upper_bound: Bound::Unbounded,
        };
        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Excluded(time2.into()),
                upper_bound: Bound::Included(time3.into()),
            }))
            .unwrap(),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(
            &search_request,
            &split,
            Some(expected_lower_exclusive.clone()),
        );

        // we take the most restrictive bounds
        let split = SplitIdAndFooterOffsets {
            timestamp_start: Some(time1),
            timestamp_end: Some(time4),
            ..SplitIdAndFooterOffsets::default()
        };

        let expected_upper_2_ex = RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Unbounded,
            upper_bound: Bound::Excluded((time2 * S_TO_NS).into()),
        };
        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Included(time1.into()),
                upper_bound: Bound::Included(time3.into()),
            }))
            .unwrap(),
            start_timestamp: Some(time1),
            end_timestamp: Some(time2),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(&search_request, &split, Some(expected_upper_2_ex));

        let expected_upper_2_inc = RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Unbounded,
            upper_bound: Bound::Included((time2 * S_TO_NS).into()),
        };
        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Included(time1.into()),
                upper_bound: Bound::Included(time2.into()),
            }))
            .unwrap(),
            start_timestamp: Some(time1),
            end_timestamp: Some(time3),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(&search_request, &split, Some(expected_upper_2_inc));

        let expected_lower_3 = RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Included((time3 * S_TO_NS).into()),
            upper_bound: Bound::Unbounded,
        };

        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Included(time2.into()),
                upper_bound: Bound::Included(time4.into()),
            }))
            .unwrap(),
            start_timestamp: Some(time3),
            end_timestamp: Some(time4 + 1),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(&search_request, &split, Some(expected_lower_3.clone()));

        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Range(RangeQuery {
                field: timestamp_field.to_string(),
                lower_bound: Bound::Included(time3.into()),
                upper_bound: Bound::Included(time4.into()),
            }))
            .unwrap(),
            start_timestamp: Some(time2),
            end_timestamp: Some(time4 + 1),
            ..SearchRequest::default()
        };
        remove_timestamp_test_case(&search_request, &split, Some(expected_lower_3));

        let mut search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::MatchAll).unwrap(),
            start_timestamp: Some(time1),
            end_timestamp: Some(time4),
            ..SearchRequest::default()
        };
        let split = SplitIdAndFooterOffsets {
            timestamp_start: Some(time2),
            timestamp_end: Some(time3),
            ..SplitIdAndFooterOffsets::default()
        };
        remove_redundant_timestamp_range(&mut search_request, &split, &timestamp_field);
        assert_ast_eq(&search_request, &QueryAst::MatchAll);
    }

    // regression test for #4935
    #[test]
    fn test_remove_timestamp_range_keep_should() {
        let time1 = 1700001000;
        let time2 = 1700002000;
        let time3 = 1700003000;

        let timestamp_field = "timestamp".to_string();

        // cases where the bounds are larger than the split: no bound is emitted
        let split = SplitIdAndFooterOffsets {
            timestamp_start: Some(time1),
            timestamp_end: Some(time3),
            ..SplitIdAndFooterOffsets::default()
        };

        let mut search_request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Bool(BoolQuery {
                should: vec![QueryAst::MatchAll],
                ..BoolQuery::default()
            }))
            .unwrap(),
            start_timestamp: Some(time2),
            end_timestamp: None,
            ..SearchRequest::default()
        };
        remove_redundant_timestamp_range(&mut search_request, &split, &timestamp_field);
        assert_ast_eq(
            &search_request,
            &QueryAst::Bool(BoolQuery {
                // original request
                must: vec![QueryAst::Bool(BoolQuery {
                    should: vec![QueryAst::MatchAll],
                    ..BoolQuery::default()
                })],
                // time bound
                filter: vec![
                    RangeQuery {
                        field: "timestamp".to_string(),
                        lower_bound: Bound::Included(1_700_002_000_000_000_000u64.into()),
                        upper_bound: Bound::Unbounded,
                    }
                    .into(),
                ],
                ..BoolQuery::default()
            }),
        );
    }

    #[test]
    fn test_remove_extended_bounds_from_histogram() {
        let histo_at_root = r#"
{
  "date_histo": {
    "date_histogram": {
      "extended_bounds": {
        "max": 1425254400000,
        "min": 1420070400000
      },
      "field": "date",
      "fixed_interval": "30d",
      "offset": "-4d"
    }
  }
}
"#;

        let histo_at_root_no_bounds = r#"
{
  "date_histo": {
    "date_histogram": {
      "field": "date",
      "fixed_interval": "30d",
      "offset": "-4d"
    }
  }
}
"#;

        let histo_at_root_with_sibling = r#"
{
  "metrics": {
    "aggs": {
      "response": {
        "percentiles": {
          "field": "response",
          "keyed": false,
          "percents": [
            85
          ]
        }
      }
    },
    "date_histogram": {
      "extended_bounds": {
        "max": 1425254400000,
        "min": 1420070400000
      },
      "field": "date",
      "fixed_interval": "30d",
      "offset": "-4d"
    }
  }
}
"#;

        let histo_at_root_with_sibling_no_bounds = r#"
{
  "metrics": {
    "aggs": {
      "response": {
        "percentiles": {
          "field": "response",
          "keyed": false,
          "percents": [
            85
          ]
        }
      }
    },
    "date_histogram": {
      "field": "date",
      "fixed_interval": "30d",
      "offset": "-4d"
    }
  }
}
"#;
        let histo_at_leaf = r#"
{
  "metrics": {
    "aggs": {
      "response": {
        "date_histogram": {
          "extended_bounds": {
            "max": 1425254400000,
            "min": 1420070400000
          },
          "field": "date",
          "fixed_interval": "30d",
          "offset": "-4d"
        }
      }
    },
    "percentiles": {
      "field": "response",
      "keyed": false,
      "percents": [
        85
      ]
    }
  }
}
"#;

        let histo_at_leaf_no_bounds = r#"
{
  "metrics": {
    "aggs": {
      "response": {
        "date_histogram": {
          "field": "date",
          "fixed_interval": "30d",
          "offset": "-4d"
        }
      }
    },
    "percentiles": {
      "field": "response",
      "keyed": false,
      "percents": [
        85
      ]
    }
  }
}
"#;
        for (bounds, no_bounds) in [
            (histo_at_root, histo_at_root_no_bounds),
            (
                histo_at_root_with_sibling,
                histo_at_root_with_sibling_no_bounds,
            ),
            (histo_at_leaf, histo_at_leaf_no_bounds),
        ] {
            // first assert we do nothing when there are no bounds
            let request_no_bounds = SearchRequest {
                aggregation_request: Some(no_bounds.to_string()),
                ..SearchRequest::default()
            };
            let mut request_no_bounds_clone = request_no_bounds.clone();
            rewrite_aggregation(&mut request_no_bounds_clone);
            assert_eq!(request_no_bounds, request_no_bounds_clone);

            let mut request_bounds = SearchRequest {
                aggregation_request: Some(bounds.to_string()),
                ..SearchRequest::default()
            };
            rewrite_aggregation(&mut request_bounds);
            // we can't just compare bounds and no_bounds, they must be structuraly equal, but not
            // necessarily identical (field order, null vs absent...). So we parse both and verify
            // the results are equal instead
            let no_bounds_agg: QuickwitAggregations =
                serde_json::from_str(&request_no_bounds.aggregation_request.unwrap()).unwrap();
            let rewrote_bounds_agg: QuickwitAggregations =
                serde_json::from_str(&request_bounds.aggregation_request.unwrap()).unwrap();
            assert_eq!(rewrote_bounds_agg, no_bounds_agg);
        }
    }

    fn create_tantivy_dir_with_hotcache<'a, V>(
        field_entry: FieldEntry,
        field_value: V,
    ) -> (HotDirectory, usize)
    where
        V: Value<'a>,
    {
        let field_name = field_entry.name().to_string();
        let mut schema_builder = Schema::builder();
        schema_builder.add_field(field_entry);
        let schema = schema_builder.build();

        let ram_directory = RamDirectory::create();
        let index = Index::open_or_create(ram_directory.clone(), schema.clone()).unwrap();

        let mut index_writer = index.writer(15_000_000).unwrap();
        let field = schema.get_field(&field_name).unwrap();
        let mut new_doc = TantivyDocument::default();
        new_doc.add_field_value(field, field_value);
        index_writer.add_document(new_doc).unwrap();
        index_writer.commit().unwrap();

        let mut hotcache_bytes_writer = Vec::new().writer();
        write_hotcache(ram_directory.clone(), &mut hotcache_bytes_writer).unwrap();
        let hotcache_bytes = OwnedBytes::new(hotcache_bytes_writer.into_inner());
        let hot_directory = HotDirectory::open(ram_directory.clone(), hotcache_bytes).unwrap();
        (hot_directory, ram_directory.total_mem_usage())
    }

    #[test]
    fn test_compute_index_size_without_store() {
        // We don't want to make assertions on absolute index sizes (it might
        // change in future Tantivy versions), but rather verify that the store
        // is properly excluded from the computed size.

        // We use random bytes so that the store can't compress them
        let mut payload = vec![0u8; 1024];
        thread_rng().fill(&mut payload[..]);

        let (hotcache_directory_stored_payload, directory_size_stored_payload) =
            create_tantivy_dir_with_hotcache(
                FieldEntry::new_bytes("payload".to_string(), BytesOptions::default().set_stored()),
                &payload,
            );
        let size_with_stored_payload =
            compute_index_size(&hotcache_directory_stored_payload).as_u64();

        let (hotcache_directory_index_only, directory_size_index_only) =
            create_tantivy_dir_with_hotcache(
                FieldEntry::new_bytes("payload".to_string(), BytesOptions::default()),
                &payload,
            );
        let size_index_only = compute_index_size(&hotcache_directory_index_only).as_u64();

        assert!(directory_size_stored_payload > directory_size_index_only + 1000);
        assert!(size_with_stored_payload.abs_diff(size_index_only) < 10);
    }

    #[test]
    fn test_compute_index_size_varies_with_data() {
        // We don't want to make assertions on absolute index sizes (it might
        // change in future Tantivy versions), but rather verify that an index
        // with more data is indeed bigger.

        let indexing_options =
            TextOptions::default().set_indexing_options(TextFieldIndexing::default());

        let (hotcache_directory_larger, directory_size_larger) = create_tantivy_dir_with_hotcache(
            FieldEntry::new_text("text".to_string(), indexing_options.clone()),
            "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium \
             doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore \
             veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam \
             voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur \
             magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, \
             qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non \
             numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat \
             voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis \
             suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum \
             iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, \
             vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?",
        );
        let larger_size = compute_index_size(&hotcache_directory_larger).as_u64();

        let (hotcache_directory_smaller, directory_size_smaller) = create_tantivy_dir_with_hotcache(
            FieldEntry::new_text("text".to_string(), indexing_options),
            "hi",
        );
        let smaller_size = compute_index_size(&hotcache_directory_smaller).as_u64();

        assert!(directory_size_larger > directory_size_smaller + 100);
        assert!(larger_size > smaller_size + 100);
    }
}
