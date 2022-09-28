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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use futures::Future;
use itertools::{Either, Itertools};
use quickwit_cache::MemorySizedCache;
use quickwit_directories::{CachingDirectory, HotDirectory, StorageDirectory};
use quickwit_doc_mapper::{DocMapper, QUICKWIT_TOKENIZER_MANAGER};
use quickwit_proto::{
    LeafSearchResponse, SearchRequest, SplitIdAndFooterOffsets, SplitSearchError,
};
use quickwit_storage::{wrap_storage_with_long_term_cache, BundleStorage, Storage};
use tantivy::collector::Collector;
use tantivy::directory::{FileSlice, OwnedBytes};
use tantivy::error::AsyncIoError;
use tantivy::query::Query;
use tantivy::schema::{Cardinality, Field, FieldType};
use tantivy::{Index, ReloadPolicy, Searcher, Term};
use tokio::task::spawn_blocking;
use tracing::*;

use crate::collector::{make_collector_for_split, make_merge_collector};
use crate::service::SearcherContext;
use crate::SearchError;

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
                "Failed to fetch hotcache and footer from {} for split `{}`",
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

/// Opens a `tantivy::Index` for the given split with several cache layers:
/// - A split footer cache given by `SearcherContext.split_footer_cache`.
/// - A fast fields cache given by `SearcherContext.storage_long_term_cache`.
/// - An ephemeral unbounded cache directory whose lifetime is tied to the returned `Index`.
pub(crate) async fn open_index_with_caches(
    searcher_context: &Arc<SearcherContext>,
    index_storage: Arc<dyn Storage>,
    split_and_footer_offsets: &SplitIdAndFooterOffsets,
    ephemeral_unbounded_cache: bool,
) -> anyhow::Result<Index> {
    let split_file = PathBuf::from(format!("{}.split", split_and_footer_offsets.split_id));
    let footer_data = get_split_footer_from_cache_or_fetch(
        index_storage.clone(),
        split_and_footer_offsets,
        &searcher_context.split_footer_cache,
    )
    .await?;

    let (hotcache_bytes, bundle_storage) = BundleStorage::open_from_split_data(
        index_storage,
        split_file,
        FileSlice::new(Arc::new(footer_data)),
    )?;
    let bundle_storage_with_cache = wrap_storage_with_long_term_cache(
        searcher_context.fast_fields_cache.clone(),
        Arc::new(bundle_storage),
    );
    let directory = StorageDirectory::new(bundle_storage_with_cache);
    let hot_directory = if ephemeral_unbounded_cache {
        let caching_directory = CachingDirectory::new_unbounded(
            Arc::new(directory),
            Some(&quickwit_storage::STORAGE_METRICS.shortlived_cache),
        );
        HotDirectory::open(caching_directory, hotcache_bytes.read_bytes()?)?
    } else {
        HotDirectory::open(directory, hotcache_bytes.read_bytes()?)?
    };
    let mut index = Index::open(hot_directory)?;
    index.set_tokenizers(QUICKWIT_TOKENIZER_MANAGER.clone());
    Ok(index)
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
#[instrument(skip(searcher, query, fast_field_names))]
pub(crate) async fn warmup(
    searcher: &Searcher,
    query: &dyn Query,
    fast_field_names: &HashSet<String>,
    term_dict_field_names: &HashSet<String>,
    requires_scoring: bool,
) -> anyhow::Result<()> {
    let warm_up_terms_future =
        warm_up_terms(searcher, query).instrument(debug_span!("warm_up_terms"));
    let warm_up_term_dict_future = warm_up_term_dict_fields(searcher, term_dict_field_names)
        .instrument(debug_span!("warm_up_term_dicts"));
    let warm_up_fastfields_future = warm_up_fastfields(searcher, fast_field_names)
        .instrument(debug_span!("warm_up_fastfields"));
    let warm_up_fieldnorms_future = warm_up_fieldnorms(searcher, requires_scoring)
        .instrument(debug_span!("warm_up_fieldnorms"));
    let (warm_up_terms_res, warm_up_fastfields_res, warm_up_term_dict_res, warm_up_fieldnorms_res) = tokio::join!(
        warm_up_terms_future,
        warm_up_fastfields_future,
        warm_up_term_dict_future,
        warm_up_fieldnorms_future,
    );
    warm_up_terms_res?;
    warm_up_fastfields_res?;
    warm_up_term_dict_res?;
    warm_up_fieldnorms_res?;
    Ok(())
}

async fn warm_up_term_dict_fields(
    searcher: &Searcher,
    term_dict_field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    let mut term_dict_fields = Vec::new();
    for term_dict_field_name in term_dict_field_names.iter() {
        let term_dict_field = searcher
            .schema()
            .get_field(term_dict_field_name)
            .with_context(|| {
                format!(
                    "Couldn't get field named {:?} from schema.",
                    term_dict_field_name
                )
            })?;

        term_dict_fields.push(term_dict_field);
    }

    let mut warm_up_futures = Vec::new();
    for field in term_dict_fields {
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(field)?.clone();
            warm_up_futures.push(async move {
                let dict = inverted_index.terms();
                dict.warm_up_dictionary().await
            });
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

// The field cardinality is not the same as the fast field cardinality.
//
// E.g. a single valued bytes field has a multivalued fast field cardinality.
fn get_fastfield_cardinality(field_type: &FieldType) -> Option<Cardinality> {
    match field_type {
        FieldType::U64(options)
        | FieldType::I64(options)
        | FieldType::F64(options)
        | FieldType::Bool(options) => options.get_fastfield_cardinality(),
        FieldType::Date(options) => options.get_fastfield_cardinality(),
        FieldType::Facet(_) => Some(Cardinality::MultiValues),
        FieldType::Bytes(options) if options.is_fast() => Some(Cardinality::MultiValues),
        FieldType::Str(options) if options.is_fast() => Some(Cardinality::MultiValues),
        _ => None,
    }
}

fn fast_field_idxs(fast_field_cardinality: Cardinality) -> &'static [usize] {
    match fast_field_cardinality {
        Cardinality::SingleValue => &[0],
        Cardinality::MultiValues => &[0, 1],
    }
}

async fn warm_up_fastfields(
    searcher: &Searcher,
    fast_field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    let mut fast_fields = Vec::new();
    for fast_field_name in fast_field_names.iter() {
        let fast_field = searcher
            .schema()
            .get_field(fast_field_name)
            .with_context(|| {
                format!(
                    "Couldn't get field named {:?} from schema.",
                    fast_field_name
                )
            })?;

        let field_entry = searcher.schema().get_field_entry(fast_field);
        if !field_entry.is_fast() {
            anyhow::bail!("Field {:?} is not a fast field.", fast_field_name);
        }
        let cardinality =
            get_fastfield_cardinality(field_entry.field_type()).with_context(|| {
                format!(
                    "Couldn't get field cardinality {:?} from type {:?}.",
                    fast_field_name, field_entry
                )
            })?;

        fast_fields.push((fast_field, cardinality));
    }

    type SendableFuture = dyn Future<Output = Result<OwnedBytes, AsyncIoError>> + Send;
    let mut warm_up_futures: Vec<Pin<Box<SendableFuture>>> = Vec::new();
    for (field, cardinality) in fast_fields {
        for segment_reader in searcher.segment_readers() {
            for &fast_field_idx in fast_field_idxs(cardinality) {
                let fast_field_slice = segment_reader
                    .fast_fields()
                    .fast_field_data(field, fast_field_idx)?;
                warm_up_futures.push(Box::pin(async move {
                    fast_field_slice.read_bytes_async().await
                }));
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_terms(searcher: &Searcher, query: &dyn Query) -> anyhow::Result<()> {
    let mut terms_grouped_by_field: HashMap<Field, Vec<(&Term, bool)>> = Default::default();
    query.query_terms(&mut |term, need_position| {
        let field = term.field();
        terms_grouped_by_field
            .entry(field)
            .or_default()
            .push((term, need_position));
    });
    let mut warm_up_futures = Vec::new();
    for (field, terms) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(field)?;
            for (term, position_needed) in terms.iter().cloned() {
                let inv_idx_clone = inv_idx.clone();
                warm_up_futures
                    .push(async move { inv_idx_clone.warm_postings(term, position_needed).await });
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

/// Apply a leaf search on a single split.
#[instrument(skip(searcher_context, search_request, storage, split, doc_mapper))]
async fn leaf_search_single_split(
    searcher_context: &Arc<SearcherContext>,
    search_request: &SearchRequest,
    storage: Arc<dyn Storage>,
    split: SplitIdAndFooterOffsets,
    doc_mapper: Arc<dyn DocMapper>,
) -> crate::Result<LeafSearchResponse> {
    let split_id = split.split_id.to_string();
    let index = open_index_with_caches(searcher_context, storage, &split, true).await?;
    let split_schema = index.schema();
    let quickwit_collector = make_collector_for_split(
        split_id.clone(),
        doc_mapper.as_ref(),
        search_request,
        &split_schema,
    )?;
    let query = doc_mapper.query(split_schema, search_request)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    warmup(
        &searcher,
        &query,
        &quickwit_collector.fast_field_names(),
        &quickwit_collector.term_dict_field_names(),
        quickwit_collector.requires_scoring(),
    )
    .await?;
    let leaf_search_response = crate::run_cpu_intensive(move || {
        let span = info_span!( "search", split_id = %split.split_id);
        let _span_guard = span.enter();
        searcher.search(&query, &quickwit_collector)
    })
    .await
    .map_err(|_| {
        crate::SearchError::InternalError(format!("Leaf search panicked. split={}", split_id))
    })??;
    Ok(leaf_search_response)
}

/// `leaf` step of search.
///
/// The leaf search collects all kind of information, and returns a set of
/// [PartialHit](quickwit_proto::PartialHit) candidates. The root will be in
/// charge to consolidate, identify the actual final top hits to display, and
/// fetch the actual documents to convert the partial hits into actual Hits.
pub async fn leaf_search(
    searcher_context: Arc<SearcherContext>,
    request: &SearchRequest,
    index_storage: Arc<dyn Storage>,
    splits: &[SplitIdAndFooterOffsets],
    doc_mapper: Arc<dyn DocMapper>,
) -> Result<LeafSearchResponse, SearchError> {
    let leaf_search_single_split_futures: Vec<_> = splits
        .iter()
        .map(|split| {
            let doc_mapper_clone = doc_mapper.clone();
            let index_storage_clone = index_storage.clone();
            let searcher_context_clone = searcher_context.clone();
            async move {
                let _leaf_split_search_permit = searcher_context_clone.leaf_search_split_semaphore
                    .acquire()
                    .await
                    .expect("Failed to acquire permit. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
                crate::SEARCH_METRICS.leaf_searches_splits_total.inc();
                let timer = crate::SEARCH_METRICS
                    .leaf_search_split_duration_secs
                    .start_timer();
                let leaf_search_single_split_res = leaf_search_single_split(
                    &searcher_context_clone,
                    request,
                    index_storage_clone,
                    split.clone(),
                    doc_mapper_clone,
                )
                .await;
                timer.observe_duration();
                leaf_search_single_split_res.map_err(|err| (split.split_id.clone(), err))
            }
        })
        .collect();
    let split_search_results = futures::future::join_all(leaf_search_single_split_futures).await;

    // the result wrapping is only for the collector api merge_fruits
    // (Vec<tantivy::Result<LeafSearchResponse>>)
    let (split_search_responses, errors): (
        Vec<tantivy::Result<LeafSearchResponse>>,
        Vec<(String, SearchError)>,
    ) = split_search_results
        .into_iter()
        .partition_map(|split_search_res| match split_search_res {
            Ok(split_search_resp) => Either::Left(Ok(split_search_resp)),
            Err(err) => Either::Right(err),
        });

    // Creates a collector which merges responses into one
    let merge_collector = make_merge_collector(request)?;

    // Merging is a cpu-bound task.
    // It should be executed by Tokio's blocking threads.
    let mut merged_search_response =
        spawn_blocking(move || merge_collector.merge_fruits(split_search_responses))
            .instrument(info_span!("merge_search_responses"))
            .await
            .context("Failed to merge split search responses.")??;

    merged_search_response
        .failed_splits
        .extend(errors.iter().map(|(split_id, err)| SplitSearchError {
            split_id: split_id.to_string(),
            error: format!("{}", err),
            retryable_error: true,
        }));
    Ok(merged_search_response)
}
