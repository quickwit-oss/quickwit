//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use crate::collector::{make_collector_for_split, make_merge_collector, GenericQuickwitCollector};
use crate::SearchError;
use anyhow::Context;
use futures::future::try_join_all;
use itertools::{Either, Itertools};
use quickwit_directories::{CachingDirectory, HotDirectory, StorageDirectory, HOTCACHE_FILENAME};
use quickwit_index_config::IndexConfig;
use quickwit_proto::{LeafSearchResult, SearchRequest, SplitSearchError};
use quickwit_storage::Storage;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tantivy::{collector::Collector, query::Query, Index, ReloadPolicy, Searcher, Term};
use tokio::task::spawn_blocking;

/// Opens a `tantivy::Index` for the given split.
///
/// The resulting index uses a dynamic and a static cache.
pub(crate) async fn open_index(split_storage: Arc<dyn Storage>) -> anyhow::Result<Index> {
    let hotcache_bytes = split_storage
        .get_all(Path::new(HOTCACHE_FILENAME))
        .await
        .with_context(|| format!("Failed to fetch hotcache from {}", split_storage.uri()))?;
    let directory = StorageDirectory::new(split_storage);
    let caching_directory = CachingDirectory::new_with_unlimited_capacity(Arc::new(directory));
    let hot_directory = HotDirectory::open(caching_directory, hotcache_bytes)?;
    let index = Index::open(hot_directory)?;
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
pub(crate) async fn warmup(
    searcher: &Searcher,
    query: &dyn Query,
    fast_field_names: Vec<String>,
) -> anyhow::Result<()> {
    warm_up_terms(searcher, query).await?;
    warm_up_fastfields(searcher, fast_field_names).await?;
    Ok(())
}

async fn warm_up_fastfields(
    searcher: &Searcher,
    fast_field_names: Vec<String>,
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
        fast_fields.push(fast_field);
    }

    let mut warm_up_futures = Vec::new();
    for field in fast_fields {
        for segment_reader in searcher.segment_readers() {
            let fast_field_slice = segment_reader.fast_fields().fast_field_data(field, 0)?;
            warm_up_futures.push(async move { fast_field_slice.read_bytes_async().await });
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_terms(searcher: &Searcher, query: &dyn Query) -> anyhow::Result<()> {
    let mut terms: BTreeMap<Term, bool> = Default::default();
    query.query_terms(&mut terms);
    let grouped_terms = terms.iter().group_by(|term| term.0.field());
    let mut warm_up_futures = Vec::new();
    for (field, terms) in grouped_terms.into_iter() {
        let terms: Vec<(&Term, bool)> = terms
            .map(|(term, position_needed)| (term, *position_needed))
            .collect();
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

/// Apply a leaf search on a single split.
async fn leaf_search_single_split(
    split_id: String,
    index_config: Box<dyn IndexConfig>,
    search_request: &SearchRequest,
    storage: Arc<dyn Storage>,
) -> crate::Result<LeafSearchResult> {
    let index = open_index(storage).await?;
    let split_schema = index.schema();
    let quickwit_collector = make_collector_for_split(
        split_id,
        index_config.as_ref(),
        search_request,
        &split_schema,
    );
    let query = index_config.query(split_schema, search_request)?;

    let reader = index
        .reader_builder()
        .num_searchers(1)
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    warmup(&*searcher, &query, quickwit_collector.fast_field_names()).await?;
    let leaf_search_result = searcher.search(&query, &quickwit_collector)?;
    Ok(leaf_search_result)
}

/// `leaf` step of search.
///
/// The leaf search collects all kind of information, and returns a set of [PartialHit] candidates.
/// The root will be in charge to consolidate, identify the actual final top hits to display, and
/// fetch the actual documents to convert the partial hits into actual Hits.
pub async fn leaf_search(
    index_config: Box<dyn IndexConfig>,
    request: &SearchRequest,
    split_ids: &[String],
    storage: Arc<dyn Storage>,
) -> Result<LeafSearchResult, SearchError> {
    let leaf_search_single_split_futures: Vec<_> = split_ids
        .iter()
        .map(|split_id| {
            let split_storage: Arc<dyn Storage> =
                quickwit_storage::add_prefix_to_storage(storage.clone(), split_id);
            let index_config_clone = index_config.clone();
            async move {
                leaf_search_single_split(
                    split_id.clone(),
                    index_config_clone,
                    request,
                    split_storage,
                )
                .await
                .map_err(|err| (split_id.to_string(), err))
            }
        })
        .collect();
    let split_search_results = futures::future::join_all(leaf_search_single_split_futures).await;

    let (search_results, errors): (Vec<LeafSearchResult>, Vec<(String, SearchError)>) =
        split_search_results
            .into_iter()
            .partition_map(|split_search_res| match split_search_res {
                Ok(search_res) => Either::Left(search_res),
                Err(err) => Either::Right(err),
            });

    let merge_collector = make_merge_collector(request);
    let mut merged_search_results =
        spawn_blocking(move || merge_collector.merge_fruits(search_results))
            .await
            .with_context(|| "Merging search on split results failed")??;

    merged_search_results
        .failed_splits
        .extend(errors.iter().map(|(split_id, err)| SplitSearchError {
            split_id: split_id.to_string(),
            error: format!("{:?}", err),
            retryable_error: true,
        }));
    Ok(merged_search_results)
}
