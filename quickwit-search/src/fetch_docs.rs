// Quickwit
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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::FetchDocsResult;
use quickwit_proto::Hit;
use quickwit_proto::PartialHit;
use quickwit_storage::Storage;
use tantivy::IndexReader;
use tantivy::ReloadPolicy;

use crate::leaf::open_index;
use crate::GlobalDocAddress;

/// Given a list of global doc address, fetch all of the documents and
/// returns them as a hashmap.
async fn fetch_docs_to_map<'a>(
    mut global_doc_addrs: Vec<GlobalDocAddress<'a>>,
    index_storage: Arc<dyn Storage>,
    split_metadata: &[SplitMetadata],
) -> anyhow::Result<HashMap<GlobalDocAddress<'a>, String>> {
    let mut split_fetch_docs_futures = Vec::new();

    let split_by_id: HashMap<String, &SplitMetadata> = split_metadata
        .iter()
        .map(|split| (split.split_id.to_string(), split))
        .collect();
    // We sort global hit addrs in order to allow for the grouby.
    global_doc_addrs.sort_by_key(|global_doc_addr| global_doc_addr.split);
    for (split_id, global_doc_addrs) in global_doc_addrs
        .iter()
        .group_by(|global_doc_addr| global_doc_addr.split)
        .into_iter()
    {
        let split = split_by_id
            .get(split_id)
            .expect("could not find split metadata");
        let split_storage: Arc<dyn Storage> =
            quickwit_storage::add_prefix_to_storage(index_storage.clone(), &split_id);
        let global_doc_addrs: Vec<GlobalDocAddress> =
            global_doc_addrs.into_iter().cloned().collect();
        split_fetch_docs_futures.push(fetch_docs_in_split(global_doc_addrs, split_storage, split));
    }

    let split_fetch_docs: Vec<Vec<(GlobalDocAddress, String)>> =
        futures::future::try_join_all(split_fetch_docs_futures)
            .await
            .with_context(|| "split-fetch-docs")?;

    let global_doc_addr_to_doc_json: HashMap<GlobalDocAddress, String> = split_fetch_docs
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
    partial_hits: Vec<PartialHit>,
    index_storage: Arc<dyn Storage>,
    split_metadata: &[SplitMetadata],
) -> anyhow::Result<FetchDocsResult> {
    let global_doc_addrs: Vec<GlobalDocAddress> = partial_hits
        .iter()
        .map(GlobalDocAddress::from_partial_hit)
        .collect();

    let mut global_doc_addr_to_doc_json =
        fetch_docs_to_map(global_doc_addrs, index_storage, split_metadata).await?;

    let hits: Vec<Hit> = partial_hits
        .iter()
        .flat_map(|partial_hit| {
            let global_doc_addr = GlobalDocAddress::from_partial_hit(partial_hit);
            if let Some((_, json)) = global_doc_addr_to_doc_json.remove_entry(&global_doc_addr) {
                Some(Hit {
                    json,
                    partial_hit: Some(partial_hit.clone()),
                })
            } else {
                None
            }
        })
        .collect();
    Ok(FetchDocsResult { hits })
}

async fn get_searcher_for_split(
    split_storage: Arc<dyn Storage>,
    num_searchers: usize,
    split_metadata: &SplitMetadata,
) -> anyhow::Result<IndexReader> {
    let index = open_index(split_storage, split_metadata)
        .await
        .with_context(|| "open-index-for-split")?;
    let reader = index
        .reader_builder()
        .num_searchers(num_searchers)
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    Ok(reader)
}

/// Fetching docs from a specific split.
async fn fetch_docs_in_split<'a>(
    global_doc_addrs: Vec<GlobalDocAddress<'a>>,
    split_storage: Arc<dyn Storage>,
    split: &SplitMetadata,
) -> anyhow::Result<Vec<(GlobalDocAddress<'a>, String)>> {
    let index_reader = get_searcher_for_split(split_storage, global_doc_addrs.len(), split).await?;
    let mut doc_futures = Vec::new();
    for global_doc_addr in global_doc_addrs {
        let searcher = index_reader.searcher();
        let doc_future = async move {
            let doc = searcher
                .doc_async(global_doc_addr.doc_addr)
                .await
                .with_context(|| "searcher-doc-async")?;
            let doc_json = searcher.schema().to_json(&doc);
            Ok((global_doc_addr, doc_json))
        };
        doc_futures.push(doc_future);
    }
    futures::future::try_join_all(doc_futures).await
}
