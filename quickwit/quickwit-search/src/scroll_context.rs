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

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use quickwit_common::metrics::GaugeGuard;
use quickwit_common::shared_consts::SCROLL_BATCH_LEN;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::search::{LeafSearchResponse, PartialHit, SearchRequest, SplitSearchError};
use quickwit_proto::types::IndexUid;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use ttl_cache::TtlCache;
use ulid::Ulid;

use crate::ClusterClient;
use crate::root::IndexMetasForLeafSearch;
use crate::service::SearcherContext;

/// Maximum number of values in the local search KV store.
///
/// TODO make configurable.
///
/// Assuming a search context of 1MB, this can
/// amount to up to 1GB.
const LOCAL_KV_CACHE_SIZE: usize = 1_000;

#[derive(Serialize, Deserialize)]
pub(crate) struct ScrollContext {
    pub split_metadatas: Vec<SplitMetadata>,
    pub search_request: SearchRequest,
    pub indexes_metas_for_leaf_search: HashMap<IndexUid, IndexMetasForLeafSearch>,
    pub total_num_hits: u64,
    pub max_hits_per_page: u64,
    pub cached_partial_hits_start_offset: u64,
    pub cached_partial_hits: Vec<PartialHit>,
    pub failed_splits: Vec<SplitSearchError>,
    pub num_successful_splits: u64,
}

impl ScrollContext {
    /// Returns as many results in cache.
    pub fn get_cached_partial_hits(&self, doc_range: Range<u64>) -> &[PartialHit] {
        if doc_range.end <= doc_range.start {
            return &[];
        }
        if doc_range.start < self.cached_partial_hits_start_offset {
            return &[];
        }
        if doc_range.start
            >= self.cached_partial_hits_start_offset + self.cached_partial_hits.len() as u64
        {
            return &[];
        }
        let truncated_partial_hits = &self.cached_partial_hits
            [(doc_range.start - self.cached_partial_hits_start_offset) as usize..];
        let num_partial_hits = truncated_partial_hits
            .len()
            .min((doc_range.end - doc_range.start) as usize);
        &truncated_partial_hits[..num_partial_hits]
    }

    /// Clear cache if it wouldn't be useful, i.e. if page size is greater than SCROLL_BATCH_LEN
    pub fn clear_cache_if_unneeded(&mut self) {
        if self.search_request.max_hits > SCROLL_BATCH_LEN as u64 {
            self.cached_partial_hits.clear();
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let uncompressed_payload = serde_json::to_string(self).unwrap();
        uncompressed_payload.as_bytes().to_vec()
    }

    pub fn load(payload: &[u8]) -> anyhow::Result<Self> {
        let scroll_context =
            serde_json::from_slice(payload).context("failed to deserialize context")?;
        Ok(scroll_context)
    }

    /// Loads in the `ScrollContext` cache all the
    /// hits in range [start_offset..start_offset + SCROLL_BATCH_LEN).
    pub async fn load_batch_starting_at(
        &mut self,
        start_offset: u64,
        previous_last_hit: PartialHit,
        cluster_client: &ClusterClient,
        searcher_context: &SearcherContext,
    ) -> crate::Result<bool> {
        self.search_request.search_after = Some(previous_last_hit);
        let leaf_search_response: LeafSearchResponse = crate::root::search_partial_hits_phase(
            searcher_context,
            &self.indexes_metas_for_leaf_search,
            &self.search_request,
            &self.split_metadatas[..],
            cluster_client,
        )
        .await?;
        self.cached_partial_hits_start_offset = start_offset;
        self.cached_partial_hits = leaf_search_response.partial_hits;
        Ok(true)
    }
}

struct TrackedValue {
    content: Vec<u8>,
    _total_size_metric_guard: GaugeGuard<'static>,
}

/// In memory key value store with TTL and limited size.
///
/// Once the capacity [LOCAL_KV_CACHE_SIZE] is reached, the oldest entries are
/// removed.
///
/// Currently this store is only used for caching scroll contexts. Using it for
/// other purposes is risky as use cases would compete for its capacity.
#[derive(Clone)]
pub(crate) struct MiniKV {
    ttl_with_cache: Arc<RwLock<TtlCache<Vec<u8>, TrackedValue>>>,
}

impl Default for MiniKV {
    fn default() -> MiniKV {
        MiniKV {
            ttl_with_cache: Arc::new(RwLock::new(TtlCache::new(LOCAL_KV_CACHE_SIZE))),
        }
    }
}

impl MiniKV {
    pub async fn put(&self, key: Vec<u8>, payload: Vec<u8>, ttl: Duration) {
        let mut metric_guard =
            GaugeGuard::from_gauge(&crate::SEARCH_METRICS.searcher_local_kv_store_size_bytes);
        metric_guard.add(payload.len() as i64);
        let mut cache_lock = self.ttl_with_cache.write().await;
        cache_lock.insert(
            key,
            TrackedValue {
                content: payload,
                _total_size_metric_guard: metric_guard,
            },
            ttl,
        );
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cache_lock = self.ttl_with_cache.read().await;
        let tracked_value = cache_lock.get(key)?;
        Some(tracked_value.content.clone())
    }
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub(crate) struct ScrollKeyAndStartOffset {
    scroll_ulid: Ulid,
    pub(crate) start_offset: u64,
    // this is set to zero if there are no more documents
    pub(crate) max_hits_per_page: u32,
    pub(crate) search_after: PartialHit,
}

impl ScrollKeyAndStartOffset {
    pub fn new_with_start_offset(
        start_offset: u64,
        max_hits_per_page: u32,
        search_after: PartialHit,
    ) -> ScrollKeyAndStartOffset {
        let scroll_ulid: Ulid = Ulid::new();
        // technically we could only initialize search_after on first call to next_page, and use
        // default() before, but that feels like partial initialization.
        ScrollKeyAndStartOffset {
            scroll_ulid,
            start_offset,
            max_hits_per_page,
            search_after,
        }
    }

    pub fn next_page(
        mut self,
        found_hits_in_current_page: u64,
        last_hit: PartialHit,
    ) -> ScrollKeyAndStartOffset {
        self.start_offset += found_hits_in_current_page;
        if found_hits_in_current_page < self.max_hits_per_page as u64 {
            self.max_hits_per_page = 0;
        }
        self.search_after = last_hit;
        self
    }

    pub fn scroll_key(&self) -> [u8; 16] {
        u128::from(self.scroll_ulid).to_le_bytes()
    }
}

impl fmt::Display for ScrollKeyAndStartOffset {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let mut payload = vec![0u8; 28];
        payload[..16].copy_from_slice(&u128::from(self.scroll_ulid).to_le_bytes());
        payload[16..24].copy_from_slice(&self.start_offset.to_le_bytes());
        payload[24..28].copy_from_slice(&self.max_hits_per_page.to_le_bytes());
        serde_json::to_writer(&mut payload, &self.search_after)
            .expect("serializing PartialHit should never fail");
        let b64_payload = BASE64_STANDARD.encode(payload);
        write!(formatter, "{b64_payload}")
    }
}

impl FromStr for ScrollKeyAndStartOffset {
    type Err = &'static str;

    fn from_str(scroll_id_str: &str) -> Result<Self, Self::Err> {
        let base64_decoded: Vec<u8> = BASE64_STANDARD
            .decode(scroll_id_str)
            .map_err(|_| "scroll id is invalid base64.")?;
        if base64_decoded.len() <= 16 + 8 + 4 {
            return Err("scroll id payload is truncated");
        }
        let (scroll_ulid_bytes, from_bytes, max_hits_bytes) = (
            &base64_decoded[..16],
            &base64_decoded[16..24],
            &base64_decoded[24..28],
        );
        let scroll_ulid = u128::from_le_bytes(scroll_ulid_bytes.try_into().unwrap()).into();
        let from = u64::from_le_bytes(from_bytes.try_into().unwrap());
        let max_hits = u32::from_le_bytes(max_hits_bytes.try_into().unwrap());
        if max_hits > 10_000 {
            return Err("scroll id is malformed");
        }
        let search_after =
            serde_json::from_slice(&base64_decoded[28..]).map_err(|_| "scroll id is malformed")?;
        Ok(ScrollKeyAndStartOffset {
            scroll_ulid,
            start_offset: from,
            max_hits_per_page: max_hits,
            search_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use quickwit_proto::search::PartialHit;

    use crate::scroll_context::ScrollKeyAndStartOffset;

    #[test]
    fn test_scroll_id() {
        let partial_hit = PartialHit {
            sort_value: None,
            sort_value2: None,
            split_id: "split".to_string(),
            segment_ord: 1,
            doc_id: 2,
        };
        let scroll = ScrollKeyAndStartOffset::new_with_start_offset(10, 100, partial_hit);
        let scroll_str = scroll.to_string();
        let ser_deser_scroll = ScrollKeyAndStartOffset::from_str(&scroll_str).unwrap();
        assert_eq!(scroll, ser_deser_scroll);
    }
}
