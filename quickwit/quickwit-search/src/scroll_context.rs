// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::search::{LeafSearchResponse, PartialHit, SearchRequest};
use quickwit_proto::types::IndexUid;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use ttl_cache::TtlCache;
use ulid::Ulid;

use crate::root::IndexMetasForLeafSearch;
use crate::service::SearcherContext;
use crate::ClusterClient;

/// Maximum capacity of the search after cache.
///
/// For the moment this value is hardcoded.
/// TODO make configurable.
///
/// Assuming a search context of 1MB, this can
/// amount to up to 1GB.
const SCROLL_BATCH_LEN: usize = 1_000;

#[derive(Serialize, Deserialize)]
pub(crate) struct ScrollContext {
    pub split_metadatas: Vec<SplitMetadata>,
    pub search_request: SearchRequest,
    pub indexes_metas_for_leaf_search: HashMap<IndexUid, IndexMetasForLeafSearch>,
    pub total_num_hits: u64,
    pub max_hits_per_page: u64,
    pub cached_partial_hits_start_offset: u64,
    pub cached_partial_hits: Vec<PartialHit>,
}

impl ScrollContext {
    /// Returns true if the current page in cache is incomplete.
    pub fn last_page_in_cache(&self) -> bool {
        self.cached_partial_hits.len() < SCROLL_BATCH_LEN
    }

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
        cluster_client: &ClusterClient,
        searcher_context: &SearcherContext,
    ) -> crate::Result<bool> {
        if self.cached_partial_hits_start_offset <= start_offset && self.last_page_in_cache() {
            return Ok(false);
        }
        self.search_request.max_hits = SCROLL_BATCH_LEN as u64;
        self.search_request.start_offset = start_offset;
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

#[derive(Clone)]
pub(crate) struct MiniKV {
    ttl_with_cache: Arc<RwLock<TtlCache<Vec<u8>, Vec<u8>>>>,
}

impl Default for MiniKV {
    fn default() -> MiniKV {
        MiniKV {
            ttl_with_cache: Arc::new(RwLock::new(TtlCache::new(SCROLL_BATCH_LEN))),
        }
    }
}

impl MiniKV {
    pub async fn put(&self, key: Vec<u8>, payload: Vec<u8>, ttl: Duration) {
        let mut cache_lock = self.ttl_with_cache.write().await;
        cache_lock.insert(key, payload, ttl);
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cache_lock = self.ttl_with_cache.read().await;
        let search_after_context_bytes = cache_lock.get(key)?;
        Some(search_after_context_bytes.clone())
    }
}

#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct ScrollKeyAndStartOffset {
    scroll_ulid: Ulid,
    pub(crate) start_offset: u64,
    pub(crate) max_hits_per_page: u32,
}

impl ScrollKeyAndStartOffset {
    pub fn new_with_start_offset(
        start_offset: u64,
        max_hits_per_page: u32,
    ) -> ScrollKeyAndStartOffset {
        let scroll_ulid: Ulid = Ulid::new();
        ScrollKeyAndStartOffset {
            scroll_ulid,
            start_offset,
            max_hits_per_page,
        }
    }

    pub fn next_page(mut self, found_hits_in_current_page: u64) -> ScrollKeyAndStartOffset {
        self.start_offset += found_hits_in_current_page;
        if found_hits_in_current_page < self.max_hits_per_page as u64 {
            self.max_hits_per_page = 0;
        }
        self
    }

    pub fn scroll_key(&self) -> [u8; 16] {
        u128::from(self.scroll_ulid).to_le_bytes()
    }
}

impl ToString for ScrollKeyAndStartOffset {
    fn to_string(&self) -> String {
        let mut payload = [0u8; 28];
        payload[..16].copy_from_slice(&u128::from(self.scroll_ulid).to_le_bytes());
        payload[16..24].copy_from_slice(&self.start_offset.to_le_bytes());
        payload[24..28].copy_from_slice(&self.max_hits_per_page.to_le_bytes());
        BASE64_STANDARD.encode(payload)
    }
}

impl FromStr for ScrollKeyAndStartOffset {
    type Err = &'static str;

    fn from_str(scroll_id_str: &str) -> Result<Self, Self::Err> {
        let base64_decoded: Vec<u8> = BASE64_STANDARD
            .decode(scroll_id_str)
            .map_err(|_| "scroll id is invalid base64.")?;
        if base64_decoded.len() != 16 + 8 + 4 {
            return Err("scroll id payload is not 8 bytes long");
        }
        let (scroll_ulid_bytes, from_bytes, max_hits_bytes) = (
            &base64_decoded[..16],
            &base64_decoded[16..24],
            &base64_decoded[24..28],
        );
        let scroll_ulid = u128::from_le_bytes(scroll_ulid_bytes.try_into().unwrap()).into();
        let from = u64::from_le_bytes(from_bytes.try_into().unwrap());
        let max_hits = u32::from_le_bytes(max_hits_bytes.try_into().unwrap());
        Ok(ScrollKeyAndStartOffset {
            scroll_ulid,
            start_offset: from,
            max_hits_per_page: max_hits,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::scroll_context::ScrollKeyAndStartOffset;

    #[test]
    fn test_scroll_id() {
        let scroll = ScrollKeyAndStartOffset::new_with_start_offset(10, 100);
        let scroll_str = scroll.to_string();
        let ser_deser_scroll = ScrollKeyAndStartOffset::from_str(&scroll_str).unwrap();
        assert_eq!(scroll, ser_deser_scroll);
    }
}
