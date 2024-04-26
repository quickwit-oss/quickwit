// Copyright (C) 2024 Quickwit, Inc.
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

mod broadcast;
mod debouncing;
mod fetch;
mod idle;
mod ingester;
mod metrics;
mod models;
mod mrecord;
mod mrecordlog_utils;
mod rate_meter;
mod replication;
mod router;
mod routing_table;
mod state;
mod workbench;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::{Add, AddAssign};
use std::time::Duration;
use std::{env, fmt};

pub use broadcast::{setup_local_shards_update_listener, LocalShardsUpdate, ShardInfo, ShardInfos};
use bytes::{BufMut, BytesMut};
use bytesize::ByteSize;
use quickwit_common::tower::Pool;
use quickwit_proto::ingest::ingester::IngesterServiceClient;
use quickwit_proto::ingest::router::{IngestRequestV2, IngestSubrequest};
use quickwit_proto::ingest::{CommitTypeV2, DocBatchV2};
use quickwit_proto::types::{IndexId, NodeId};
use tracing::{error, info};

pub use self::fetch::{FetchStreamError, MultiFetchStream};
pub use self::ingester::{wait_for_ingester_decommission, wait_for_ingester_status, Ingester};
use self::mrecord::MRECORD_HEADER_LEN;
pub use self::mrecord::{decoded_mrecords, MRecord};
pub use self::router::IngestRouter;

pub type IngesterPool = Pool<NodeId, IngesterServiceClient>;

/// Identifies an ingester client, typically a source, for logging and debugging purposes.
pub type ClientId = String;

pub type LeaderId = NodeId;

pub type FollowerId = NodeId;

const IDLE_SHARD_TIMEOUT_ENV_KEY: &str = "QW_IDLE_SHARD_TIMEOUT_SECS";

const DEFAULT_IDLE_SHARD_TIMEOUT: Duration = Duration::from_secs(15 * 60); // 15 minutes

pub fn get_idle_shard_timeout() -> Duration {
    env::var(IDLE_SHARD_TIMEOUT_ENV_KEY)
        .ok()
        .and_then(|idle_shard_timeout_str| {
            if let Ok(idle_shard_timeout_secs) = idle_shard_timeout_str.parse::<u64>() {
                info!("overriding idle shard timeout to {idle_shard_timeout_secs} seconds");
                Some(idle_shard_timeout_secs)
            } else {
                error!(
                    "failed to parse environment variable \
                     `{IDLE_SHARD_TIMEOUT_ENV_KEY}={idle_shard_timeout_str}`"
                );
                None
            }
        })
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_IDLE_SHARD_TIMEOUT)
}

const INGEST_ROUTER_BUFFER_SIZE_ENV_KEY: &str = "QW_INGEST_ROUTER_BUFFER_SIZE_BYTES";

const DEFAULT_INGEST_ROUTER_BUFFER_SIZE: ByteSize = ByteSize::mib(if cfg!(test) { 8 } else { 256 }); // 256 MiB

pub(crate) fn get_ingest_router_buffer_size() -> ByteSize {
    env::var(INGEST_ROUTER_BUFFER_SIZE_ENV_KEY)
        .ok()
        .and_then(|buffer_size_bytes_str| {
            if let Ok(buffer_size) = buffer_size_bytes_str.parse::<ByteSize>() {
                info!("overriding ingest router buffer size to {buffer_size}");
                Some(buffer_size)
            } else {
                error!(
                    "failed to parse environment variable \
                     `{INGEST_ROUTER_BUFFER_SIZE_ENV_KEY}={buffer_size_bytes_str}`"
                );
                None
            }
        })
        .unwrap_or(DEFAULT_INGEST_ROUTER_BUFFER_SIZE)
}

/// Helper struct to build a [`DocBatchV2`]`.
#[derive(Debug, Default)]
pub struct DocBatchV2Builder {
    doc_buffer: BytesMut,
    doc_lengths: Vec<u32>,
}

impl DocBatchV2Builder {
    /// Adds a document to the batch.
    pub fn add_doc(&mut self, doc: &[u8]) {
        self.doc_lengths.push(doc.len() as u32);
        self.doc_buffer.put(doc);
    }

    /// Builds the [`DocBatchV2`], returning `None` if the batch is empty.
    pub fn build(self) -> Option<DocBatchV2> {
        if self.doc_lengths.is_empty() {
            return None;
        }
        let doc_batch = DocBatchV2 {
            doc_buffer: self.doc_buffer.freeze(),
            doc_lengths: self.doc_lengths,
        };
        Some(doc_batch)
    }
}

/// Helper struct to build an [`IngestRequestV2`].
#[derive(Debug, Default)]
pub struct IngestRequestV2Builder {
    per_index_id_doc_batch_builders: HashMap<IndexId, (u32, DocBatchV2Builder)>,
    subrequest_id_sequence: u32,
}

impl IngestRequestV2Builder {
    /// Adds a document to the request.
    pub fn add_doc(&mut self, index_id: IndexId, doc: &[u8]) -> u32 {
        match self.per_index_id_doc_batch_builders.entry(index_id) {
            Entry::Occupied(mut entry) => {
                let (subrequest_id, doc_batch_builder) = entry.get_mut();
                doc_batch_builder.add_doc(doc);
                *subrequest_id
            }
            Entry::Vacant(entry) => {
                let subrequest_id = self.subrequest_id_sequence;
                self.subrequest_id_sequence += 1;
                let mut doc_batch_builder = DocBatchV2Builder::default();
                doc_batch_builder.add_doc(doc);
                entry.insert((subrequest_id, doc_batch_builder));
                subrequest_id
            }
        }
    }

    /// Builds the [`IngestRequestV2`], returning `None` if the request is empty.
    pub fn build(self, source_id: &str, commit_type: CommitTypeV2) -> Option<IngestRequestV2> {
        let subrequests: Vec<IngestSubrequest> = self
            .per_index_id_doc_batch_builders
            .into_iter()
            .flat_map(|(index_id, (subrequest_id, doc_batch_builder))| {
                let doc_batch = doc_batch_builder.build()?;
                let ingest_subrequest = IngestSubrequest {
                    subrequest_id,
                    index_id,
                    source_id: source_id.to_string(),
                    doc_batch: Some(doc_batch),
                };
                Some(ingest_subrequest)
            })
            .collect();

        if subrequests.is_empty() {
            return None;
        }
        let ingest_request = IngestRequestV2 {
            subrequests,
            commit_type: commit_type as i32,
        };
        Some(ingest_request)
    }
}

pub(super) fn estimate_size(doc_batch: &DocBatchV2) -> ByteSize {
    let estimate = doc_batch.num_bytes() + doc_batch.num_docs() * MRECORD_HEADER_LEN;
    ByteSize(estimate as u64)
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct RateMibPerSec(pub u16);

impl fmt::Display for RateMibPerSec {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}MiB/s", self.0)
    }
}

impl PartialEq<u16> for RateMibPerSec {
    fn eq(&self, other: &u16) -> bool {
        self.0 == *other
    }
}

impl Add<RateMibPerSec> for RateMibPerSec {
    type Output = RateMibPerSec;

    #[inline(always)]
    fn add(self, rhs: RateMibPerSec) -> Self::Output {
        RateMibPerSec(self.0 + rhs.0)
    }
}

impl AddAssign<RateMibPerSec> for RateMibPerSec {
    #[inline(always)]
    fn add_assign(&mut self, rhs: RateMibPerSec) {
        self.0 += rhs.0;
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_doc_batch_builder() {
        let doc_batch_builder = DocBatchV2Builder::default();
        let doc_batch_opt = doc_batch_builder.build();
        assert!(doc_batch_opt.is_none());

        let mut doc_batch_builder = DocBatchV2Builder::default();
        doc_batch_builder.add_doc(b"Hello, ");
        doc_batch_builder.add_doc(b"World!");
        let doc_batch = doc_batch_builder.build().unwrap();

        assert_eq!(doc_batch.num_docs(), 2);
        assert_eq!(doc_batch.num_bytes(), 21);
        assert_eq!(doc_batch.doc_lengths, [7, 6]);
        assert_eq!(doc_batch.doc_buffer, Bytes::from(&b"Hello, World!"[..]));
    }

    #[test]
    fn test_ingest_request_builder() {
        let ingest_request_builder = IngestRequestV2Builder::default();
        let ingest_request_opt = ingest_request_builder.build("test-source", CommitTypeV2::Auto);
        assert!(ingest_request_opt.is_none());

        let mut ingest_request_builder = IngestRequestV2Builder::default();
        ingest_request_builder.add_doc("test-index-foo".to_string(), b"Hello, ");
        ingest_request_builder.add_doc("test-index-foo".to_string(), b"World!");

        ingest_request_builder.add_doc("test-index-bar".to_string(), b"Hola, ");
        ingest_request_builder.add_doc("test-index-bar".to_string(), b"Mundo!");
        let mut ingest_request = ingest_request_builder
            .build("test-source", CommitTypeV2::Auto)
            .unwrap();

        ingest_request
            .subrequests
            .sort_by(|left, right| left.index_id.cmp(&right.index_id).reverse());

        assert_eq!(ingest_request.subrequests.len(), 2);
        assert_eq!(ingest_request.subrequests[0].index_id, "test-index-foo");
        assert_eq!(ingest_request.subrequests[0].source_id, "test-source");
        assert_eq!(
            ingest_request.subrequests[0]
                .doc_batch
                .as_ref()
                .unwrap()
                .num_docs(),
            2
        );
        assert_eq!(
            ingest_request.subrequests[0]
                .doc_batch
                .as_ref()
                .unwrap()
                .num_bytes(),
            21
        );
        assert_eq!(
            ingest_request.subrequests[0]
                .doc_batch
                .as_ref()
                .unwrap()
                .doc_lengths,
            [7, 6]
        );
        assert_eq!(
            ingest_request.subrequests[0]
                .doc_batch
                .as_ref()
                .unwrap()
                .doc_buffer,
            Bytes::from(&b"Hello, World!"[..])
        );

        assert_eq!(ingest_request.subrequests[1].index_id, "test-index-bar");
        assert_eq!(ingest_request.subrequests[1].source_id, "test-source");
        assert_eq!(
            ingest_request.subrequests[1]
                .doc_batch
                .as_ref()
                .unwrap()
                .num_docs(),
            2
        );
        assert_eq!(
            ingest_request.subrequests[1]
                .doc_batch
                .as_ref()
                .unwrap()
                .num_bytes(),
            20
        );
        assert_eq!(
            ingest_request.subrequests[1]
                .doc_batch
                .as_ref()
                .unwrap()
                .doc_lengths,
            [6, 6]
        );
        assert_eq!(
            ingest_request.subrequests[1]
                .doc_batch
                .as_ref()
                .unwrap()
                .doc_buffer,
            Bytes::from(&b"Hola, Mundo!"[..])
        );
    }

    #[test]
    fn test_estimate_size() {
        let doc_batch = DocBatchV2 {
            doc_buffer: Vec::new().into(),
            doc_lengths: Vec::new(),
        };
        assert_eq!(estimate_size(&doc_batch), ByteSize(0));

        let doc_batch = DocBatchV2 {
            doc_buffer: vec![0u8; 100].into(),
            doc_lengths: vec![10, 20, 30],
        };
        assert_eq!(estimate_size(&doc_batch), ByteSize(118));
    }
}
