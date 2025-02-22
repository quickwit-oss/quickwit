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

use std::iter::zip;

use bytes::Bytes;
use bytesize::ByteSize;
use quickwit_common::rate_limited_error;
use quickwit_common::tower::MakeLoadShedError;
use serde::{Deserialize, Serialize};

use self::ingester::{PersistFailureReason, ReplicateFailureReason};
use self::router::IngestFailureReason;
use super::GrpcServiceError;
use crate::types::{queue_id, DocUid, NodeIdRef, Position, QueueId, ShardId, SourceUid};
use crate::{ServiceError, ServiceErrorCode};

pub mod ingester;
pub mod router;

include!("../codegen/quickwit/quickwit.ingest.rs");
pub type IngestV2Result<T> = std::result::Result<T, IngestV2Error>;

#[derive(Debug, Copy, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum RateLimitingCause {
    #[error("router load shedding")]
    RouterLoadShedding,
    #[error("load shedding")]
    LoadShedding,
    #[error("wal full (memory or disk)")]
    WalFull,
    #[error("circuit breaker")]
    CircuitBreaker,
    #[error("attempted shards rate limited")]
    AttemptedShardsRateLimited,
    #[error("all shards rate limited")]
    AllShardsRateLimited,
    #[error("unknown")]
    Unknown,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestV2Error {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("shard `{shard_id}` not found")]
    ShardNotFound { shard_id: ShardId },
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests(RateLimitingCause),
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl From<quickwit_common::tower::TimeoutExceeded> for IngestV2Error {
    fn from(_: quickwit_common::tower::TimeoutExceeded) -> IngestV2Error {
        IngestV2Error::Timeout("tower layer timeout".to_string())
    }
}

impl From<quickwit_common::tower::TaskCancelled> for IngestV2Error {
    fn from(task_cancelled: quickwit_common::tower::TaskCancelled) -> IngestV2Error {
        IngestV2Error::Internal(task_cancelled.to_string())
    }
}

impl ServiceError for IngestV2Error {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(error_msg) => {
                rate_limited_error!(limit_per_min = 6, "ingest internal error: {error_msg}");
                ServiceErrorCode::Internal
            }
            Self::ShardNotFound { .. } => ServiceErrorCode::NotFound,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests(_) => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for IngestV2Error {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        Self::TooManyRequests(RateLimitingCause::Unknown)
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }
}

impl MakeLoadShedError for IngestV2Error {
    fn make_load_shed_error() -> Self {
        IngestV2Error::TooManyRequests(RateLimitingCause::LoadShedding)
    }
}

impl Shard {
    /// List of nodes that are storing the shard (the leader, and optionally the follower).
    pub fn ingesters(&self) -> impl Iterator<Item = &NodeIdRef> + '_ {
        [Some(&self.leader_id), self.follower_id.as_ref()]
            .into_iter()
            .flatten()
            .map(|node_id| NodeIdRef::from_str(node_id))
    }

    pub fn source_uid(&self) -> SourceUid {
        SourceUid {
            index_uid: self.index_uid().clone(),
            source_id: self.source_id.clone(),
        }
    }
}

impl ShardPKey {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl DocBatchV2 {
    pub fn docs(&self) -> impl Iterator<Item = (DocUid, Bytes)> + '_ {
        zip(&self.doc_uids, &self.doc_lengths).scan(
            self.doc_buffer.clone(),
            |doc_buffer, (doc_uid, doc_len)| {
                let doc = doc_buffer.split_to(*doc_len as usize);
                Some((*doc_uid, doc))
            },
        )
    }

    pub fn into_docs(self) -> impl Iterator<Item = (DocUid, Bytes)> {
        zip(self.doc_uids, self.doc_lengths).scan(
            self.doc_buffer,
            |doc_buffer, (doc_uid, doc_len)| {
                let doc = doc_buffer.split_to(doc_len as usize);
                Some((doc_uid, doc))
            },
        )
    }

    pub fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    pub fn num_bytes(&self) -> usize {
        self.doc_buffer.len() + self.doc_lengths.len() * 4
    }

    pub fn num_docs(&self) -> usize {
        self.doc_lengths.len()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(docs: impl IntoIterator<Item = &'static str>) -> Self {
        let mut doc_uids = Vec::new();
        let mut doc_buffer = Vec::new();
        let mut doc_lengths = Vec::new();

        for (doc_uid, doc) in docs.into_iter().enumerate() {
            doc_uids.push(DocUid::for_test(doc_uid as u128));
            doc_buffer.extend(doc.as_bytes());
            doc_lengths.push(doc.len() as u32);
        }
        Self {
            doc_uids,
            doc_buffer: Bytes::from(doc_buffer),
            doc_lengths,
        }
    }
}

impl MRecordBatch {
    pub fn encoded_mrecords(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.mrecord_lengths
            .iter()
            .scan(0, |start_offset, mrecord_length| {
                let start = *start_offset;
                let end = start + *mrecord_length as usize;
                *start_offset = end;
                Some(self.mrecord_buffer.slice(start..end))
            })
    }

    pub fn is_empty(&self) -> bool {
        self.mrecord_lengths.is_empty()
    }

    pub fn estimate_size(&self) -> ByteSize {
        ByteSize((self.mrecord_buffer.len() + self.mrecord_lengths.len() * 4) as u64)
    }

    pub fn num_mrecords(&self) -> usize {
        self.mrecord_lengths.len()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(mrecords: impl IntoIterator<Item = &'static str>) -> Option<Self> {
        let mut mrecord_buffer = Vec::new();
        let mut mrecord_lengths = Vec::new();

        for mrecord in mrecords {
            mrecord_buffer.extend(mrecord.as_bytes());
            mrecord_lengths.push(mrecord.len() as u32);
        }
        Some(Self {
            mrecord_lengths,
            mrecord_buffer: Bytes::from(mrecord_buffer),
        })
    }
}

impl Shard {
    pub fn is_open(&self) -> bool {
        self.shard_state().is_open()
    }

    pub fn is_unavailable(&self) -> bool {
        self.shard_state().is_unavailable()
    }

    pub fn is_closed(&self) -> bool {
        self.shard_state().is_closed()
    }

    pub fn queue_id(&self) -> super::types::QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl ShardState {
    pub fn is_open(&self) -> bool {
        *self == ShardState::Open
    }

    pub fn is_unavailable(&self) -> bool {
        *self == ShardState::Unavailable
    }

    pub fn is_closed(&self) -> bool {
        *self == ShardState::Closed
    }

    pub fn as_json_str_name(&self) -> &'static str {
        match self {
            ShardState::Unspecified => "unspecified",
            ShardState::Open => "open",
            ShardState::Unavailable => "unavailable",
            ShardState::Closed => "closed",
        }
    }

    pub fn from_json_str_name(shard_state_json_name: &str) -> Option<Self> {
        match shard_state_json_name {
            "unspecified" => Some(Self::Unspecified),
            "open" => Some(Self::Open),
            "unavailable" => Some(Self::Unavailable),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }
}

impl ShardIds {
    pub fn queue_ids(&self) -> impl Iterator<Item = QueueId> + '_ {
        self.shard_ids
            .iter()
            .map(|shard_id| queue_id(self.index_uid(), &self.source_id, shard_id))
    }

    pub fn pkeys(&self) -> impl Iterator<Item = ShardPKey> + '_ {
        self.shard_ids.iter().map(move |shard_id| ShardPKey {
            index_uid: self.index_uid.clone(),
            source_id: self.source_id.clone(),
            shard_id: Some(shard_id.clone()),
        })
    }
}

impl ShardIdPositions {
    pub fn queue_id_positions(&self) -> impl Iterator<Item = (QueueId, Position)> + '_ {
        self.shard_positions.iter().map(|shard_position| {
            let queue_id = queue_id(self.index_uid(), &self.source_id, shard_position.shard_id());
            (queue_id, shard_position.publish_position_inclusive())
        })
    }
}

impl From<PersistFailureReason> for IngestFailureReason {
    fn from(reason: PersistFailureReason) -> Self {
        match reason {
            PersistFailureReason::Unspecified => IngestFailureReason::Unspecified,
            PersistFailureReason::ShardNotFound => IngestFailureReason::NoShardsAvailable,
            PersistFailureReason::ShardClosed => IngestFailureReason::NoShardsAvailable,
            PersistFailureReason::WalFull => IngestFailureReason::WalFull,
            PersistFailureReason::ShardRateLimited => {
                IngestFailureReason::AttemptedShardsRateLimited
            }
            PersistFailureReason::Timeout => IngestFailureReason::Timeout,
        }
    }
}

impl From<ReplicateFailureReason> for PersistFailureReason {
    fn from(reason: ReplicateFailureReason) -> Self {
        match reason {
            ReplicateFailureReason::Unspecified => PersistFailureReason::Unspecified,
            ReplicateFailureReason::ShardNotFound => PersistFailureReason::ShardNotFound,
            ReplicateFailureReason::ShardClosed => PersistFailureReason::ShardClosed,
            ReplicateFailureReason::WalFull => PersistFailureReason::WalFull,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_state_json_str_name() {
        let shard_state_json_name = ShardState::Unspecified.as_json_str_name();
        let shard_state = ShardState::from_json_str_name(shard_state_json_name).unwrap();
        assert_eq!(shard_state, ShardState::Unspecified);

        let shard_state_json_name = ShardState::Open.as_json_str_name();
        let shard_state = ShardState::from_json_str_name(shard_state_json_name).unwrap();
        assert_eq!(shard_state, ShardState::Open);

        let shard_state_json_name = ShardState::Unavailable.as_json_str_name();
        let shard_state = ShardState::from_json_str_name(shard_state_json_name).unwrap();
        assert_eq!(shard_state, ShardState::Unavailable);

        let shard_state_json_name = ShardState::Closed.as_json_str_name();
        let shard_state = ShardState::from_json_str_name(shard_state_json_name).unwrap();
        assert_eq!(shard_state, ShardState::Closed);

        assert!(ShardState::from_json_str_name("unknown").is_none());
    }
}
