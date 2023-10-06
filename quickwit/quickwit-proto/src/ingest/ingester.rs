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

use bytes::Bytes;

use crate::types::{queue_id, QueueId};

include!("../codegen/quickwit/quickwit.ingest.ingester.rs");

pub use ingester_service_grpc_server::IngesterServiceGrpcServer;

impl FetchResponseV2 {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }

    pub fn docs(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.doc_batch.iter().flat_map(|doc_batch| doc_batch.docs())
    }

    pub fn num_docs(&self) -> usize {
        if let Some(doc_batch) = &self.doc_batch {
            doc_batch.doc_lengths.len()
        } else {
            0
        }
    }

    pub fn to_position_inclusive(&self) -> Option<u64> {
        let Some(doc_batch) = &self.doc_batch else {
            return None;
        };
        let num_docs = doc_batch.num_docs() as u64;
        Some(self.from_position_inclusive + num_docs - 1)
    }
}

impl OpenFetchStreamRequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl PersistSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl PersistSuccess {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl SynReplicationMessage {
    pub fn into_open_request(self) -> Option<OpenReplicationStreamRequest> {
        match self.message {
            Some(syn_replication_message::Message::OpenRequest(open_request)) => Some(open_request),
            _ => None,
        }
    }

    pub fn into_replicate_request(self) -> Option<ReplicateRequest> {
        match self.message {
            Some(syn_replication_message::Message::ReplicateRequest(replicate_request)) => {
                Some(replicate_request)
            }
            _ => None,
        }
    }

    pub fn new_open_request(open_request: OpenReplicationStreamRequest) -> Self {
        Self {
            message: Some(syn_replication_message::Message::OpenRequest(open_request)),
        }
    }

    pub fn new_replicate_request(replicate_request: ReplicateRequest) -> Self {
        Self {
            message: Some(syn_replication_message::Message::ReplicateRequest(
                replicate_request,
            )),
        }
    }
}

impl AckReplicationMessage {
    pub fn into_open_response(self) -> Option<OpenReplicationStreamResponse> {
        match self.message {
            Some(ack_replication_message::Message::OpenResponse(open_response)) => {
                Some(open_response)
            }
            _ => None,
        }
    }

    pub fn new_open_response(open_response: OpenReplicationStreamResponse) -> Self {
        Self {
            message: Some(ack_replication_message::Message::OpenResponse(
                open_response,
            )),
        }
    }

    pub fn new_replicate_response(replicate_response: ReplicateResponse) -> Self {
        Self {
            message: Some(ack_replication_message::Message::ReplicateResponse(
                replicate_response,
            )),
        }
    }
}

impl ReplicateSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }

    pub fn to_position_inclusive(&self) -> Option<u64> {
        let Some(doc_batch) = &self.doc_batch else {
            return self.from_position_exclusive;
        };
        let num_docs = doc_batch.num_docs() as u64;

        match self.from_position_exclusive {
            Some(from_position_exclusive) => Some(from_position_exclusive + num_docs),
            None => Some(num_docs - 1),
        }
    }
}

impl TruncateSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::DocBatchV2;

    #[test]
    fn test_fetch_response_to_position_inclusive() {
        let mut response = FetchResponseV2 {
            index_uid: "test-index".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 0,
            from_position_inclusive: 0,
            doc_batch: None,
        };
        assert_eq!(response.to_position_inclusive(), None);

        response.doc_batch = Some(DocBatchV2 {
            doc_buffer: Bytes::from_static(b"test-doc"),
            doc_lengths: vec![8],
        });
        assert_eq!(response.to_position_inclusive(), Some(0));

        response.doc_batch = Some(DocBatchV2 {
            doc_buffer: Bytes::from_static(b"test-doctest-doc"),
            doc_lengths: vec![8, 8],
        });
        assert_eq!(response.to_position_inclusive(), Some(1));
    }

    #[test]
    fn test_replicate_subrequest_to_position_inclusive() {
        let mut subrequest = ReplicateSubrequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 0,
            from_position_exclusive: None,
            doc_batch: None,
        };
        assert_eq!(subrequest.to_position_inclusive(), None);

        subrequest.from_position_exclusive = Some(0);
        assert_eq!(subrequest.to_position_inclusive(), Some(0));

        subrequest.doc_batch = Some(DocBatchV2 {
            doc_buffer: Bytes::from_static(b"test-doc"),
            doc_lengths: vec![8],
        });
        assert_eq!(subrequest.to_position_inclusive(), Some(1));

        subrequest.from_position_exclusive = None;
        assert_eq!(subrequest.to_position_inclusive(), Some(0));
    }
}
