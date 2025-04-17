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

use bytesize::ByteSize;

use crate::types::{Position, QueueId, queue_id};

include!("../codegen/quickwit/quickwit.ingest.ingester.rs");

pub use ingester_service_grpc_server::IngesterServiceGrpcServer;

impl FetchMessage {
    pub fn new_payload(payload: FetchPayload) -> Self {
        assert!(
            matches!(&payload.mrecord_batch, Some(batch) if !batch.mrecord_lengths.is_empty()),
            "`mrecord_batch` must be set and non-empty"
        );

        Self {
            message: Some(fetch_message::Message::Payload(payload)),
        }
    }

    pub fn new_eof(eof: FetchEof) -> Self {
        assert!(
            matches!(eof.eof_position, Some(Position::Eof(_))),
            "`eof_position` must be set"
        );

        Self {
            message: Some(fetch_message::Message::Eof(eof)),
        }
    }
}

impl FetchPayload {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }

    pub fn num_mrecords(&self) -> usize {
        if let Some(mrecord_batch) = &self.mrecord_batch {
            mrecord_batch.mrecord_lengths.len()
        } else {
            0
        }
    }

    pub fn estimate_size(&self) -> ByteSize {
        if let Some(mrecord_batch) = &self.mrecord_batch {
            mrecord_batch.estimate_size()
        } else {
            ByteSize(0)
        }
    }
}

impl IngesterStatus {
    pub fn as_json_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Initializing => "initializing",
            Self::Ready => "ready",
            Self::Decommissioning => "decommissioning",
            Self::Decommissioned => "decommissioned",
            Self::Failed => "failed",
        }
    }
}

impl OpenFetchStreamRequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl PersistSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl PersistSuccess {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl SynReplicationMessage {
    pub fn into_open_request(self) -> Option<OpenReplicationStreamRequest> {
        match self.message {
            Some(syn_replication_message::Message::OpenRequest(open_request)) => Some(open_request),
            _ => None,
        }
    }

    pub fn new_open_request(open_request: OpenReplicationStreamRequest) -> Self {
        Self {
            message: Some(syn_replication_message::Message::OpenRequest(open_request)),
        }
    }

    pub fn new_init_replica_request(init_replica_request: InitReplicaRequest) -> Self {
        Self {
            message: Some(syn_replication_message::Message::InitRequest(
                init_replica_request,
            )),
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

    pub fn new_init_replica_response(init_replica_response: InitReplicaResponse) -> Self {
        Self {
            message: Some(ack_replication_message::Message::InitResponse(
                init_replica_response,
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

impl ReplicateRequest {
    pub fn num_bytes(&self) -> usize {
        self.subrequests
            .iter()
            .flat_map(|subrequest| &subrequest.doc_batch)
            .map(|doc_batch| doc_batch.num_bytes())
            .sum()
    }
}

impl ReplicateSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl TruncateShardsSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}
