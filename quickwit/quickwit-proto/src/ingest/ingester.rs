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

use crate::types::{queue_id, Position, QueueId};

include!("../codegen/quickwit/quickwit.ingest.ingester.rs");

pub use ingester_service_grpc_server::IngesterServiceGrpcServer;

impl FetchResponseV2 {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }

    pub fn num_mrecords(&self) -> usize {
        if let Some(mrecord_batch) = &self.mrecord_batch {
            mrecord_batch.mrecord_lengths.len()
        } else {
            0
        }
    }
}

impl OpenFetchStreamRequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }

    pub fn from_position_exclusive(&self) -> Position {
        self.from_position_exclusive.clone().unwrap_or_default()
    }

    pub fn to_position_inclusive(&self) -> Position {
        self.to_position_inclusive.clone().unwrap_or_default()
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

    pub fn from_position_exclusive(&self) -> Position {
        self.from_position_exclusive.clone().unwrap_or_default()
    }

    pub fn to_position_inclusive(&self) -> Position {
        self.to_position_inclusive.clone().unwrap_or_default()
    }
}

impl ReplicateSuccess {
    pub fn replication_position_inclusive(&self) -> Position {
        self.replication_position_inclusive
            .clone()
            .unwrap_or_default()
    }
}

impl TruncateSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }

    pub fn to_position_inclusive(&self) -> Position {
        self.to_position_inclusive.clone().unwrap_or_default()
    }
}
