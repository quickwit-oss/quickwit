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
            Self::Retiring => "retiring",
            Self::Decommissioning => "decommissioning",
            Self::Decommissioned => "decommissioned",
            Self::Failed => "failed",
        }
    }

    pub fn from_json_str_name(value: &str) -> Option<Self> {
        match value {
            "unspecified" => Some(Self::Unspecified),
            "initializing" => Some(Self::Initializing),
            "ready" => Some(Self::Ready),
            "retiring" => Some(Self::Retiring),
            "decommissioning" => Some(Self::Decommissioning),
            "decommissioned" => Some(Self::Decommissioned),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn accepts_write_requests(&self) -> bool {
        matches!(self, Self::Ready | Self::Retiring)
    }
}

impl std::fmt::Display for IngesterStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_json_str_name())
    }
}

impl OpenFetchStreamRequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl PersistSuccess {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}

impl TruncateShardsSubrequest {
    pub fn queue_id(&self) -> QueueId {
        queue_id(self.index_uid(), &self.source_id, self.shard_id())
    }
}
