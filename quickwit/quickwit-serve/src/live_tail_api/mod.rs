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

//! Live tail REST API: streams, as Server-Sent Events, the documents of one or several indexes
//! matching a query, as they are processed by the indexing pipelines.

mod coordinator;
mod rest_handler;

use bytesize::ByteSize;
use quickwit_cluster::Cluster;
use quickwit_proto::live_tail::{LiveTailDropReason, LiveTailServiceClient};
use quickwit_proto::metastore::MetastoreServiceClient;
use serde::Serialize;
use serde_json::Value as JsonValue;

pub(crate) use self::rest_handler::{LiveTailApi, live_tail_handler};

/// Services used by the live tail API.
#[derive(Clone)]
pub(crate) struct LiveTailContext {
    pub cluster: Cluster,
    pub metastore: MetastoreServiceClient,
    /// Live tail service of this node, if it is an indexer.
    pub local_live_tail_service_opt: Option<LiveTailServiceClient>,
    pub grpc_max_message_size: ByteSize,
}

/// Event sent to the client. The variant name is the SSE event name and the fields are the data.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum LiveTailEvent {
    /// Documents matching the query, in the order they were processed by an indexer.
    Hits {
        index_id: String,
        docs: Vec<JsonValue>,
    },
    /// Documents that matched the query but were not sent. For `lagged`, `num_dropped` counts
    /// batches of documents, not documents.
    Dropped {
        reason: LiveTailDropReason,
        num_dropped: u64,
    },
    /// Non-fatal error, e.g. an indexer could not be reached.
    Error { message: String },
}

impl LiveTailEvent {
    fn event_name(&self) -> &'static str {
        match self {
            Self::Hits { .. } => "hits",
            Self::Dropped { .. } => "dropped",
            Self::Error { .. } => "error",
        }
    }

    pub fn into_sse_event(self) -> warp::sse::Event {
        let event = warp::sse::Event::default().event(self.event_name());
        let data = serde_json::to_string(&self).expect("live tail event should be serializable");
        event.data(data)
    }
}
