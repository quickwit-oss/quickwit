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

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, Handler, Mailbox, QueueCapacity};
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_proto::metastore::MetastoreServiceClient;
use serde::Serialize;
use tracing::info;

use crate::actors::MergePlanner;
use crate::source::{SourceActor, SuggestTruncate};

#[derive(Clone, Debug, Default, Serialize)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
    pub num_replace_operations: u64,
    pub num_empty_splits: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum PublisherType {
    MainPublisher,
    MergePublisher,
    ParquetPublisher,
}

impl PublisherType {
    pub fn actor_name(&self) -> &'static str {
        match self {
            PublisherType::MainPublisher => "Publisher",
            PublisherType::MergePublisher => "MergePublisher",
            PublisherType::ParquetPublisher => "ParquetPublisher",
        }
    }
}

/// Disconnect the merge planner loop back.
/// This message is used to cut the merge pipeline loop, and let it terminate.
#[derive(Debug)]
pub(crate) struct DisconnectMergePlanner;

#[derive(Clone)]
pub struct Publisher {
    pub(crate) publisher_type: PublisherType,
    pub(crate) metastore: MetastoreServiceClient,
    pub(crate) merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
    pub(crate) source_mailbox_opt: Option<Mailbox<SourceActor>>,
    pub(crate) counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        publisher_type: PublisherType,
        metastore: MetastoreServiceClient,
        merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
        source_mailbox_opt: Option<Mailbox<SourceActor>>,
    ) -> Publisher {
        Publisher {
            publisher_type,
            metastore,
            merge_planner_mailbox_opt,
            source_mailbox_opt,
            counters: PublisherCounters::default(),
        }
    }
}

pub(crate) fn serialize_checkpoint_delta(
    checkpoint_delta_opt: &Option<IndexCheckpointDelta>,
) -> anyhow::Result<Option<String>> {
    checkpoint_delta_opt
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize `IndexCheckpointDelta`")
}

pub(crate) async fn suggest_truncate(
    ctx: &ActorContext<Publisher>,
    source_mailbox_opt: &Option<Mailbox<SourceActor>>,
    checkpoint_delta_opt: Option<IndexCheckpointDelta>,
) {
    if let Some(source_mailbox) = source_mailbox_opt.as_ref()
        && let Some(checkpoint) = checkpoint_delta_opt
    {
        let _ = ctx
            .send_message(
                source_mailbox,
                SuggestTruncate(checkpoint.source_delta.get_source_checkpoint()),
            )
            .await;
    }
}

#[async_trait]
impl Actor for Publisher {
    type ObservableState = PublisherCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        self.publisher_type.actor_name().to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        match self.publisher_type {
            PublisherType::MainPublisher => QueueCapacity::Bounded(1),
            PublisherType::MergePublisher => QueueCapacity::Unbounded,
            PublisherType::ParquetPublisher => QueueCapacity::Bounded(1),
        }
    }
}
