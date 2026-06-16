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

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Mailbox, QueueCapacity};
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult, MetastoreServiceClient};
use serde::Serialize;
use tracing::warn;

use crate::actors::MergePlanner;
use crate::models::SharedPublishToken;
use crate::source::{SourceActor, SuggestTruncate};

#[derive(Clone, Debug, Default, Serialize)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
    pub num_replace_operations: u64,
    pub num_empty_splits: u64,
}

/// Disconnect the merge planner loop back.
/// This message is used to cut the merge pipeline loop, and let it terminate.
#[derive(Debug)]
pub(crate) struct DisconnectMergePlanner;

#[derive(Clone)]
pub struct Publisher {
    pub(crate) name: &'static str,
    pub(crate) queue_capacity: QueueCapacity,
    pub(crate) metastore: MetastoreServiceClient,
    pub(crate) merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
    #[cfg(feature = "metrics")]
    pub(crate) parquet_merge_planner_mailbox_opt:
        Option<Mailbox<super::parquet_pipeline::ParquetMergePlanner>>,
    pub(crate) source_mailbox_opt: Option<Mailbox<SourceActor>>,
    pub(crate) publish_token: SharedPublishToken,
    pub(crate) counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        name: &'static str,
        queue_capacity: QueueCapacity,
        metastore: MetastoreServiceClient,
        merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
        source_mailbox_opt: Option<Mailbox<SourceActor>>,
        publish_token: SharedPublishToken,
    ) -> Publisher {
        Publisher {
            name,
            queue_capacity,
            metastore,
            merge_planner_mailbox_opt,
            #[cfg(feature = "metrics")]
            parquet_merge_planner_mailbox_opt: None,
            source_mailbox_opt,
            publish_token,
            counters: PublisherCounters::default(),
        }
    }

    /// Sets the Parquet merge planner mailbox for merge feedback.
    /// Post-construction setter because the Publisher is created before the
    /// planner mailbox is available (bottom-up actor spawn order).
    #[cfg(feature = "metrics")]
    pub fn set_parquet_merge_planner_mailbox(
        mut self,
        mailbox: Mailbox<super::parquet_pipeline::ParquetMergePlanner>,
    ) -> Self {
        self.parquet_merge_planner_mailbox_opt = Some(mailbox);
        self
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

pub(crate) async fn publish_with_retry<T, F, Fut>(
    ctx: &ActorContext<Publisher>,
    operation_name: &str,
    mut publish: F,
) -> Result<(), ActorExitStatus>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = MetastoreResult<T>>,
{
    for retry_delay in [
        Some(Duration::from_millis(100)),
        Some(Duration::from_millis(250)),
        None,
    ] {
        let Err(error) = ctx.protect_future(publish()).await else {
            return Ok(());
        };
        let retryable = matches!(
            error,
            MetastoreError::InvalidArgument { .. }
                | MetastoreError::Unavailable(_)
                | MetastoreError::TooManyRequests
                | MetastoreError::Connection { .. }
                | MetastoreError::Internal { .. }
        );
        match retry_delay {
            Some(retry_delay) if retryable => {
                warn!(%error, operation = operation_name, "metastore publish failed, retrying");
                ctx.protect_future(tokio::time::sleep(retry_delay)).await;
            }
            _ => {
                warn!(%error, operation = operation_name, retryable, "metastore publish failed, giving up after 3 tries");
                return Err(anyhow::Error::from(error)
                    .context(format!("failed to {operation_name}"))
                    .into());
            }
        }
    }
    unreachable!("retry loop returns on the final attempt")
}

#[async_trait]
impl Actor for Publisher {
    type ObservableState = PublisherCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        self.name.to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        self.queue_capacity
    }
}
