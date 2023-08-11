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

use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_default::WithAuthExt;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::Subscription;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_common::rand::append_random_suffix;
use quickwit_config::GcpPubSubSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use serde_json::Value as JsonValue;
use tokio::time;
use tracing::{debug, info};

use super::SourceActor;
use crate::actors::DocProcessor;
use crate::source::{
    BatchBuilder, Source, SourceContext, SourceExecutionContext, TypedSourceFactory,
};

const BATCH_NUM_BYTES_LIMIT: u64 = 5_000_000;
const DEFAULT_MAX_MESSAGES_PER_PULL: i32 = 1_000;
// const DEFAULT_PULL_PARALLELISM: u64 = 1;

pub struct GcpPubSubSourceFactory;

#[async_trait]
impl TypedSourceFactory for GcpPubSubSourceFactory {
    type Source = GcpPubSubSource;
    type Params = GcpPubSubSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: GcpPubSubSourceParams,
        _checkpoint: SourceCheckpoint, // TODO: Use checkpoint!
    ) -> anyhow::Result<Self::Source> {
        GcpPubSubSource::try_new(ctx, params).await
    }
}

#[derive(Default)]
pub struct GcpPubSubSourceState {
    /// Number of bytes processed by the source.
    num_bytes_processed: u64,
    /// Number of messages processed by the source.
    num_messages_processed: u64,
    /// Current position of the source, i.e. the position of the last message processed.
    current_position: Position,
    /// Whether the subscription is empty.
    subscription_is_empty: bool,
}

pub struct GcpPubSubSource {
    ctx: Arc<SourceExecutionContext>,
    subscription_name: String,
    subscription: Subscription,
    state: GcpPubSubSourceState,
    backfill_mode_enabled: bool,
    partition_id: PartitionId,
    // Parallelism will be enabled in next PR
    // pull_parallelism: u64,
    max_messages_per_pull: i32,
}

impl GcpPubSubSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: GcpPubSubSourceParams,
    ) -> anyhow::Result<Self> {
        let subscription_name = params.subscription;
        let backfill_mode_enabled = params.enable_backfill_mode;
        // let pull_parallelism = params.pull_parallelism.unwrap_or(DEFAULT_PULL_PARALLELISM);
        let max_messages_per_pull = params
            .max_messages_per_pull
            .unwrap_or(DEFAULT_MAX_MESSAGES_PER_PULL);
        let client_config = ClientConfig::default()
            .with_auth()
            .await
            .context("Failed to authenticate GCP PubSub source.")?;
        let client = Client::new(client_config)
            .await
            .context("Failed to create GCP PubSub client.")?;
        let subscription = client.subscription(&subscription_name);
        // TODO: replace with "<node_id>/<index_id>/<source_id>/<pipeline_ord>"
        let partition_id = append_random_suffix(&format!("gpc-pubsub-{subscription_name}"));
        let partition_id = PartitionId::from(partition_id);

        info!(
            index_id=%ctx.index_uid.index_id(),
            source_id=%ctx.source_config.source_id,
            subscription=%subscription_name,
            max_messages_per_pull=%max_messages_per_pull,
            // parallelism=%pull_parallelism,
            "Starting GCP PubSub source."
        );

        Ok(Self {
            ctx,
            subscription_name,
            subscription,
            state: GcpPubSubSourceState::default(),
            backfill_mode_enabled,
            partition_id,
            // pull_parallelism,
            max_messages_per_pull,
        })
    }

    fn should_exit(&self) -> bool {
        self.backfill_mode_enabled && self.state.subscription_is_empty
    }
}

#[async_trait]
impl Source for GcpPubSubSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut batch: BatchBuilder = BatchBuilder::default();
        let deadline = time::sleep(*quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        // TODO: lets add parallelism support in next PR
        // TODO: ensure we ACK the message after being commit: at least once
        // TODO: ensure we increase_ack_deadline for the items
        loop {
            tokio::select! {
                _ = self.pull_message_batch(&mut batch) => {
                    if batch.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }
        // TODO: need to wait for all the id to be ack for at_least_once
        if self.should_exit() {
            info!(subscription=%self.subscription_name, "Reached end of subscription.");
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        if !batch.checkpoint_delta.is_empty() {
            debug!(
                num_bytes=%batch.num_bytes,
                num_docs=%batch.docs.len(),
                num_millis=%now.elapsed().as_millis(),
                "Sending doc batch to indexer.");
            let message = batch.build();
            ctx.send_message(doc_processor_mailbox, message).await?;
        }
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        _checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        // TODO: add ack of ids
        Ok(())
    }

    fn name(&self) -> String {
        format!(
            "GcpPubSubSource{{source_id={}}}",
            self.ctx.source_config.source_id
        )
    }

    fn observable_state(&self) -> JsonValue {
        JsonValue::Object(Default::default())
    }
}

impl GcpPubSubSource {
    async fn pull_message_batch(&mut self, batch: &mut BatchBuilder) -> anyhow::Result<()> {
        let messages = self
            .subscription
            .pull(self.max_messages_per_pull, None)
            .await
            .context("Failed to pull messages from subscription.")?;

        let Some(last_message) = messages.last() else {
            self.state.subscription_is_empty = true;
            return Ok(());
        };
        let message_id = last_message.message.message_id.clone();
        let publish_timestamp_millis = last_message
            .message
            .publish_time
            .as_ref()
            .map(|timestamp| timestamp.seconds * 1_000 + (timestamp.nanos as i64 / 1_000_000))
            .unwrap_or(0); // TODO: Replace with now UTC millis.

        for message in messages {
            message.ack().await?; // TODO: remove ACK here when doing at least once
            self.state.num_messages_processed += 1;
            self.state.num_bytes_processed += message.message.data.len() as u64;
            let doc = Bytes::from(message.message.data);
            batch.push(doc);
        }
        let to_position = Position::from(format!(
            "{}:{message_id}:{publish_timestamp_millis}",
            self.state.num_messages_processed
        ));
        let from_position = mem::replace(&mut self.state.current_position, to_position.clone());

        batch
            .checkpoint_delta
            .record_partition_delta(self.partition_id.clone(), from_position, to_position)
            .context("Failed to record partition delta.")?;
        Ok(())
    }
}
