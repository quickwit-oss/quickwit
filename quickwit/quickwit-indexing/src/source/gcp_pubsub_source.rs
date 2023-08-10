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

use std::borrow::BorrowMut;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use google_cloud_default::WithAuthExt;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::Subscription;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::GcpPubSubSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use serde_json::Value as JsonValue;
use tokio::time;
use tracing::{info, warn};

use super::SourceActor;
use crate::actors::DocProcessor;
use crate::source::{
    BatchBuilder, Source, SourceContext, SourceExecutionContext, TypedSourceFactory,
};

const BATCH_NUM_BYTES_LIMIT: u64 = 5_000_000;
const DEFAULT_MAX_MESSAGES_PER_PULL: i32 = 1000;
// const DEFAULT_PULL_PARALLELISM: u64 = 10; // TODO: is 10 too high as a default?

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
    subscription_is_empty: bool,
    doc_count: u64,
}

pub struct GcpPubSubSource {
    ctx: Arc<SourceExecutionContext>,
    subscription: String,
    state: GcpPubSubSourceState,
    subscription_source: Subscription,
    backfill_mode_enabled: bool,
    partition_id: String,
    // Parallelism will be enabled in next PR
    // pull_parallelism: u64,
    max_messages_per_pull: i32,
}

impl GcpPubSubSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: GcpPubSubSourceParams,
    ) -> anyhow::Result<Self> {
        let subscription = params.subscription.clone();
        let backfill_mode_enabled = params.enable_backfill_mode;
        // let pull_parallelism = params.pull_parallelism.unwrap_or(DEFAULT_PULL_PARALLELISM);
        let max_messages_per_pull = match params.max_messages_per_pull {
            Some(max) => max,
            None => DEFAULT_MAX_MESSAGES_PER_PULL,
        };

        let config = match params.credentials {
            Some(_credentials) => todo!("Add specific credentials file config"),
            None => ClientConfig::default().with_auth(),
        }
        .await
        .expect("Failed to use GCP credentials");

        let client = Client::new(config)
            .await
            .expect("Failed to create GcpPubSub client");
        let subscription_source = client.subscription(&subscription);

        info!(
            index_id=%ctx.index_uid.index_id(),
            source_id=%ctx.source_config.source_id,
            subscription=%subscription,
            // parallelism=%pull_parallelism,
            "Starting GcpPubSub source."
        );

        Ok(GcpPubSubSource {
            ctx,
            subscription,
            state: GcpPubSubSourceState::default(),
            subscription_source,
            backfill_mode_enabled,
            // TODO: get the real value
            partition_id: "<node_id>/<pipeline_ord>".to_string(),
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
        info!("GcpPubSub beginning batch");
        let now = Instant::now();
        let mut batch: BatchBuilder = BatchBuilder::default();
        let deadline = time::sleep(*quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        // TODO: lets add parallelism support in next PR
        // TODO: ensure we ACK the message after being commit: at least once
        // TODO: ensure we increase_ack_deadline for the items
        loop {
            tokio::select! {
                _ = self.pull_message_batch(batch.borrow_mut()) => {
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

        if self.should_exit() {
            info!(subscription = %self.subscription, "Reached end of subscription.");
            ctx.ask(doc_processor_mailbox, batch.build_force())
                .await
                .context("Failed to force commit last batch!")?;
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }

        if batch.checkpoint_delta.is_empty() {
            self.state.subscription_is_empty = true
        } else {
            info!(
                num_docs=%batch.docs.len(),
                num_bytes=%batch.num_bytes,
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
        anyhow::Ok(())
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
        let event = self
            .subscription_source
            .pull(self.max_messages_per_pull, None)
            .await;

        let messages = event.map_err(|err| {
            warn!("{err}");
            ActorExitStatus::from(anyhow!("GcpPubSub encountered an error."))
        })?;

        let length = messages.len();
        info!("GcpPubSub pulled {length} messages");
        let initial_position = self.state.doc_count;

        for message in messages {
            message.ack().await?; // TODO: remove ACK here when doing at least once
            let num_bytes = message.message.data.len() as u64;
            batch.push(message.message.data.into(), num_bytes);
            self.state.doc_count += 1
        }

        let first_position = Position::from(initial_position);
        let last_position = Position::from(self.state.doc_count);
        let partition_id = PartitionId::from(self.partition_id.clone());

        if first_position != last_position {
            batch
                .checkpoint_delta
                .record_partition_delta(partition_id, first_position, last_position)
                .context("Failed to record partition delta.")?;
        }
        Ok(())
    }
}
