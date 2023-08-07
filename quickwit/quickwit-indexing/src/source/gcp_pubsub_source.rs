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

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_default::WithAuthExt;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscriber::ReceivedMessage;
use google_cloud_pubsub::subscription::Subscription;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::GcpPubSubSourceParams;
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use serde_json::Value as JsonValue;
use tokio::sync::RwLock;
use tokio::time;
use tracing::{info, warn};
use uuid::Uuid;

use super::SourceActor;
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, PublishLock, RawDocBatch};
use crate::source::{Source, SourceContext, SourceExecutionContext, TypedSourceFactory};

const BATCH_NUM_BYTES_LIMIT: u64 = 5_000_000;
const DEFAULT_MAX_MESSAGES_PER_PULL: i32 = 1000;
const DEFAULT_PULL_PARALLELISM: u64 = 10; // TODO: is 10 too high as a default?

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
    ack_ids: RwLock<Vec<String>>,
    subscription_is_empty: bool,
    doc_count: RwLock<u64>,
}

pub struct GcpPubSubSource {
    ctx: Arc<SourceExecutionContext>,
    subscription: String,
    state: GcpPubSubSourceState,
    subscription_source: Subscription,
    backfill_mode_enabled: bool,
    publish_lock: PublishLock,
    partition_id: String,
    pull_parallelism: u64,
    max_messages_per_pull: i32,
}

impl GcpPubSubSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: GcpPubSubSourceParams,
    ) -> anyhow::Result<Self> {
        let subscription = params.subscription.clone();
        let backfill_mode_enabled = params.enable_backfill_mode;
        let pull_parallelism = match params.pull_parallelism {
            Some(parallelism) => parallelism,
            None => DEFAULT_PULL_PARALLELISM,
        };
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
        let publish_lock = PublishLock::default();

        info!(
            index_id=%ctx.index_uid.index_id(),
            source_id=%ctx.source_config.source_id,
            subscription=%subscription,
            parallelism=%pull_parallelism,
            "Starting GcpPubSub source."
        );

        Ok(GcpPubSubSource {
            ctx,
            subscription,
            state: GcpPubSubSourceState::default(),
            subscription_source,
            backfill_mode_enabled,
            publish_lock,
            partition_id: Uuid::new_v4().to_string(),
            pull_parallelism,
            max_messages_per_pull,
        })
    }

    fn should_exit(&self) -> bool {
        self.backfill_mode_enabled && self.state.subscription_is_empty
    }
}

#[derive(Debug, Default, Clone)]
struct BatchBuilder {
    docs: Vec<Bytes>,
    num_bytes: u64,
    checkpoint_delta: SourceCheckpointDelta,
}

impl BatchBuilder {
    fn build(self) -> RawDocBatch {
        RawDocBatch {
            docs: self.docs,
            checkpoint_delta: self.checkpoint_delta,
            force_commit: false,
        }
    }

    fn build_force(self) -> RawDocBatch {
        RawDocBatch {
            docs: self.docs,
            checkpoint_delta: self.checkpoint_delta,
            force_commit: true,
        }
    }

    // fn clear(&mut self) {
    //     self.docs.clear();
    //     self.num_bytes = 0;
    //     self.checkpoint_delta = SourceCheckpointDelta::default();
    // }

    fn push(&mut self, message: ReceivedMessage) {
        let doc = message.message.data;
        self.num_bytes += doc.len() as u64;
        self.docs.push(doc.into());
    }
}

#[async_trait]
impl Source for GcpPubSubSource {
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        info!("GcpPubSub initializing");
        let publish_lock = self.publish_lock.clone();
        ctx.send_message(doc_processor_mailbox, NewPublishLock(publish_lock))
            .await?;
        info!("GcpPubSub initialized");
        Ok(())
    }

    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        info!("GcpPubSub beginning batch");
        let now = Instant::now();
        let batch_lock = Arc::new(RwLock::new(BatchBuilder::default()));
        let deadline = time::sleep(*quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        info!("GcpPubSub pulling batch");

        loop {
            let mut handles = vec![];

            for _ in 1..self.pull_parallelism {
                handles.push(self.pull_message_batch(Arc::clone(&batch_lock)));
            }

            tokio::select! {
                _ = futures::future::join_all(handles) => {
                    let batch = batch_lock.read().await;
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

        let batch = batch_lock.read().await.clone(); // TODO: This clone is wasteful! There must be a better way
        if self.should_exit() {
            info!(subscription = %self.subscription, "Reached end of subscription.");
            ctx.ask(doc_processor_mailbox, batch.build_force())
                .await
                .context("Failed to force commit last batch!")?;
            self.ack_ids().await?;
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
        // TODO: How can we know if these are ok to ack? We need to use the checkpoint!
        self.ack_ids().await
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.ack_ids().await
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
    async fn ack_ids(&mut self) -> anyhow::Result<()> {
        let mut ack_ids = self.state.ack_ids.write().await;
        if ack_ids.is_empty() {
            return Ok(());
        }

        // TODO: For ordered GcpPubSub topics/subscriptions we need to ensure we
        // also ack in that same order!
        // TODO: We may have consumed more messages by the time we get a reply!
        // We can't just blindly clear... instead we need to keep track of which
        // ack ids were present in each checkpoint
        info!("Acking ids...");
        for chunk in ack_ids.chunks(1000) {
            self.subscription_source
                .ack(Vec::from(chunk))
                .await
                .map_err(anyhow::Error::from)?;
        }
        ack_ids.clear();
        info!("Acked!");
        Ok(())
    }

    async fn pull_message_batch(
        &self,
        batch_lock: Arc<RwLock<BatchBuilder>>,
    ) -> anyhow::Result<()> {
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
        let mut batch = batch_lock.write().await;
        let mut ack_ids = self.state.ack_ids.write().await;
        let mut doc_count = self.state.doc_count.write().await;
        let initial_position = *doc_count;

        for message in messages {
            ack_ids.push(String::from(message.ack_id()));
            batch.push(message);
            *doc_count += 1
        }

        let first_position = Position::from(initial_position);
        let last_position = Position::from(*doc_count);
        let partition_id = PartitionId::from(self.partition_id.clone());

        if first_position != last_position {
            batch
                .checkpoint_delta
                .record_partition_delta(partition_id, first_position, last_position)
                .context("Failed to record partition delta.")?;
        }

        Ok(())
    }

    // async fn increase_ack_deadline(self) {
    //     todo!("google-cloud-GcpPubSub doesn't implement this...")
    //     // BUT it does!... kind of...
    //     // It has the ability to do this for a subscriber (streaming pull).
    //     // We could potentially make a hacky copy of how it does that...
    //     // Or PR in this functionality into the Subscription struct properly.
    //     //
    //     // For now we can just set the deadline to 600 seconds on gcp and call it day?
    // }
}
