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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt, mem};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_auth::credentials::CredentialsFile;
use google_cloud_gax::retry::RetrySetting;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::Subscription;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_common::rand::append_random_suffix;
use quickwit_config::GcpPubSubSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use serde_json::{json, Value as JsonValue};
use tokio::time;
use tracing::{debug, info, warn};
use ulid::Ulid;

use super::SourceActor;
use crate::actors::DocProcessor;
use crate::source::{
    BatchBuilder, Source, SourceContext, SourceExecutionContext, TypedSourceFactory,
};

const BATCH_NUM_BYTES_LIMIT: u64 = 5_000_000;
const DEFAULT_MAX_MESSAGES_PER_PULL: i32 = 1_000;
type BatchId = Ulid; // Ulids are monotonically increasing
type MessageIDs = Vec<String>;

pub struct GcpPubSubSourceFactory;

#[async_trait]
impl TypedSourceFactory for GcpPubSubSourceFactory {
    type Source = GcpPubSubSource;
    type Params = GcpPubSubSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: GcpPubSubSourceParams,
        _checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        GcpPubSubSource::try_new(ctx, params).await
    }
}

/// Checks whether we can establish a connection.
pub(crate) async fn check_connectivity(params: &GcpPubSubSourceParams) -> anyhow::Result<()> {
    connect_gcp_pubsub(params).await?;
    Ok(())
}

async fn connect_gcp_pubsub(params: &GcpPubSubSourceParams) -> anyhow::Result<Subscription> {
    let mut client_config: ClientConfig = match &params.credentials_file {
        Some(credentials_file) => {
            let credentials = CredentialsFile::new_from_file(credentials_file.clone())
                .await
                .with_context(|| {
                    format!("Failed to load GCP PubSub credentials file from `{credentials_file}`.")
                })?;
            ClientConfig::default().with_credentials(credentials).await
        }
        _ => ClientConfig::default().with_auth().await,
    }
    .context("Failed to create GCP PubSub client config.")?;

    if params.project_id.is_some() {
        client_config.project_id = params.project_id.clone()
    }

    let client = Client::new(client_config)
        .await
        .context("Failed to create GCP PubSub client.")?;
    let subscription = client.subscription(&params.subscription);
    if !subscription.exists(Some(RetrySetting::default())).await? {
        anyhow::bail!(
            "GCP PubSub subscription `{}` does not exist.",
            &params.subscription
        );
    }
    Ok(subscription)
}

#[derive(Default)]
pub struct GcpPubSubSourceState {
    /// Number of bytes processed by the source.
    num_bytes_processed: u64,
    /// Number of messages processed by the source.
    num_messages_processed: u64,
    /// Current position of the source, i.e. the position of the last message processed.
    current_position: Position,
    // Number of invalid messages, i.e., that were empty or could not be parsed.
    num_invalid_messages: u64,
    /// Number of time we looped without getting a single message
    num_consecutive_empty_batches: u64,
}

pub struct GcpPubSubSource {
    ctx: Arc<SourceExecutionContext>,
    subscription_name: String,
    subscription: Subscription,
    state: GcpPubSubSourceState,
    backfill_mode_enabled: bool,
    partition_id: PartitionId,
    max_messages_per_pull: i32,
    // in_flight_batches contains messages that are not ack.
    // batchId is send to the quickwit checkpoint, so that we track until where
    // we should ack when quickit storage has commited the msgs
    // TODO: we should increase_ack_deadline for the messageIDs
    in_flight_batches: VecDeque<(BatchId, MessageIDs)>,
}

impl fmt::Debug for GcpPubSubSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("GcpPubSubSource")
            .field("index_id", &self.ctx.index_uid.index_id())
            .field("source_id", &self.ctx.source_config.source_id)
            .field("subscription", &self.subscription)
            .finish()
    }
}

impl GcpPubSubSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: GcpPubSubSourceParams,
    ) -> anyhow::Result<Self> {
        let subscription = connect_gcp_pubsub(&params).await?;
        let subscription_name = params.subscription;
        let backfill_mode_enabled = params.enable_backfill_mode;
        let max_messages_per_pull = params
            .max_messages_per_pull
            .unwrap_or(DEFAULT_MAX_MESSAGES_PER_PULL);

        // TODO: replace with "<node_id>/<index_id>/<source_id>/<pipeline_ord>" !
        let partition_id = append_random_suffix(&format!("gpc-pubsub-{subscription_name}"));
        let partition_id = PartitionId::from(partition_id);
        info!(
            index_id=%ctx.index_uid.index_id(),
            source_id=%ctx.source_config.source_id,
            subscription=%subscription_name,
            max_messages_per_pull=%max_messages_per_pull,
            "Starting GCP PubSub source."
        );
        Ok(Self {
            ctx,
            subscription_name,
            subscription,
            state: GcpPubSubSourceState::default(),
            backfill_mode_enabled,
            partition_id,
            max_messages_per_pull,
            in_flight_batches: VecDeque::new(),
        })
    }

    fn should_exit(&self) -> bool {
        self.backfill_mode_enabled
            && self.state.num_consecutive_empty_batches > 3
            && self.in_flight_batches.is_empty()
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
        loop {
            tokio::select! {
                resp = self.pull_message_batch(&mut batch) => {
                    if let Err(err) = resp {
                        warn!("Failed to pull messages from subscription `{}`: {:?}", self.subscription_name, err);
                    }
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

        if batch.num_bytes > 0 {
            self.state.num_consecutive_empty_batches = 0
        } else {
            self.state.num_consecutive_empty_batches += 1
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
        checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        if let Some(Position::Offset(offset_str)) =
            checkpoint.position_for_partition(&self.partition_id)
        {
            let commited_until = offset_str.parse::<Ulid>()?;
            while let Some((position, _)) = self.in_flight_batches.front() {
                if position > &commited_until {
                    break;
                }
                // if we failed to ack the message, for now we just return an error that will be log
                // we might consider to push it back to the queue later, to be a bit more resilient
                // but we need to implements logics to extends ack first
                let (_, messages) = self.in_flight_batches.pop_front().unwrap();
                self.subscription
                    .ack(messages)
                    .await
                    .map_err(anyhow::Error::from)
                    .context("fail to ack some message. they might be duplicated")?
            }
        }

        Ok(())
    }

    fn name(&self) -> String {
        format!(
            "GcpPubSubSource{{source_id={}}}",
            self.ctx.source_config.source_id
        )
    }

    fn observable_state(&self) -> JsonValue {
        json!({
            "index_id": self.ctx.index_uid.index_id(),
            "source_id": self.ctx.source_config.source_id,
            "subscription": self.subscription_name,
            "partition_id": self.partition_id,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
            "num_consecutive_empty_batches": self.state.num_consecutive_empty_batches,
        })
    }
}

impl GcpPubSubSource {
    async fn pull_message_batch(&mut self, batch: &mut BatchBuilder) -> anyhow::Result<()> {
        let messages = self
            .subscription
            .pull(self.max_messages_per_pull, None)
            .await
            .context("Failed to pull messages from subscription.")?;

        if messages.is_empty() {
            return Ok(());
        };
        // TODO: do we want to log some stuff depending on the publish time. Like the "lag" behind
        // or put it in the position ?
        let mut message_ids: MessageIDs = Vec::with_capacity(messages.len());
        for message in messages {
            self.state.num_messages_processed += 1;
            self.state.num_bytes_processed += message.message.data.len() as u64;
            message_ids.push(message.ack_id().to_string());
            let doc: Bytes = Bytes::from(message.message.data);
            if doc.is_empty() {
                self.state.num_invalid_messages += 1;
            } else {
                batch.push(doc);
            }
        }

        let position_id: Ulid = Ulid::new();
        self.in_flight_batches.push_back((position_id, message_ids));

        let to_position = Position::from(format!("{position_id}"));
        let from_position = mem::replace(&mut self.state.current_position, to_position.clone());

        batch
            .checkpoint_delta
            .record_partition_delta(self.partition_id.clone(), from_position, to_position)
            .context("Failed to record partition delta.")?;
        Ok(())
    }
}

// TODO: first implementation of the test
// After we need to ensure at_least_once and concurrent pipeline
#[cfg(all(test, feature = "gcp-pubsub-emulator-tests"))]
mod gcp_pubsub_emulator_tests {
    use std::env::var;
    use std::num::NonZeroUsize;
    use std::path::PathBuf;

    use google_cloud_googleapis::pubsub::v1::PubsubMessage;
    use google_cloud_pubsub::publisher::Publisher;
    use google_cloud_pubsub::subscription::SubscriptionConfig;
    use quickwit_actors::Universe;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::setup_logging_for_tests;
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::IndexUid;
    use serde_json::json;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::{quickwit_supported_sources, SuggestTruncate};

    static GCP_TEST_PROJECT: &str = "quickwit-emulator";

    fn get_source_config(subscription: &str) -> SourceConfig {
        setup_logging_for_tests();
        var("PUBSUB_EMULATOR_HOST").expect(
            "environment variable `PUBSUB_EMULATOR_HOST` should be set when running GCP PubSub \
             source tests",
        );
        let source_id = append_random_suffix("test-gcp-pubsub-source--source");
        SourceConfig {
            source_id,
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::GcpPubSub(GcpPubSubSourceParams {
                project_id: Some(GCP_TEST_PROJECT.to_string()),
                enable_backfill_mode: true,
                subscription: subscription.to_string(),
                credentials_file: None,
                max_messages_per_pull: None,
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    async fn create_topic_and_subscription(topic: &str, subscription: &str) -> Publisher {
        let client_config = google_cloud_pubsub::client::ClientConfig {
            project_id: Some(GCP_TEST_PROJECT.to_string()),
            ..Default::default()
        };
        let client = Client::new(client_config.with_auth().await.unwrap())
            .await
            .unwrap();
        let subscription_config = SubscriptionConfig::default();

        let created_topic = client.create_topic(topic, None, None).await.unwrap();
        client
            .create_subscription(subscription, topic, subscription_config, None)
            .await
            .unwrap();
        created_topic.new_publisher(None)
    }

    fn get_partition_id(source: &Box<dyn Source>) -> String {
        source.observable_state()["partition_id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_gcp_pubsub_source_invalid_subscription() {
        let subscription =
            append_random_suffix("test-gcp-pubsub-source--invalid-subscription--subscription");
        let source_config = get_source_config(&subscription);

        let index_id = append_random_suffix("test-gcp-pubsub-source--invalid-subscription--index");
        let index_uid = IndexUid::new(&index_id);
        let metastore = metastore_for_test();
        let SourceParams::GcpPubSub(params) = source_config.clone().source_params else {
            panic!(
                "Expected `SourceParams::GcpPubSub` source params, got {:?}",
                source_config.source_params
            );
        };
        let ctx = SourceExecutionContext::for_test(
            metastore,
            index_uid,
            PathBuf::from("./queues"),
            source_config,
        );
        GcpPubSubSource::try_new(ctx, params).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_gcp_pubsub_source() {
        let universe = Universe::with_accelerated_time();

        let topic = append_random_suffix("test-gcp-pubsub-source--topic");
        let subscription = append_random_suffix("test-gcp-pubsub-source--subscription");
        let publisher = create_topic_and_subscription(&topic, &subscription).await;

        let source_config = get_source_config(&subscription);
        let source_id = source_config.source_id.clone();

        let source_loader = quickwit_supported_sources();
        let metastore = metastore_for_test();
        let index_id: String = append_random_suffix("test-gcp-pubsub-source--index");
        let index_uid = IndexUid::new(&index_id);

        let mut pubsub_messages = Vec::with_capacity(6);
        for i in 0..6 {
            let pubsub_message = PubsubMessage {
                data: format!("Message {}", i).into(),
                ..Default::default()
            };
            pubsub_messages.push(pubsub_message);
        }
        let awaiters = publisher.publish_bulk(pubsub_messages).await;
        for awaiter in awaiters {
            awaiter.get().await.unwrap();
        }
        let source = source_loader
            .load_source(
                SourceExecutionContext::for_test(
                    metastore,
                    index_uid,
                    PathBuf::from("./queues"),
                    source_config,
                ),
                SourceCheckpoint::default(),
            )
            .await
            .unwrap();

        let partition = get_partition_id(&source);
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let source_actor = SourceActor {
            source,
            doc_processor_mailbox: doc_processor_mailbox.clone(),
        };
        let (source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
        let suggest_truncate_partition = partition.clone();
        let trigger_suggest_truncate = tokio::spawn(async move {
            loop {
                let to_position = Position::from(format!("{}", Ulid::new()));
                let checkpoint: SourceCheckpoint =
                    vec![(PartitionId::from(suggest_truncate_partition.clone()), to_position)]
                        .into_iter()
                        .collect();

                let suggest_truncate_req = SuggestTruncate(checkpoint);
                tokio::time::sleep(Duration::from_millis(50)).await;
                source_mailbox.ask(suggest_truncate_req).await.unwrap();
            }
        });

        let (exit_status, exit_state) = source_handle.join().await;
        trigger_suggest_truncate.abort();
        assert!(exit_status.is_success());

        let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert_eq!(messages.len(), 1);
        let expected_docs = vec![
            "Message 0",
            "Message 1",
            "Message 2",
            "Message 3",
            "Message 4",
            "Message 5",
        ];
        assert_eq!(messages[0].docs, expected_docs);
        let expected_exit_state = json!({
            "index_id": index_id,
            "source_id": source_id,
            "subscription": subscription,
            "partition_id": partition,
            "num_bytes_processed": 54,
            "num_messages_processed": 6,
            "num_invalid_messages": 0,
            "num_consecutive_empty_batches": 4,
        });
        assert_eq!(exit_state, expected_exit_state);
    }
}
