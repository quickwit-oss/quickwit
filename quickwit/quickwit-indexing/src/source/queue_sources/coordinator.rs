// Copyright (C) 2024 Quickwit, Inc.
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

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::rate_limited_error;
use quickwit_config::{FileSourceMessageType, FileSourceSqs};
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::SourceType;
use quickwit_storage::StorageResolver;
use serde::Serialize;
use ulid::Ulid;

use super::helpers::QueueReceiver;
use super::local_state::QueueLocalState;
use super::message::{MessageType, PreProcessingError, ReadyMessage};
use super::shared_state::{checkpoint_messages, QueueSharedState};
use super::visibility::{spawn_visibility_task, VisibilitySettings};
use super::Queue;
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, NewPublishToken, PublishLock};
use crate::source::{SourceContext, SourceRuntime};

/// Maximum duration that the `emit_batches()` callback can wait for
/// `queue.receive()` calls. If too small, the actor loop will spin
/// un-necessarily. If too large, the actor loop will be slow to react to new
/// messages (or shutdown).
pub const RECEIVE_POLL_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Default, Serialize)]
pub struct QueueCoordinatorObservableState {
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of lines processed by the source.
    pub num_lines_processed: u64,
    /// Number of messages processed by the source.
    pub num_messages_processed: u64,
    /// Number of messages that could not be pre-processed.
    pub num_messages_failed_preprocessing: u64,
    /// Number of messages that could not be moved to in-progress.
    pub num_messages_failed_opening: u64,
}

/// The `QueueCoordinator` fetches messages from a queue, converts them into
/// record batches, and tracks the messages' state until their entire content is
/// published. Its API closely resembles the [`crate::source::Source`] trait,
/// making the implementation of queue sources straightforward.
pub struct QueueCoordinator {
    storage_resolver: StorageResolver,
    pipeline_id: IndexingPipelineId,
    source_type: SourceType,
    queue: Arc<dyn Queue>,
    queue_receiver: QueueReceiver,
    observable_state: QueueCoordinatorObservableState,
    message_type: MessageType,
    publish_lock: PublishLock,
    shared_state: QueueSharedState,
    local_state: QueueLocalState,
    publish_token: String,
    visibility_settings: VisibilitySettings,
}

impl fmt::Debug for QueueCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("QueueTracker")
            .field("index_id", &self.pipeline_id.index_uid.index_id)
            .field("queue", &self.queue)
            .finish()
    }
}

impl QueueCoordinator {
    pub fn new(
        source_runtime: SourceRuntime,
        queue: Arc<dyn Queue>,
        message_type: MessageType,
    ) -> Self {
        Self {
            shared_state: QueueSharedState {
                metastore: source_runtime.metastore,
                source_id: source_runtime.pipeline_id.source_id.clone(),
                index_uid: source_runtime.pipeline_id.index_uid.clone(),
                reacquire_grace_period: Duration::from_secs(
                    2 * source_runtime.indexing_setting.commit_timeout_secs as u64,
                ),
                last_pruning: Instant::now(),
                max_age: None,
                max_count: None,
                pruning_interval: Duration::from_secs(60),
            },
            local_state: QueueLocalState::default(),
            pipeline_id: source_runtime.pipeline_id,
            source_type: source_runtime.source_config.source_type(),
            storage_resolver: source_runtime.storage_resolver,
            queue_receiver: QueueReceiver::new(queue.clone(), RECEIVE_POLL_TIMEOUT),
            queue,
            observable_state: QueueCoordinatorObservableState::default(),
            message_type,
            publish_lock: PublishLock::default(),
            publish_token: Ulid::new().to_string(),
            visibility_settings: VisibilitySettings::from_commit_timeout(
                source_runtime.indexing_setting.commit_timeout_secs,
            ),
        }
    }

    #[cfg(feature = "sqs")]
    pub async fn try_from_sqs_config(
        config: FileSourceSqs,
        source_runtime: SourceRuntime,
    ) -> anyhow::Result<Self> {
        use super::sqs_queue::SqsQueue;
        let queue = SqsQueue::try_new(config.queue_url).await?;
        let message_type = match config.message_type {
            FileSourceMessageType::S3Notification => MessageType::S3Notification,
            FileSourceMessageType::RawUri => MessageType::RawUri,
        };
        Ok(QueueCoordinator::new(
            source_runtime,
            Arc::new(queue),
            message_type,
        ))
    }

    pub async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let publish_lock = self.publish_lock.clone();
        ctx.send_message(doc_processor_mailbox, NewPublishLock(publish_lock))
            .await?;
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishToken(self.publish_token.clone()),
        )
        .await?;
        Ok(())
    }

    /// Polls messages from the queue and prepares them for processing
    async fn poll_messages(&mut self, ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        let raw_messages = self
            .queue_receiver
            .receive(1, self.visibility_settings.deadline_for_receive)
            .await?;

        let mut format_errors = Vec::new();
        let mut discardable_ack_ids = Vec::new();
        let mut preprocessed_messages = Vec::new();
        for message in raw_messages {
            match message.pre_process(self.message_type) {
                Ok(preprocessed_message) => preprocessed_messages.push(preprocessed_message),
                Err(PreProcessingError::UnexpectedFormat(err)) => format_errors.push(err),
                Err(PreProcessingError::Discardable { ack_id }) => discardable_ack_ids.push(ack_id),
            }
        }
        if !format_errors.is_empty() {
            self.observable_state.num_messages_failed_preprocessing += format_errors.len() as u64;
            rate_limited_error!(
                limit_per_min = 10,
                count = format_errors.len(),
                last_err = ?format_errors.last().unwrap(),
                "invalid messages not processed, use a dead letter queue to limit retries"
            );
        }
        if preprocessed_messages.is_empty() {
            self.queue.acknowledge(&discardable_ack_ids).await?;
            return Ok(());
        }

        // in rare situations, there might be duplicates within a batch
        let deduplicated_messages = preprocessed_messages
            .into_iter()
            .unique_by(|x| x.partition_id());

        let mut untracked_locally = Vec::new();
        let mut already_completed = Vec::new();
        for message in deduplicated_messages {
            let partition_id = message.partition_id();
            if self.local_state.is_completed(&partition_id) {
                already_completed.push(message);
            } else if !self.local_state.is_tracked(&partition_id) {
                untracked_locally.push(message);
            }
        }

        let checkpointed_messages =
            checkpoint_messages(&self.shared_state, &self.publish_token, untracked_locally).await?;

        let mut ready_messages = Vec::new();
        for (message, position) in checkpointed_messages {
            if position.is_eof() {
                self.local_state.mark_completed(message.partition_id());
                already_completed.push(message);
            } else {
                ready_messages.push(ReadyMessage {
                    visibility_handle: spawn_visibility_task(
                        ctx,
                        self.queue.clone(),
                        message.metadata.ack_id.clone(),
                        message.metadata.initial_deadline,
                        self.visibility_settings.clone(),
                    ),
                    content: message,
                    position,
                })
            }
        }

        self.local_state.set_ready_for_read(ready_messages);

        // Acknowledge messages that already have been processed
        let mut ack_ids = already_completed
            .iter()
            .map(|msg| msg.metadata.ack_id.clone())
            .collect::<Vec<_>>();
        ack_ids.append(&mut discardable_ack_ids);
        self.queue.acknowledge(&ack_ids).await?;

        Ok(())
    }

    pub async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        if let Some(in_progress_ref) = self.local_state.read_in_progress_mut() {
            // TODO: should we kill the publish lock if the message visibility extension failed?
            let batch_builder = in_progress_ref
                .batch_reader
                .read_batch(ctx.progress(), self.source_type)
                .await?;
            self.observable_state.num_lines_processed += batch_builder.docs.len() as u64;
            self.observable_state.num_bytes_processed += batch_builder.num_bytes;
            doc_processor_mailbox
                .send_message(batch_builder.build())
                .await?;
            if in_progress_ref.batch_reader.is_eof() {
                self.local_state
                    .drop_currently_read(self.visibility_settings.deadline_for_last_extension)
                    .await?;
                self.observable_state.num_messages_processed += 1;
            }
        } else if let Some(ready_message) = self.local_state.get_ready_for_read() {
            match ready_message.start_processing(&self.storage_resolver).await {
                Ok(new_in_progress) => {
                    self.local_state.set_currently_read(new_in_progress)?;
                }
                Err(err) => {
                    self.observable_state.num_messages_failed_opening += 1;
                    rate_limited_error!(
                        limit_per_min = 5,
                        err = ?err,
                        "failed to start message processing"
                    );
                }
            }
        } else {
            self.poll_messages(ctx).await?;
        }

        Ok(Duration::ZERO)
    }

    pub async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let committed_partition_ids = checkpoint
            .iter()
            .filter(|(_, pos)| pos.is_eof())
            .map(|(pid, _)| pid)
            .collect::<Vec<_>>();
        let mut completed = Vec::new();
        for partition_id in committed_partition_ids {
            let ack_id_opt = self.local_state.mark_completed(partition_id);
            if let Some(ack_id) = ack_id_opt {
                completed.push(ack_id);
            }
        }
        self.queue.acknowledge(&completed).await
    }

    pub fn observable_state(&self) -> &QueueCoordinatorObservableState {
        &self.observable_state
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use quickwit_actors::{ActorContext, Universe};
    use quickwit_common::uri::Uri;
    use quickwit_proto::types::{NodeId, PipelineUid, Position};
    use tokio::sync::watch;
    use ulid::Ulid;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::doc_file_reader::file_test_helpers::{generate_dummy_doc_file, DUMMY_DOC};
    use crate::source::queue_sources::memory_queue::MemoryQueueForTests;
    use crate::source::queue_sources::message::PreProcessedPayload;
    use crate::source::queue_sources::shared_state::shared_state_for_tests::init_state;
    use crate::source::{SourceActor, BATCH_NUM_BYTES_LIMIT};

    fn setup_coordinator(
        queue: Arc<MemoryQueueForTests>,
        shared_state: QueueSharedState,
    ) -> QueueCoordinator {
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from_str("test-node").unwrap(),
            index_uid: shared_state.index_uid.clone(),
            source_id: shared_state.source_id.clone(),
            pipeline_uid: PipelineUid::random(),
        };

        QueueCoordinator {
            local_state: QueueLocalState::default(),
            shared_state,
            pipeline_id,
            observable_state: QueueCoordinatorObservableState::default(),
            publish_lock: PublishLock::default(),
            // set a very high chunking timeout to make it possible to count the
            // number of iterations required to process messages
            queue_receiver: QueueReceiver::new(queue.clone(), Duration::from_secs(10)),
            queue,
            message_type: MessageType::RawUri,
            source_type: SourceType::Unspecified,
            storage_resolver: StorageResolver::for_test(),
            publish_token: Ulid::new().to_string(),
            visibility_settings: VisibilitySettings::from_commit_timeout(5),
        }
    }

    async fn process_messages(
        coordinator: &mut QueueCoordinator,
        queue: Arc<MemoryQueueForTests>,
        messages: &[(&Uri, &str)],
    ) -> Vec<RawDocBatch> {
        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        coordinator
            .initialize(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        coordinator
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        for (uri, ack_id) in messages {
            queue.send_message(uri.to_string(), ack_id);
        }

        // Need 3 iterations for each msg to emit the first batch (receive,
        // start, emit), assuming the `QueueReceiver` doesn't chunk the receive
        // future.
        for _ in 0..(messages.len() * 4) {
            coordinator
                .emit_batches(&doc_processor_mailbox, &ctx)
                .await
                .unwrap();
        }

        let batches = doc_processor_inbox
            .drain_for_test()
            .into_iter()
            .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
            .map(|box_raw_doc_batch| *box_raw_doc_batch)
            .collect::<Vec<_>>();
        universe.assert_quit().await;
        batches
    }

    #[tokio::test]
    async fn test_process_empty_queue() {
        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state("test-index", Default::default());
        let mut coordinator = setup_coordinator(queue.clone(), shared_state);
        let batches = process_messages(&mut coordinator, queue, &[]).await;
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_process_one_small_message() {
        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state("test-index", Default::default());
        let mut coordinator = setup_coordinator(queue.clone(), shared_state.clone());
        let (dummy_doc_file, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();
        let batches = process_messages(&mut coordinator, queue, &[(&test_uri, "ack-id")]).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].docs.len(), 10);
        assert!(coordinator.local_state.is_awaiting_commit(&partition_id));
    }

    #[tokio::test]
    async fn test_process_one_big_message() {
        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state("test-index", Default::default());
        let mut coordinator = setup_coordinator(queue.clone(), shared_state);
        let lines = BATCH_NUM_BYTES_LIMIT as usize / DUMMY_DOC.len() + 1;
        let (dummy_doc_file, _) = generate_dummy_doc_file(true, lines).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let batches = process_messages(&mut coordinator, queue, &[(&test_uri, "ack-id")]).await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(|b| b.docs.len()).sum::<usize>(), lines);
    }

    #[tokio::test]
    async fn test_process_two_messages_different_compression() {
        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state("test-index", Default::default());
        let mut coordinator = setup_coordinator(queue.clone(), shared_state);
        let (dummy_doc_file_1, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri_1 = Uri::from_str(dummy_doc_file_1.path().to_str().unwrap()).unwrap();
        let (dummy_doc_file_2, _) = generate_dummy_doc_file(true, 10).await;
        let test_uri_2 = Uri::from_str(dummy_doc_file_2.path().to_str().unwrap()).unwrap();
        let batches = process_messages(
            &mut coordinator,
            queue,
            &[(&test_uri_1, "ack-id-1"), (&test_uri_2, "ack-id-2")],
        )
        .await;
        // could be generated in 1 or 2 batches, it doesn't matter
        assert_eq!(batches.iter().map(|b| b.docs.len()).sum::<usize>(), 20);
    }

    #[tokio::test]
    async fn test_process_local_duplicate_message() {
        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state("test-index", Default::default());
        let mut coordinator = setup_coordinator(queue.clone(), shared_state);
        let (dummy_doc_file, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let batches = process_messages(
            &mut coordinator,
            queue,
            &[(&test_uri, "ack-id-1"), (&test_uri, "ack-id-2")],
        )
        .await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches.iter().map(|b| b.docs.len()).sum::<usize>(), 10);
    }

    #[tokio::test]
    async fn test_process_shared_complete_message() {
        let (dummy_doc_file, file_size) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();

        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state(
            "test-index",
            &[(
                partition_id.clone(),
                (
                    "existing_token".to_string(),
                    Position::eof(file_size),
                    false,
                ),
            )],
        );
        let mut coordinator = setup_coordinator(queue.clone(), shared_state.clone());

        assert!(!coordinator.local_state.is_tracked(&partition_id));
        let batches = process_messages(&mut coordinator, queue, &[(&test_uri, "ack-id-1")]).await;
        assert_eq!(batches.len(), 0);
        assert!(coordinator.local_state.is_completed(&partition_id));
    }

    #[tokio::test]
    async fn test_process_existing_messages() {
        let (dummy_doc_file_1, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri_1 = Uri::from_str(dummy_doc_file_1.path().to_str().unwrap()).unwrap();
        let partition_id_1 = PreProcessedPayload::ObjectUri(test_uri_1.clone()).partition_id();

        let (dummy_doc_file_2, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri_2 = Uri::from_str(dummy_doc_file_2.path().to_str().unwrap()).unwrap();
        let partition_id_2 = PreProcessedPayload::ObjectUri(test_uri_2.clone()).partition_id();

        let (dummy_doc_file_3, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri_3 = Uri::from_str(dummy_doc_file_3.path().to_str().unwrap()).unwrap();
        let partition_id_3 = PreProcessedPayload::ObjectUri(test_uri_3.clone()).partition_id();

        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state(
            "test-index",
            &[
                (
                    partition_id_1.clone(),
                    ("existing_token_1".to_string(), Position::Beginning, true),
                ),
                (
                    partition_id_2.clone(),
                    (
                        "existing_token_2".to_string(),
                        Position::offset((DUMMY_DOC.len() + 1) * 2),
                        true,
                    ),
                ),
                (
                    partition_id_3.clone(),
                    (
                        "existing_token_3".to_string(),
                        Position::offset((DUMMY_DOC.len() + 1) * 6),
                        false, // should not be processed because not stale yet
                    ),
                ),
            ],
        );
        let mut coordinator = setup_coordinator(queue.clone(), shared_state.clone());
        let batches = process_messages(
            &mut coordinator,
            queue,
            &[
                (&test_uri_1, "ack-id-1"),
                (&test_uri_2, "ack-id-2"),
                (&test_uri_3, "ack-id-3"),
            ],
        )
        .await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(|b| b.docs.len()).sum::<usize>(), 18);
        assert!(coordinator.local_state.is_awaiting_commit(&partition_id_1));
        assert!(coordinator.local_state.is_awaiting_commit(&partition_id_2));
    }

    #[tokio::test]
    async fn test_process_multiple_coordinator() {
        let queue = Arc::new(MemoryQueueForTests::new());
        let shared_state = init_state("test-index", Default::default());
        let mut coord_1 = setup_coordinator(queue.clone(), shared_state.clone());
        let mut coord_2 = setup_coordinator(queue.clone(), shared_state.clone());
        let (dummy_doc_file, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();

        let batches_1 = process_messages(&mut coord_1, queue.clone(), &[(&test_uri, "ack1")]).await;
        let batches_2 = process_messages(&mut coord_2, queue, &[(&test_uri, "ack2")]).await;

        assert_eq!(batches_1.len(), 1);
        assert_eq!(batches_1[0].docs.len(), 10);
        assert!(coord_1.local_state.is_awaiting_commit(&partition_id));
        // proc_2 learns from shared state that the message is likely still
        // being processed and skips it
        assert_eq!(batches_2.len(), 0);
        assert!(!coord_2.local_state.is_tracked(&partition_id));
    }
}
