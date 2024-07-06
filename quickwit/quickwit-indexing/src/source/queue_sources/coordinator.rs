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
use std::time::Duration;

use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::{FileSourceMessageType, FileSourceSqs};
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::SourceType;
use quickwit_storage::StorageResolver;
use serde::Serialize;
use tracing::debug;
use ulid::Ulid;

use super::local_state::QueueLocalState;
use super::message::{MessageType, ReadyMessage};
use super::shared_state::{checkpoint_messages, QueueSharedState, QueueSharedStateImpl};
use super::sqs_queue::SqsQueue;
use super::visibility::spawn_visibility_task;
use super::Queue;
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, NewPublishToken, PublishLock};
use crate::source::{SourceContext, SourceRuntime};

#[derive(Default, Serialize)]
pub struct QueueCoordinatorObservableState {
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of lines processed by the source.
    pub num_lines_processed: u64,
    /// Number of messages processed by the source.
    pub num_messages_processed: u64,
    // Number of invalid messages, i.e., that were empty or could not be parsed.
    // pub num_invalid_messages: u64,
    /// Number of time we looped without getting a single message
    pub num_consecutive_empty_batches: u64,
}

pub struct QueueCoordinator {
    storage_resolver: StorageResolver,
    pipeline_id: IndexingPipelineId,
    source_type: SourceType,
    queue: Arc<dyn Queue>,
    observable_state: QueueCoordinatorObservableState,
    message_type: MessageType,
    publish_lock: PublishLock,
    shared_state: Box<dyn QueueSharedState + Sync>,
    local_state: QueueLocalState,
    publish_token: String,
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
            shared_state: Box::new(QueueSharedStateImpl {
                metastore: source_runtime.metastore,
                source_id: source_runtime.pipeline_id.source_id.clone(),
                index_uid: source_runtime.pipeline_id.index_uid.clone(),
            }),
            local_state: QueueLocalState::default(),
            pipeline_id: source_runtime.pipeline_id,
            source_type: source_runtime.source_config.source_type(),
            storage_resolver: source_runtime.storage_resolver,
            queue,
            observable_state: QueueCoordinatorObservableState::default(),
            message_type,
            publish_lock: PublishLock::default(),
            publish_token: Ulid::new().to_string(),
        }
    }

    pub async fn try_from_config(
        config: FileSourceSqs,
        source_runtime: SourceRuntime,
    ) -> anyhow::Result<Self> {
        let queue = SqsQueue::try_new(config.queue_url, config.wait_time_seconds).await?;
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
        // TODO increase `max_messages` when previous messages were small
        // receive() typically uses long polling so it can be long to respond
        let raw_messages = ctx.protect_future(self.queue.receive(1)).await?;

        if raw_messages.is_empty() {
            self.observable_state.num_consecutive_empty_batches += 1;
            return Ok(());
        } else {
            self.observable_state.num_consecutive_empty_batches = 0;
        }

        let preprocessed_messages = raw_messages
            .into_iter()
            .map(|msg| msg.pre_process(self.message_type))
            .collect::<anyhow::Result<Vec<_>>>()?;

        // in rare situations, the same partition might be duplicted within batch
        let deduplicated_messages = preprocessed_messages
            .into_iter()
            .dedup_by(|x, y| x.partition_id() == y.partition_id());

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

        let checkpointed_messages = checkpoint_messages(
            self.shared_state.as_ref(),
            &self.publish_token,
            untracked_locally,
        )
        .await?;

        let mut ready_messages = Vec::new();
        for (message, position) in checkpointed_messages {
            if position.is_eof() {
                self.local_state.mark_completed(message.partition_id());
                already_completed.push(message);
            } else {
                ready_messages.push(ReadyMessage {
                    visibility_handle: spawn_visibility_task(
                        self.queue.clone(),
                        message.metadata.ack_id.clone(),
                        message.metadata.initial_deadline,
                    ),
                    content: message,
                    position,
                })
            }
        }

        self.local_state.set_ready_for_read(ready_messages);

        // Acknowledge messages that already have been processed
        let ack_ids = already_completed
            .iter()
            .map(|msg| msg.metadata.ack_id.clone())
            .collect::<Vec<_>>();
        self.queue.acknowledge(&ack_ids).await?;

        Ok(())
    }

    pub async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        if let Some(msg) = self.local_state.read_in_progress_mut() {
            debug!("process_batch");
            // TODO: should we kill the publish lock if the message visibility extension fails?
            let batch_builder = msg.reader.read_batch(ctx, self.source_type).await?;
            println!("batch_builder.num_bytes={}", batch_builder.num_bytes);
            if batch_builder.num_bytes > 0 {
                self.observable_state.num_lines_processed += batch_builder.docs.len() as u64;
                self.observable_state.num_bytes_processed += batch_builder.num_bytes;
                doc_processor_mailbox
                    .send_message(batch_builder.build())
                    .await?;
            }
            if msg.reader.is_eof() {
                self.observable_state.num_messages_processed += 1;
                self.local_state.replace_currently_read(None);
            }
        } else if let Some(ready_message) = self.local_state.get_ready_for_read() {
            debug!("start_processing");
            let new_in_progress = ready_message
                .start_processing(&self.storage_resolver)
                .await?;
            self.local_state.replace_currently_read(new_in_progress);
        } else {
            debug!("poll_messages");
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
            .map(|(pid, _)| pid);
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
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid};
    use tokio::sync::watch;
    use ulid::Ulid;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::doc_file_reader::file_test_helpers::{generate_dummy_doc_file, DUMMY_DOC};
    use crate::source::queue_sources::memory_queue::MemoryQueue;
    use crate::source::queue_sources::message::PreProcessedPayload;
    use crate::source::queue_sources::shared_state::shared_state_for_tests::{
        InMemoryQueueSharedState, SharedStateForPartition,
    };
    use crate::source::{SourceActor, BATCH_NUM_BYTES_LIMIT};

    fn setup_coordinator(
        queue: Arc<MemoryQueue>,
        shared_state: InMemoryQueueSharedState,
    ) -> QueueCoordinator {
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from_str("test-node").unwrap(),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::random(),
        };

        QueueCoordinator {
            local_state: QueueLocalState::default(),
            shared_state: Box::new(shared_state.clone()),
            pipeline_id,
            observable_state: QueueCoordinatorObservableState::default(),
            publish_lock: PublishLock::default(),
            queue,
            message_type: MessageType::RawUri,
            source_type: SourceType::Unspecified,
            storage_resolver: StorageResolver::for_test(),
            publish_token: Ulid::new().to_string(),
        }
    }

    async fn process_messages(
        tracker: &mut QueueCoordinator,
        queue: Arc<MemoryQueue>,
        messages: &[(&Uri, &str)],
    ) -> Vec<RawDocBatch> {
        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        tracker
            .initialize(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        tracker
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        for (uri, ack_id) in messages {
            queue.send_message(uri.to_string(), ack_id);
        }

        // Need 3 iterations for each msg to emit the first batch (receive, start, emit)
        for _ in 0..(messages.len() * 4) {
            tracker
                .emit_batches(&doc_processor_mailbox, &ctx)
                .await
                .unwrap();
        }

        doc_processor_inbox
            .drain_for_test()
            .into_iter()
            .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
            .map(|box_raw_doc_batch| *box_raw_doc_batch)
            .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn test_process_empty_queue() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut coordinator = setup_coordinator(queue.clone(), shared_state);
        let batches = process_messages(&mut coordinator, queue, &[]).await;
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_process_one_small_message() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut coordinator = setup_coordinator(queue.clone(), shared_state.clone());
        let (dummy_doc_file, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();
        let batches = process_messages(&mut coordinator, queue, &[(&test_uri, "ack-id")]).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].docs.len(), 10);
        assert_eq!(
            shared_state.get(&partition_id),
            SharedStateForPartition::InProgress(coordinator.publish_token)
        );
        assert!(coordinator.local_state.is_awating_commit(&partition_id));
    }

    #[tokio::test]
    async fn test_process_one_big_message() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
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
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
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
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
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
    async fn test_process_shared_duplicate_message() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut coordinator = setup_coordinator(queue.clone(), shared_state.clone());
        let (dummy_doc_file, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();

        shared_state.set(partition_id.clone(), SharedStateForPartition::Completed);

        assert!(!coordinator.local_state.is_tracked(&partition_id));
        let batches = process_messages(&mut coordinator, queue, &[(&test_uri, "ack-id-1")]).await;
        assert_eq!(batches.len(), 0);
        assert!(coordinator.local_state.is_completed(&partition_id));
    }

    #[tokio::test]
    async fn test_process_multiple_coordinator() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut proc_1 = setup_coordinator(queue.clone(), shared_state.clone());
        let mut proc_2 = setup_coordinator(queue.clone(), shared_state.clone());
        let (dummy_doc_file, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();

        let batches_1 = process_messages(&mut proc_1, queue.clone(), &[(&test_uri, "ack1")]).await;
        let batches_2 = process_messages(&mut proc_2, queue, &[(&test_uri, "ack-id-2")]).await;

        assert_eq!(batches_1.len(), 1);
        assert_eq!(batches_2.len(), 0);
        assert_eq!(batches_1[0].docs.len(), 10);
        assert!(proc_1.local_state.is_awating_commit(&partition_id));
        // proc_2 doesn't know for sure what is happening with the message
        // (proc_1 might have crashed), so it just forgets about it until it
        // will be received again
        assert!(!proc_2.local_state.is_tracked(&partition_id));
        assert!(matches!(
            shared_state.get(&partition_id),
            SharedStateForPartition::InProgress(_)
        ));
    }
}
