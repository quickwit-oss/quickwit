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

use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::QueueParams;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::SourceType;
use quickwit_storage::StorageResolver;
use serde_json::{json, Value as JsonValue};
use tracing::debug;
use ulid::Ulid;

use super::local_state::QueueLocalState;
use super::message::CheckpointedMessage;
use super::shared_state::{QueueSharedState, QueueSharedStateImpl};
use super::visibility::{acknowledge_and_abort, spawn_visibility_task, VisibilityTaskHandle};
use super::{acknowledge, Queue};
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, NewPublishToken, PublishLock};
use crate::source::{SourceContext, SourceRuntime};

#[derive(Default)]
pub struct QueueProcessorObservableState {
    /// Number of bytes processed by the source.
    num_bytes_processed: u64,
    /// Number of messages processed by the source.
    num_messages_processed: u64,
    // Number of invalid messages, i.e., that were empty or could not be parsed.
    num_invalid_messages: u64,
    /// Number of time we looped without getting a single message
    num_consecutive_empty_batches: u64,
}

pub struct QueueProcessor {
    storage_resolver: StorageResolver,
    pipeline_id: IndexingPipelineId,
    source_type: SourceType,
    queue: Arc<dyn Queue>,
    observable_state: QueueProcessorObservableState,
    queue_params: QueueParams,
    publish_lock: PublishLock,
    shared_state: Box<dyn QueueSharedState>,
    local_state: QueueLocalState,
    publish_token: String,
}

impl fmt::Debug for QueueProcessor {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("QueueProcessor")
            .field("index_id", &self.pipeline_id.index_uid.index_id)
            .field("queue", &self.queue)
            .finish()
    }
}

impl QueueProcessor {
    pub fn new(
        source_runtime: SourceRuntime,
        queue: Arc<dyn Queue>,
        queue_params: QueueParams,
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
            observable_state: QueueProcessorObservableState::default(),
            queue_params,
            publish_lock: PublishLock::default(),
            publish_token: Ulid::new().to_string(),
        }
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

    /// Starts background tasks that extend the visibility deadline of the
    /// messages until they are dropped.
    fn spawn_visibility_tasks(
        &mut self,
        messages: Vec<CheckpointedMessage>,
    ) -> Vec<(CheckpointedMessage, VisibilityTaskHandle)> {
        messages
            .into_iter()
            .map(|message| {
                let handle = spawn_visibility_task(
                    self.queue.clone(),
                    message.content.metadata.ack_id.clone(),
                    message.content.metadata.initial_deadline,
                    self.publish_lock.clone(),
                );
                (message, handle)
            })
            .collect()
    }

    /// Polls messages from the queue and prepares them for processing
    async fn poll_messages(&mut self, ctx: &SourceContext) -> anyhow::Result<()> {
        // receive() typically uses long polling so it can be long to respond
        let raw_messages = ctx.protect_future(self.queue.receive()).await?;

        if raw_messages.is_empty() {
            self.observable_state.num_consecutive_empty_batches += 1;
            return Ok(());
        } else {
            self.observable_state.num_consecutive_empty_batches = 0;
        }

        let preprocessed_messages = raw_messages
            .into_iter()
            .map(|msg| msg.pre_process(self.queue_params.message_type))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let categorized_using_local_state =
            self.local_state.filter_completed(preprocessed_messages);

        let categorized_using_shared_state = self
            .shared_state
            .checkpoint_messages(
                &self.publish_token,
                categorized_using_local_state.processable,
            )
            .await?;

        // Drop visibility tasks for messages that have been processed by another pipeline
        let mut completed_visibility_tasks = Vec::new();
        for preproc_msg in &categorized_using_shared_state.already_processed {
            let handle_opt = self.local_state.mark_completed(preproc_msg.partition_id());
            if let Some(handle) = handle_opt {
                completed_visibility_tasks.push(handle);
            }
        }
        acknowledge_and_abort(&*self.queue, completed_visibility_tasks).await?;

        // Acknowledge messages that have been processed by another pipeline
        let mut already_processed = categorized_using_local_state.already_processed;
        already_processed.extend(categorized_using_shared_state.already_processed);
        acknowledge(&*self.queue, already_processed).await?;

        let ready_messages =
            self.spawn_visibility_tasks(categorized_using_shared_state.processable);

        self.local_state.set_ready_messages(ready_messages);

        Ok(())
    }

    pub async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        if let Some(msg) = self.local_state.in_progress_mut() {
            debug!("process_batch");
            msg.process_batch(doc_processor_mailbox, ctx).await?;
            if msg.is_eof() {
                self.local_state.set_in_progress(None);
            }
        } else if let Some(ready_message) = self.local_state.get_ready_message() {
            debug!("start_processing");
            let new_in_progress = ready_message
                .start_processing(&self.storage_resolver, self.source_type)
                .await?;
            self.local_state.set_in_progress(new_in_progress);
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
        let completed_visibility_handles = checkpoint
            .iter()
            .filter(|(_, pos)| pos.is_eof())
            .filter_map(|(pid, _)| self.local_state.mark_completed(pid))
            .collect();
        acknowledge_and_abort(&*self.queue, completed_visibility_handles).await
    }

    pub fn observable_state(&self) -> JsonValue {
        json!({
            "index_id": self.pipeline_id.index_uid.index_id.clone(),
            "source_id": self.pipeline_id.source_id.clone(),
            "num_bytes_processed": self.observable_state.num_bytes_processed,
            "num_messages_processed": self.observable_state.num_messages_processed,
            "num_invalid_messages": self.observable_state.num_invalid_messages,
            "num_consecutive_empty_batches": self.observable_state.num_consecutive_empty_batches,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use quickwit_actors::{ActorContext, Universe};
    use quickwit_common::uri::Uri;
    use quickwit_config::QueueMessageType;
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid};
    use tokio::sync::watch;
    use ulid::Ulid;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::doc_file_reader::file_test_helpers::{generate_dummy_doc_file, DUMMY_DOC};
    use crate::source::queue_sources::local_state::test_helpers::LocalStateForPartition;
    use crate::source::queue_sources::memory_queue::MemoryQueue;
    use crate::source::queue_sources::message::PreProcessedPayload;
    use crate::source::queue_sources::shared_state::shared_state_for_tests::{
        InMemoryQueueSharedState, SharedStateForPartition,
    };
    use crate::source::{SourceActor, BATCH_NUM_BYTES_LIMIT};

    fn setup_processor(
        queue: Arc<MemoryQueue>,
        shared_state: InMemoryQueueSharedState,
    ) -> QueueProcessor {
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from_str("test-node").unwrap(),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::new(),
        };
        let queue_params = QueueParams {
            message_type: QueueMessageType::RawUri,
        };

        QueueProcessor {
            local_state: QueueLocalState::default(),
            shared_state: Box::new(shared_state.clone()),
            pipeline_id,
            observable_state: QueueProcessorObservableState::default(),
            publish_lock: PublishLock::default(),
            queue,
            queue_params,
            source_type: SourceType::Unspecified,
            storage_resolver: StorageResolver::for_test(),
            publish_token: Ulid::new().to_string(),
        }
    }

    async fn process_messages(
        processor: &mut QueueProcessor,
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

        processor
            .initialize(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        processor
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        for (uri, ack_id) in messages {
            queue.send_message(uri.to_string(), ack_id);
        }

        // Need 3 iterations for each msg to emit the first batch (receive, start, emit)
        for _ in 0..(messages.len() * 4) {
            processor
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
        let mut processor = setup_processor(queue.clone(), shared_state);
        let batches = process_messages(&mut processor, queue, &[]).await;
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_process_one_small_message() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut processor = setup_processor(queue.clone(), shared_state.clone());
        let dummy_doc_file = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();
        let batches = process_messages(&mut processor, queue, &[(&test_uri, "ack-id")]).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].docs.len(), 10);
        assert_eq!(
            shared_state.get(&partition_id),
            SharedStateForPartition::InProgress(processor.publish_token)
        );
        assert_eq!(
            processor.local_state.state(&partition_id),
            LocalStateForPartition::WaitForCommit
        );
    }

    #[tokio::test]
    async fn test_process_one_big_message() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut processor = setup_processor(queue.clone(), shared_state);
        let lines = BATCH_NUM_BYTES_LIMIT as usize / DUMMY_DOC.len() + 1;
        let dummy_doc_file = generate_dummy_doc_file(true, lines).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let batches = process_messages(&mut processor, queue, &[(&test_uri, "ack-id")]).await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(|b| b.docs.len()).sum::<usize>(), lines);
    }

    #[tokio::test]
    async fn test_process_two_messages_different_compression() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut processor = setup_processor(queue.clone(), shared_state);
        let dummy_doc_file_1 = generate_dummy_doc_file(false, 10).await;
        let test_uri_1 = Uri::from_str(dummy_doc_file_1.path().to_str().unwrap()).unwrap();
        let dummy_doc_file_2 = generate_dummy_doc_file(true, 10).await;
        let test_uri_2 = Uri::from_str(dummy_doc_file_2.path().to_str().unwrap()).unwrap();
        let batches = process_messages(
            &mut processor,
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
        let mut processor = setup_processor(queue.clone(), shared_state);
        let dummy_doc_file = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let batches = process_messages(
            &mut processor,
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
        let mut processor = setup_processor(queue.clone(), shared_state.clone());
        let dummy_doc_file = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();

        shared_state.set(partition_id.clone(), SharedStateForPartition::Completed);

        assert_eq!(
            processor.local_state.state(&partition_id),
            LocalStateForPartition::Unknown
        );
        let batches = process_messages(&mut processor, queue, &[(&test_uri, "ack-id-1")]).await;
        assert_eq!(batches.len(), 0);
        assert_eq!(
            processor.local_state.state(&partition_id),
            LocalStateForPartition::Completed
        );
    }

    #[tokio::test]
    async fn test_process_multiple_processor() {
        let queue = Arc::new(MemoryQueue::default());
        let shared_state = InMemoryQueueSharedState::default();
        let mut proc_1 = setup_processor(queue.clone(), shared_state.clone());
        let mut proc_2 = setup_processor(queue.clone(), shared_state.clone());
        let dummy_doc_file = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        let partition_id = PreProcessedPayload::ObjectUri(test_uri.clone()).partition_id();

        let batches_1 = process_messages(&mut proc_1, queue.clone(), &[(&test_uri, "ack1")]).await;
        let batches_2 = process_messages(&mut proc_2, queue, &[(&test_uri, "ack-id-2")]).await;

        assert_eq!(batches_1.len(), 1);
        assert_eq!(batches_2.len(), 0);
        assert_eq!(batches_1[0].docs.len(), 10);
        assert_eq!(
            proc_1.local_state.state(&partition_id),
            LocalStateForPartition::WaitForCommit
        );
        // proc_2 doesn't know for sure what is happening with the message
        // (proc_1 might have crashed), so it just forgets about it until it
        // will be received again
        assert_eq!(
            proc_2.local_state.state(&partition_id),
            LocalStateForPartition::Unknown
        );
        assert!(matches!(
            shared_state.get(&partition_id),
            SharedStateForPartition::InProgress(_)
        ));
    }
}
