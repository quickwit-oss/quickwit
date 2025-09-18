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

use std::collections::HashSet;
use std::iter::FromIterator;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::spawn_named_task;
use quickwit_config::RetentionPolicy;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_metastore::{SplitMetadata, StageSplitsRequestExt};
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient, StageSplitsRequest};
use quickwit_proto::search::{ReportSplit, ReportSplitsRequest};
use quickwit_proto::types::{IndexUid, PublishToken};
use quickwit_storage::SplitPayloadBuilder;
use serde::Serialize;
use tokio::sync::oneshot::Sender;
use tokio::sync::{Semaphore, SemaphorePermit, oneshot};
use tracing::{Instrument, Span, debug, info, instrument, warn};

use crate::actors::Publisher;
use crate::actors::sequencer::{Sequencer, SequencerCommand};
use crate::merge_policy::{MergePolicy, MergeTask};
use crate::metrics::INDEXER_METRICS;
use crate::models::{
    EmptySplit, PackagedSplit, PackagedSplitBatch, PublishLock, SplitsUpdate, create_split_metadata,
};
use crate::split_store::IndexingSplitStore;

/// The following two semaphores ensures that, we have at most `max_concurrent_split_uploads` split
/// uploads can happen at the same time, as configured in the `IndexerConfig`.
///
/// This "budget" is actually split into two semaphores: one for the indexing pipeline and the merge
/// pipeline. The idea is that the merge pipeline is by nature a bit irregular, and we don't want it
/// to stall the indexing pipeline, decreasing its throughput.
static CONCURRENT_UPLOAD_PERMITS_INDEX: OnceCell<Semaphore> = OnceCell::new();
static CONCURRENT_UPLOAD_PERMITS_MERGE: OnceCell<Semaphore> = OnceCell::new();

#[derive(Clone, Copy, Debug)]
pub enum UploaderType {
    IndexUploader,
    MergeUploader,
    DeleteUploader,
}

/// [`SplitsUpdateMailbox`] wraps either a [`Mailbox<Sequencer>`] or [`Mailbox<Publisher>`].
///
/// It makes it possible to send a [`SplitsUpdate`] either to the [`Sequencer`] or directly
/// to [`Publisher`]. It is used in combination with `SplitsUpdateSender` that will do the send.
///
/// This is useful as we have different requirements between the indexing pipeline and
/// the merge/delete task pipelines.
/// 1. In the indexing pipeline, we want to publish splits in the same order as they are produced by
///    the indexer/packager to ensure we are publishing splits without "holes" in checkpoints. We
///    thus send [`SplitsUpdate`] to the [`Sequencer`] to keep the right ordering.
/// 2. In the merge pipeline and the delete task pipeline, we are merging splits and in in this
///    case, publishing order does not matter. In this case, we can just send [`SplitsUpdate`]
///    directly to the [`Publisher`].
#[derive(Clone, Debug)]
pub enum SplitsUpdateMailbox {
    Sequencer(Mailbox<Sequencer<Publisher>>),
    Publisher(Mailbox<Publisher>),
}

impl From<Mailbox<Publisher>> for SplitsUpdateMailbox {
    fn from(publisher_mailbox: Mailbox<Publisher>) -> Self {
        SplitsUpdateMailbox::Publisher(publisher_mailbox)
    }
}

impl From<Mailbox<Sequencer<Publisher>>> for SplitsUpdateMailbox {
    fn from(publisher_sequencer_mailbox: Mailbox<Sequencer<Publisher>>) -> Self {
        SplitsUpdateMailbox::Sequencer(publisher_sequencer_mailbox)
    }
}

impl SplitsUpdateMailbox {
    async fn get_split_update_sender(
        &self,
        ctx: &ActorContext<Uploader>,
    ) -> anyhow::Result<SplitsUpdateSender> {
        match self {
            SplitsUpdateMailbox::Sequencer(sequencer_mailbox) => {
                // We send the future to the sequencer right away.
                // The sequencer will then resolve the future in their arrival order and ensure that
                // the publisher publishes splits in order.
                let (split_uploaded_tx, split_uploaded_rx) =
                    oneshot::channel::<SequencerCommand<SplitsUpdate>>();
                ctx.send_message(sequencer_mailbox, split_uploaded_rx)
                    .await?;
                Ok(SplitsUpdateSender::Sequencer(split_uploaded_tx))
            }
            SplitsUpdateMailbox::Publisher(publisher_mailbox) => {
                // We just need the publisher mailbox to send the split in this case.
                Ok(SplitsUpdateSender::Publisher(publisher_mailbox.clone()))
            }
        }
    }
}

enum SplitsUpdateSender {
    Sequencer(Sender<SequencerCommand<SplitsUpdate>>),
    Publisher(Mailbox<Publisher>),
}

impl SplitsUpdateSender {
    fn discard(self) -> anyhow::Result<()> {
        if let SplitsUpdateSender::Sequencer(split_uploader_tx) = self
            && split_uploader_tx.send(SequencerCommand::Discard).is_err()
        {
            bail!("failed to send cancel command to sequencer. the sequencer is probably dead");
        }
        Ok(())
    }

    async fn send(
        self,
        split_update: SplitsUpdate,
        ctx: &ActorContext<Uploader>,
    ) -> anyhow::Result<()> {
        match self {
            SplitsUpdateSender::Sequencer(split_uploaded_tx) => {
                if let Err(publisher_message) =
                    split_uploaded_tx.send(SequencerCommand::Proceed(split_update))
                {
                    bail!(
                        "failed to send upload split `{:?}`. the publisher is probably dead",
                        &publisher_message
                    );
                }
            }
            SplitsUpdateSender::Publisher(publisher_mailbox) => {
                ctx.send_message(&publisher_mailbox, split_update).await?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Uploader {
    uploader_type: UploaderType,
    metastore: MetastoreServiceClient,
    merge_policy: Arc<dyn MergePolicy>,
    retention_policy: Option<RetentionPolicy>,
    split_store: IndexingSplitStore,
    split_update_mailbox: SplitsUpdateMailbox,
    max_concurrent_split_uploads: usize,
    counters: UploaderCounters,
    event_broker: EventBroker,
}

impl Uploader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        uploader_type: UploaderType,
        metastore: MetastoreServiceClient,
        merge_policy: Arc<dyn MergePolicy>,
        retention_policy: Option<RetentionPolicy>,
        split_store: IndexingSplitStore,
        split_update_mailbox: SplitsUpdateMailbox,
        max_concurrent_split_uploads: usize,
        event_broker: EventBroker,
    ) -> Uploader {
        Uploader {
            uploader_type,
            metastore,
            merge_policy,
            retention_policy,
            split_store,
            split_update_mailbox,
            max_concurrent_split_uploads,
            counters: Default::default(),
            event_broker,
        }
    }
    async fn acquire_semaphore(
        &self,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<SemaphorePermit<'static>> {
        let _guard = ctx.protect_zone();
        let (concurrent_upload_permits_once_cell, concurrent_upload_permits_gauge) =
            match self.uploader_type {
                UploaderType::IndexUploader => (
                    &CONCURRENT_UPLOAD_PERMITS_INDEX,
                    INDEXER_METRICS
                        .available_concurrent_upload_permits
                        .with_label_values(["indexer"]),
                ),
                UploaderType::MergeUploader => (
                    &CONCURRENT_UPLOAD_PERMITS_MERGE,
                    INDEXER_METRICS
                        .available_concurrent_upload_permits
                        .with_label_values(["merger"]),
                ),
                UploaderType::DeleteUploader => (
                    &CONCURRENT_UPLOAD_PERMITS_MERGE,
                    INDEXER_METRICS
                        .available_concurrent_upload_permits
                        .with_label_values(["merger"]),
                ),
            };
        let concurrent_upload_permits = concurrent_upload_permits_once_cell
            .get_or_init(|| Semaphore::const_new(self.max_concurrent_split_uploads));
        concurrent_upload_permits_gauge.set(concurrent_upload_permits.available_permits() as i64);
        concurrent_upload_permits
            .acquire()
            .await
            .context("the uploader semaphore is closed. (this should never happen)")
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UploaderCounters {
    pub num_staged_splits: Arc<AtomicU64>,
    pub num_uploaded_splits: Arc<AtomicU64>,
}

#[async_trait]
impl Actor for Uploader {
    type ObservableState = UploaderCounters;

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        // We do not need a large capacity here...
        // The uploader just spawns tasks that are uploading,
        // so that in a sense, the CONCURRENT_UPLOAD_PERMITS semaphore also acts as
        // a queue capacity.
        //
        // Having a large queue is costly too, because each message is a handle over
        // a split directory. We DO need aggressive backpressure here.
        QueueCapacity::Bounded(0)
    }

    fn name(&self) -> String {
        format!("{:?}", self.uploader_type)
    }
}

#[async_trait]
impl Handler<PackagedSplitBatch> for Uploader {
    type Reply = ();

    #[instrument(name = "uploader",
        parent=batch.batch_parent_span.id(),
        skip_all)]
    async fn handle(
        &mut self,
        batch: PackagedSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("uploader:before");
        let split_update_sender = self
            .split_update_mailbox
            .get_split_update_sender(ctx)
            .await?;

        // The permit will be added back manually to the semaphore the task after it is finished.
        // This is not a valid usage of protected zone here.
        //
        // Protected zone are supposed to be used when the cause for blocking is
        // outside of the responsibility of the current actor.
        // For instance, when sending a message on a downstream actor with a saturated
        // mailbox.
        // This is meant to be fixed with ParallelActors.
        let permit_guard = self.acquire_semaphore(ctx).await?;
        let kill_switch = ctx.kill_switch().clone();
        let split_ids = batch.split_ids();
        if kill_switch.is_dead() {
            warn!(split_ids=?split_ids,"kill switch was activated, cancelling upload");
            return Err(ActorExitStatus::Killed);
        }
        let metastore = self.metastore.clone();
        let split_store = self.split_store.clone();
        let counters = self.counters.clone();
        let index_uid = batch.index_uid();
        let ctx_clone = ctx.clone();
        let merge_policy = self.merge_policy.clone();
        let retention_policy = self.retention_policy.clone();
        debug!(split_ids=?split_ids, "start-stage-and-store-splits");
        let event_broker = self.event_broker.clone();
        spawn_named_task(
            async move {
                fail_point!("uploader:intask:before");

                let mut split_metadata_list = Vec::with_capacity(batch.splits.len());
                let mut report_splits: Vec<ReportSplit> = Vec::with_capacity(batch.splits.len());

                for packaged_split in batch.splits.iter() {
                    if batch.publish_lock.is_dead() {
                        // TODO: Remove the junk right away?
                        info!("splits' publish lock is dead");
                        if let Err(e) = split_update_sender.discard() {
                            warn!(cause=?e, "could not discard split");
                        }
                        return;
                    }

                    let split_streamer = match SplitPayloadBuilder::get_split_payload(
                        &packaged_split.split_files,
                        &packaged_split.serialized_split_fields,
                        &packaged_split.hotcache_bytes,
                    ) {
                        Ok(split_streamer) => split_streamer,
                        Err(e) => {
                            warn!(cause=?e, split_id=packaged_split.split_id(), "could not create split streamer");
                            return;
                        }
                    };
                    let split_metadata = create_split_metadata(
                        &merge_policy,
                        retention_policy.as_ref(),
                        &packaged_split.split_attrs,
                        packaged_split.tags.clone(),
                        split_streamer.footer_range.start..split_streamer.footer_range.end,
                    );

                    report_splits.push(ReportSplit {
                        storage_uri: split_store.remote_uri().to_string(),
                        split_id: packaged_split.split_id().to_string(),
                    });

                    split_metadata_list.push(split_metadata);

                }

                let stage_splits_request = match StageSplitsRequest::try_from_splits_metadata(index_uid.clone(), split_metadata_list.clone()) {
                    Ok(stage_splits_request) => stage_splits_request,
                    Err(e) => {
                        warn!(cause=?e, "could not create stage splits request");
                        return;
                    }
                };
                if let Err(e) = metastore
                    .clone()
                    .stage_splits(stage_splits_request)
                    .await
                {
                    warn!(cause=?e, "failed to stage splits");
                    return;
                };

                counters.num_staged_splits.fetch_add(split_metadata_list.len() as u64, Ordering::SeqCst);

                let mut packaged_splits_and_metadata = Vec::with_capacity(batch.splits.len());

                event_broker.publish(ReportSplitsRequest { report_splits });

                for (packaged_split, metadata) in batch.splits.into_iter().zip(split_metadata_list) {
                    let upload_result = upload_split(
                        &packaged_split,
                        &metadata,
                        &split_store,
                        counters.clone(),
                    )
                    .await;

                    if let Err(cause) = upload_result {
                        warn!(cause=?cause, split_id=packaged_split.split_id(), "Failed to upload split. Killing!");
                        kill_switch.kill();
                        return;
                    }

                    packaged_splits_and_metadata.push((packaged_split, metadata));
                }

                let splits_update = make_publish_operation(
                    index_uid,
                    packaged_splits_and_metadata,
                    batch.checkpoint_delta_opt,
                    batch.publish_lock,
                    batch.publish_token_opt,
                    batch.merge_task_opt,
                    batch.batch_parent_span,
                );

                let target = match &split_update_sender {
                    SplitsUpdateSender::Sequencer(_) => "sequencer",
                    SplitsUpdateSender::Publisher(_) => "publisher",
                };
                if let Err(e) = split_update_sender.send(splits_update, &ctx_clone).await {
                    warn!(cause=?e, target, "failed to send uploaded split");
                    return;
                }
                // We explicitly drop it in order to force move the permit guard into the async
                // task.
                mem::drop(permit_guard);
            }
            .instrument(Span::current()),
            "upload_single_task"
        );
        fail_point!("uploader:intask:after");
        Ok(())
    }
}

#[async_trait]
impl Handler<EmptySplit> for Uploader {
    type Reply = ();

    #[instrument(
        name="upload_empty_split",
        parent=empty_split.batch_parent_span.id(),
        skip_all,
    )]
    async fn handle(
        &mut self,
        empty_split: EmptySplit,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let split_update_sender = self
            .split_update_mailbox
            .get_split_update_sender(ctx)
            .await?;
        let splits_update = SplitsUpdate {
            index_uid: empty_split.index_uid,
            new_splits: Vec::new(),
            replaced_split_ids: Vec::new(),
            checkpoint_delta_opt: Some(empty_split.checkpoint_delta),
            publish_lock: empty_split.publish_lock,
            publish_token_opt: empty_split.publish_token_opt,
            merge_task: None,
            parent_span: empty_split.batch_parent_span,
        };

        split_update_sender.send(splits_update, ctx).await?;
        Ok(())
    }
}

fn make_publish_operation(
    index_uid: IndexUid,
    packaged_splits_and_metadatas: Vec<(PackagedSplit, SplitMetadata)>,
    checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    publish_lock: PublishLock,
    publish_token_opt: Option<PublishToken>,
    merge_task: Option<MergeTask>,
    parent_span: Span,
) -> SplitsUpdate {
    assert!(!packaged_splits_and_metadatas.is_empty());
    let replaced_split_ids = packaged_splits_and_metadatas
        .iter()
        .flat_map(|(split, _)| split.split_attrs.replaced_split_ids.clone())
        .collect::<HashSet<_>>();
    SplitsUpdate {
        index_uid,
        new_splits: packaged_splits_and_metadatas
            .into_iter()
            .map(|split_and_meta| split_and_meta.1)
            .collect_vec(),
        replaced_split_ids: Vec::from_iter(replaced_split_ids),
        checkpoint_delta_opt,
        publish_lock,
        publish_token_opt,
        merge_task,
        parent_span,
    }
}

#[instrument(
    level = "info"
    name = "upload",
    fields(split = %packaged_split.split_attrs.split_id),
    skip_all
)]
async fn upload_split(
    packaged_split: &PackagedSplit,
    split_metadata: &SplitMetadata,
    split_store: &IndexingSplitStore,
    counters: UploaderCounters,
) -> anyhow::Result<()> {
    let split_streamer = SplitPayloadBuilder::get_split_payload(
        &packaged_split.split_files,
        &packaged_split.serialized_split_fields,
        &packaged_split.hotcache_bytes,
    )?;

    split_store
        .store_split(
            split_metadata,
            packaged_split.split_scratch_directory.path(),
            Box::new(split_streamer),
        )
        .await?;
    counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use quickwit_actors::{ObservationType, Universe};
    use quickwit_common::pubsub::EventSubscriber;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_proto::types::{DocMappingUid, NodeId};
    use quickwit_storage::RamStorage;
    use tantivy::DateTime;
    use tokio::sync::oneshot;

    use super::*;
    use crate::merge_policy::{NopMergePolicy, default_merge_policy};
    use crate::models::{SplitAttrs, SplitsUpdate};

    #[tokio::test]
    async fn test_uploader_with_sequencer() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();

        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();

        let event_broker = EventBroker::default();
        let universe = Universe::new();
        let (sequencer_mailbox, sequencer_inbox) =
            universe.create_test_mailbox::<Sequencer<Publisher>>();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| -> bool {
                let splits_metadata = stage_splits_request.deserialize_splits_metadata().unwrap();
                let split_metadata = &splits_metadata[0];
                let index_uid: IndexUid = stage_splits_request.index_uid().clone();
                index_uid.index_id == "test-index"
                    && split_metadata.split_id() == "test-split"
                    && split_metadata.time_range == Some(1628203589..=1628203640)
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store =
            IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
            4,
            event_broker,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory = TempDirectory::for_test();
        let checkpoint_delta_opt: Option<IndexCheckpointDelta> = Some(IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(3..15),
        });
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![PackagedSplit {
                    split_attrs: SplitAttrs {
                        node_id,
                        index_uid,
                        source_id,
                        doc_mapping_uid: DocMappingUid::default(),
                        partition_id: 3u64,
                        time_range: Some(
                            DateTime::from_timestamp_secs(1_628_203_589)
                                ..=DateTime::from_timestamp_secs(1_628_203_640),
                        ),
                        uncompressed_docs_size_in_bytes: 1_000,
                        num_docs: 10,
                        replaced_split_ids: Vec::new(),
                        split_id: "test-split".to_string(),
                        delete_opstamp: 10,
                        num_merge_ops: 0,
                    },
                    serialized_split_fields: Vec::new(),
                    split_scratch_directory,
                    tags: Default::default(),
                    hotcache_bytes: Vec::new(),
                    split_files: Vec::new(),
                }],
                checkpoint_delta_opt,
                PublishLock::default(),
                None,
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let mut publish_futures: Vec<oneshot::Receiver<SequencerCommand<SplitsUpdate>>> =
            sequencer_inbox.drain_for_test_typed();
        assert_eq!(publish_futures.len(), 1);

        let publisher_message = match publish_futures.pop().unwrap().await? {
            SequencerCommand::Discard => panic!(
                "expected `SequencerCommand::Proceed(SplitUpdate)`, got \
                 `SequencerCommand::Discard`"
            ),
            SequencerCommand::Proceed(publisher_message) => publisher_message,
        };
        let SplitsUpdate {
            index_uid,
            new_splits,
            checkpoint_delta_opt,
            replaced_split_ids,
            ..
        } = publisher_message;

        assert_eq!(index_uid.index_id, "test-index");
        assert_eq!(new_splits.len(), 1);
        assert_eq!(new_splits[0].split_id(), "test-split");
        let checkpoint_delta = checkpoint_delta_opt.unwrap();
        assert_eq!(checkpoint_delta.source_id, "test-source");
        assert_eq!(
            checkpoint_delta.source_delta,
            SourceCheckpointDelta::from_range(3..15)
        );
        assert!(replaced_split_ids.is_empty());
        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(&files, &[PathBuf::from("test-split.split")]);
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_uploader_with_sequencer_emits_replace() -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();

        let universe = Universe::new();
        let (sequencer_mailbox, sequencer_inbox) =
            universe.create_test_mailbox::<Sequencer<Publisher>>();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| -> bool {
                let splits_metadata = stage_splits_request.deserialize_splits_metadata().unwrap();
                let is_metadata_valid = splits_metadata.iter().all(|metadata| {
                    ["test-split-1", "test-split-2"].contains(&metadata.split_id())
                        && metadata.time_range == Some(1628203589..=1628203640)
                });
                let index_uid: IndexUid = stage_splits_request.index_uid().clone();
                index_uid.index_id == "test-index" && is_metadata_valid
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store =
            IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
            4,
            EventBroker::default(),
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory_1 = TempDirectory::for_test();
        let split_scratch_directory_2 = TempDirectory::for_test();
        let packaged_split_1 = PackagedSplit {
            split_attrs: SplitAttrs {
                node_id: node_id.clone(),
                index_uid: index_uid.clone(),
                source_id: source_id.clone(),
                doc_mapping_uid: DocMappingUid::default(),
                split_id: "test-split-1".to_string(),
                partition_id: 3u64,
                num_docs: 10,
                uncompressed_docs_size_in_bytes: 1_000,
                time_range: Some(
                    DateTime::from_timestamp_secs(1_628_203_589)
                        ..=DateTime::from_timestamp_secs(1_628_203_640),
                ),
                replaced_split_ids: vec![
                    "replaced-split-1".to_string(),
                    "replaced-split-2".to_string(),
                ],
                delete_opstamp: 0,
                num_merge_ops: 0,
            },
            serialized_split_fields: Vec::new(),
            split_scratch_directory: split_scratch_directory_1,
            tags: Default::default(),
            split_files: Vec::new(),
            hotcache_bytes: Vec::new(),
        };
        let package_split_2 = PackagedSplit {
            split_attrs: SplitAttrs {
                node_id,
                index_uid,
                source_id,
                doc_mapping_uid: DocMappingUid::default(),
                split_id: "test-split-2".to_string(),
                partition_id: 3u64,
                num_docs: 10,
                uncompressed_docs_size_in_bytes: 1_000,
                time_range: Some(
                    DateTime::from_timestamp_secs(1_628_203_589)
                        ..=DateTime::from_timestamp_secs(1_628_203_640),
                ),
                replaced_split_ids: vec![
                    "replaced-split-1".to_string(),
                    "replaced-split-2".to_string(),
                ],
                delete_opstamp: 0,
                num_merge_ops: 0,
            },
            serialized_split_fields: Vec::new(),
            split_scratch_directory: split_scratch_directory_2,
            tags: Default::default(),
            split_files: Vec::new(),
            hotcache_bytes: Vec::new(),
        };
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![packaged_split_1, package_split_2],
                None,
                PublishLock::default(),
                None,
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let mut publish_futures: Vec<oneshot::Receiver<SequencerCommand<SplitsUpdate>>> =
            sequencer_inbox.drain_for_test_typed();
        assert_eq!(publish_futures.len(), 1);

        let publisher_message = match publish_futures.pop().unwrap().await? {
            SequencerCommand::Discard => panic!(
                "Expected `SequencerCommand::Proceed(SplitsUpdate)`, got \
                 `SequencerCommand::Discard`."
            ),
            SequencerCommand::Proceed(publisher_message) => publisher_message,
        };
        let SplitsUpdate {
            index_uid,
            new_splits,
            mut replaced_split_ids,
            checkpoint_delta_opt,
            ..
        } = publisher_message;
        assert_eq!(index_uid.index_id, "test-index");
        // Sort first to avoid test failing.
        replaced_split_ids.sort();
        assert_eq!(new_splits.len(), 2);
        assert_eq!(new_splits[0].split_id(), "test-split-1");
        assert_eq!(new_splits[1].split_id(), "test-split-2");
        assert_eq!(
            &replaced_split_ids,
            &[
                "replaced-split-1".to_string(),
                "replaced-split-2".to_string()
            ]
        );
        assert!(checkpoint_delta_opt.is_none());

        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(
            &files,
            &[
                PathBuf::from("test-split-1.split"),
                PathBuf::from("test-split-2.split")
            ]
        );
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_uploader_without_sequencer() -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::for_test("test-index", 0);
        let index_uid_clone = index_uid.clone();
        let source_id = "test-source".to_string();

        let universe = Universe::new();
        let (publisher_mailbox, publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| -> bool {
                stage_splits_request.index_uid() == &index_uid_clone
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store =
            IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
            4,
            EventBroker::default(),
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory = TempDirectory::for_test();
        let checkpoint_delta_opt: Option<IndexCheckpointDelta> = Some(IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(3..15),
        });
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![PackagedSplit {
                    split_attrs: SplitAttrs {
                        node_id,
                        index_uid,
                        source_id,
                        doc_mapping_uid: DocMappingUid::default(),
                        split_id: "test-split".to_string(),
                        partition_id: 3u64,
                        time_range: None,
                        uncompressed_docs_size_in_bytes: 1_000,
                        num_docs: 10,
                        replaced_split_ids: Vec::new(),
                        delete_opstamp: 10,
                        num_merge_ops: 0,
                    },
                    serialized_split_fields: Vec::new(),
                    split_scratch_directory,
                    tags: Default::default(),
                    hotcache_bytes: Vec::new(),
                    split_files: Vec::new(),
                }],
                checkpoint_delta_opt,
                PublishLock::default(),
                None,
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            ..
        } = publisher_inbox.recv_typed_message().await.unwrap();

        assert_eq!(index_uid.index_id, "test-index");
        assert_eq!(new_splits.len(), 1);
        assert!(replaced_split_ids.is_empty());
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_uploader_with_empty_splits() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (sequencer_mailbox, sequencer_inbox) =
            universe.create_test_mailbox::<Sequencer<Publisher>>();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_stage_splits().never();
        let ram_storage = RamStorage::default();
        let split_store =
            IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            default_merge_policy(),
            None,
            split_store,
            SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
            4,
            EventBroker::default(),
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let checkpoint_delta = IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(3..15),
        };
        uploader_mailbox
            .send_message(EmptySplit {
                index_uid: IndexUid::new_with_random_ulid("test-index"),
                checkpoint_delta,
                publish_lock: PublishLock::default(),
                publish_token_opt: None,
                batch_parent_span: Span::none(),
            })
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let mut publish_futures: Vec<oneshot::Receiver<SequencerCommand<SplitsUpdate>>> =
            sequencer_inbox.drain_for_test_typed();
        assert_eq!(publish_futures.len(), 1);

        let publisher_message = match publish_futures.pop().unwrap().await? {
            SequencerCommand::Discard => panic!(
                "Expected `SequencerCommand::Proceed(SplitUpdate)`, got \
                 `SequencerCommand::Discard`."
            ),
            SequencerCommand::Proceed(publisher_message) => publisher_message,
        };
        let SplitsUpdate {
            index_uid,
            new_splits,
            checkpoint_delta_opt,
            replaced_split_ids,
            ..
        } = publisher_message;

        assert_eq!(index_uid.index_id, "test-index");
        assert_eq!(new_splits.len(), 0);
        let checkpoint_delta = checkpoint_delta_opt.unwrap();
        assert_eq!(checkpoint_delta.source_id, "test-source");
        assert_eq!(
            checkpoint_delta.source_delta,
            SourceCheckpointDelta::from_range(3..15)
        );
        assert!(replaced_split_ids.is_empty());
        let files = ram_storage.list_files().await;
        assert!(files.is_empty());
        universe.assert_quit().await;
        Ok(())
    }

    struct ReportSplitListener {
        report_splits_tx: flume::Sender<ReportSplitsRequest>,
    }

    impl std::fmt::Debug for ReportSplitListener {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.debug_struct("ReportSplitListener").finish()
        }
    }

    #[async_trait]
    impl EventSubscriber<ReportSplitsRequest> for ReportSplitListener {
        async fn handle_event(&mut self, event: ReportSplitsRequest) {
            self.report_splits_tx.send(event).unwrap();
        }
    }

    #[tokio::test]
    async fn test_uploader_notifies_event_broker() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        const SPLIT_ULID_STR: &str = "01HAV29D4XY3D462FS3D8K5Q2H";
        let event_broker = EventBroker::default();
        let (report_splits_tx, report_splits_rx) = flume::unbounded();
        let report_splits_listener = ReportSplitListener { report_splits_tx };

        // we need to keep the handle alive.
        let _subscribe_handle = event_broker.subscribe(report_splits_listener);

        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();

        let universe = Universe::new();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store =
            IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox();
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
            4,
            event_broker,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory = TempDirectory::for_test();
        let checkpoint_delta_opt: Option<IndexCheckpointDelta> = Some(IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(3..15),
        });
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![PackagedSplit {
                    split_attrs: SplitAttrs {
                        node_id,
                        index_uid,
                        source_id,
                        doc_mapping_uid: DocMappingUid::default(),
                        partition_id: 3u64,
                        time_range: Some(
                            DateTime::from_timestamp_secs(1_628_203_589)
                                ..=DateTime::from_timestamp_secs(1_628_203_640),
                        ),
                        uncompressed_docs_size_in_bytes: 1_000,
                        num_docs: 10,
                        replaced_split_ids: Vec::new(),
                        split_id: SPLIT_ULID_STR.to_string(),
                        delete_opstamp: 10,
                        num_merge_ops: 0,
                    },
                    serialized_split_fields: Vec::new(),
                    split_scratch_directory,
                    tags: Default::default(),
                    hotcache_bytes: Vec::new(),
                    split_files: Vec::new(),
                }],
                checkpoint_delta_opt,
                PublishLock::default(),
                None,
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        mem::drop(uploader_mailbox);
        let report_splits: ReportSplitsRequest = report_splits_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert_eq!(report_splits.report_splits.len(), 1);
        let split = &report_splits.report_splits[0];
        assert_eq!(split.storage_uri, "ram:///");
        assert_eq!(split.split_id, SPLIT_ULID_STR);
        universe.assert_quit().await;
        Ok(())
    }
}
