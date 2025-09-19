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

//! # Sources
//!
//! Quickwit gets its data from so-called `Sources`.
//!
//! The role of a source is to push message to an indexer mailbox.
//! Implementers need to focus on the implementation of the [`Source`] trait
//! and in particular its emit_batches method.
//! In addition, they need to implement a source factory.
//!
//! The source trait will executed in an actor.
//!
//! # Checkpoints and exactly-once semantics
//!
//! Quickwit is designed to offer exactly-once semantics whenever possible using the following
//! strategy, using checkpoints.
//!
//! Messages are split into partitions, and within a partition messages are totally ordered: they
//! are marked by a unique position within this partition.
//!
//! Sources are required to emit messages in a way that respects this partial order.
//! If two message belong 2 different partitions, they can be emitted in any order.
//! If two message belong to the same partition, they  are required to be emitted in the order of
//! their position.
//!
//! The set of documents processed by a source can then be expressed entirely as Checkpoint, that is
//! simply a mapping `(PartitionId -> Position)`.
//!
//! This checkpoint is used in Quickwit to implement exactly-once semantics.
//! When a new split is published, it is atomically published with an update of the last indexed
//! checkpoint.
//!
//! If the indexing pipeline is restarted, the source will simply be recreated with that checkpoint.
//!
//! # Example sources
//!
//! Right now two sources are implemented in quickwit.
//! - the file source: there partition here is a filepath, and the position is a byte-offset within
//!   that file.
//! - the kafka source: the partition id is a kafka topic partition id, and the position is a kafka
//!   offset.
mod doc_file_reader;
mod file_source;
#[cfg(feature = "gcp-pubsub")]
mod gcp_pubsub_source;
mod ingest;
mod ingest_api_source;
#[cfg(feature = "kafka")]
mod kafka_source;
#[cfg(feature = "kinesis")]
mod kinesis;
#[cfg(feature = "pulsar")]
mod pulsar_source;
#[cfg(feature = "queue-sources")]
mod queue_sources;
mod source_factory;
mod stdin_source;
mod vec_source;
mod void_source;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use bytesize::ByteSize;
pub use file_source::{FileSource, FileSourceFactory};
#[cfg(feature = "gcp-pubsub")]
pub use gcp_pubsub_source::{GcpPubSubSource, GcpPubSubSourceFactory};
#[cfg(feature = "kafka")]
pub use kafka_source::{KafkaSource, KafkaSourceFactory};
#[cfg(feature = "kinesis")]
pub use kinesis::kinesis_source::{KinesisSource, KinesisSourceFactory};
use once_cell::sync::{Lazy, OnceCell};
#[cfg(feature = "pulsar")]
pub use pulsar_source::{PulsarSource, PulsarSourceFactory};
#[cfg(feature = "sqs")]
pub use queue_sources::sqs_queue;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::{
    FileSourceNotification, FileSourceParams, IndexingSettings, SourceConfig, SourceParams,
};
use quickwit_ingest::IngesterPool;
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_metastore::checkpoint::{SourceCheckpoint, SourceCheckpointDelta};
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::{
    IndexMetadataRequest, MetastoreError, MetastoreResult, MetastoreService,
    MetastoreServiceClient, SourceType,
};
use quickwit_proto::types::{IndexUid, NodeIdRef, PipelineUid, ShardId};
use quickwit_storage::StorageResolver;
use serde_json::Value as JsonValue;
pub use source_factory::{SourceFactory, SourceLoader, TypedSourceFactory};
use tokio::runtime::Handle;
use tracing::error;
pub use vec_source::{VecSource, VecSourceFactory};
pub use void_source::{VoidSource, VoidSourceFactory};

use self::doc_file_reader::dir_and_filename;
use self::stdin_source::StdinSourceFactory;
use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::ingest::IngestSourceFactory;
use crate::source::ingest_api_source::IngestApiSourceFactory;

/// Number of bytes after which we cut a new batch.
///
/// We try to emit chewable batches for the indexer.
/// One batch = one message to the indexer actor.
///
/// If batches are too large:
/// - we might not be able to observe the state of the indexer for 5 seconds.
/// - we will be needlessly occupying resident memory in the mailbox.
/// - we will not have a precise control of the timeout before commit.
///
/// 5MB seems like a good one size fits all value.
const BATCH_NUM_BYTES_LIMIT: u64 = ByteSize::mib(5).as_u64();

static EMIT_BATCHES_TIMEOUT: Lazy<Duration> = Lazy::new(|| {
    if cfg!(any(test, feature = "testsuite")) {
        let timeout = Duration::from_millis(100);
        assert!(timeout < *quickwit_actors::HEARTBEAT);
        timeout
    } else {
        let timeout = Duration::from_millis(1_000);
        if *quickwit_actors::HEARTBEAT < timeout {
            error!("QW_ACTOR_HEARTBEAT_SECS smaller than batch timeout");
        }
        timeout
    }
});

/// Runtime configuration used during execution of a source actor.
#[derive(Clone)]
pub struct SourceRuntime {
    pub pipeline_id: IndexingPipelineId,
    pub source_config: SourceConfig,
    pub metastore: MetastoreServiceClient,
    pub ingester_pool: IngesterPool,
    // Ingest API queues directory path.
    pub queues_dir_path: PathBuf,
    pub storage_resolver: StorageResolver,
    pub event_broker: EventBroker,
    pub indexing_setting: IndexingSettings,
}

impl SourceRuntime {
    pub fn node_id(&self) -> &NodeIdRef {
        &self.pipeline_id.node_id
    }

    pub fn index_uid(&self) -> &IndexUid {
        &self.pipeline_id.index_uid
    }

    pub fn index_id(&self) -> &str {
        &self.pipeline_id.index_uid.index_id
    }

    pub fn source_id(&self) -> &str {
        &self.pipeline_id.source_id
    }

    pub fn pipeline_uid(&self) -> PipelineUid {
        self.pipeline_id.pipeline_uid
    }

    pub async fn fetch_checkpoint(&self) -> MetastoreResult<SourceCheckpoint> {
        let index_uid = self.index_uid().clone();
        let request = IndexMetadataRequest::for_index_uid(index_uid);
        let response = self.metastore.clone().index_metadata(request).await?;
        let index_metadata = response.deserialize_index_metadata()?;

        if let Some(checkpoint) = index_metadata
            .checkpoint
            .source_checkpoint(self.source_id())
            .cloned()
        {
            return Ok(checkpoint);
        }
        Err(MetastoreError::Internal {
            message: format!(
                "could not find checkpoint for index `{}` and source `{}`",
                self.index_uid(),
                self.source_id()
            ),
            cause: "".to_string(),
        })
    }
}

pub type SourceContext = ActorContext<SourceActor>;

/// A Source is a trait that is mounted in a light wrapping Actor called `SourceActor`.
///
/// For this reason, its methods mimics those of Actor.
/// One key difference is the absence of messages.
///
/// The `SourceActor` implements a loop until emit_batches returns an
/// ActorExitStatus.
///
/// Conceptually, a source execution works as if it was a simple loop
/// as follow:
///
/// ```ignore
/// fn whatever() -> anyhow::Result<()> {
///     source.initialize(ctx)?;
///     let exit_status = loop {
///         if let Err(exit_status) = source.emit_batches()? {
///             break exit_status;
///         }
///     };
///     source.finalize(exit_status)?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait Source: Send + 'static {
    /// This method will be called before any calls to `emit_batches`.
    async fn initialize(
        &mut self,
        _doc_processor_mailbox: &Mailbox<DocProcessor>,
        _ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    /// Main part of the source implementation, `emit_batches` can emit 0..n batches.
    ///
    /// The `batch_sink` is a mailbox that has a bounded capacity.
    /// In that case, `batch_sink` will block.
    ///
    /// It returns an optional duration specifying how long the batch requester
    /// should wait before polling again.
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus>;

    /// Assign shards is called when the source is assigned a new set of shards by the control
    /// plane.
    async fn assign_shards(
        &mut self,
        _shard_ids: BTreeSet<ShardId>,
        _doc_processor_mailbox: &Mailbox<DocProcessor>,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// After publication of a split, `suggest_truncate` is called.
    /// This makes it possible for the implementation of a source to
    /// release some resources associated to the data that was just published.
    ///
    /// This method is for instance useful for the ingest API, as it is possible
    /// to delete all message anterior to the checkpoint in the ingest API queue.
    ///
    /// It is perfectly fine for implementation to ignore this function.
    /// For instance, message queue like kafka are meant to be shared by different
    /// client, and rely on a retention strategy to delete messages.
    ///
    /// Returning an error has no effect on the source actor itself or the
    /// indexing pipeline, as truncation is just "a suggestion".
    /// The error will however be logged.
    async fn suggest_truncate(
        &mut self,
        _checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Finalize is called once after the actor terminates.
    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// A name identifying the type of source.
    fn name(&self) -> String;

    /// Returns an observable_state for the actor.
    ///
    /// This object is simply a json object, and its content may vary depending on the
    /// source.
    fn observable_state(&self) -> JsonValue;
}

/// The SourceActor acts as a thin wrapper over a source trait object to execute.
///
/// It mostly takes care of running a loop calling `emit_batches(...)`.
pub struct SourceActor {
    pub source: Box<dyn Source>,
    pub doc_processor_mailbox: Mailbox<DocProcessor>,
}

#[derive(Debug)]
struct Loop;

#[derive(Debug)]
pub struct Assignment {
    pub shard_ids: BTreeSet<ShardId>,
}

#[derive(Debug)]
pub struct AssignShards(pub Assignment);

#[async_trait]
impl Actor for SourceActor {
    type ObservableState = JsonValue;

    fn name(&self) -> String {
        self.source.name()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.source.observable_state()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::NonBlocking.get_runtime_handle()
    }

    fn yield_after_each_message(&self) -> bool {
        false
    }

    async fn initialize(&mut self, ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        self.source
            .initialize(&self.doc_processor_mailbox, ctx)
            .await?;
        self.handle(Loop, ctx).await?;
        Ok(())
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.source.finalize(exit_status, ctx).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Loop> for SourceActor {
    type Reply = ();

    async fn handle(&mut self, _message: Loop, ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        let wait_for = self
            .source
            .emit_batches(&self.doc_processor_mailbox, ctx)
            .await?;
        if wait_for.is_zero() {
            ctx.send_self_message(Loop).await?;
            return Ok(());
        }
        ctx.schedule_self_msg(wait_for, Loop);
        Ok(())
    }
}

#[async_trait]
impl Handler<AssignShards> for SourceActor {
    type Reply = ();

    async fn handle(
        &mut self,
        assign_shards_message: AssignShards,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let AssignShards(Assignment { shard_ids }) = assign_shards_message;
        self.source
            .assign_shards(shard_ids, &self.doc_processor_mailbox, ctx)
            .await?;
        Ok(())
    }
}

// TODO: Use `SourceType` instead of `&str``.
pub fn quickwit_supported_sources() -> &'static SourceLoader {
    static SOURCE_LOADER: OnceCell<SourceLoader> = OnceCell::new();
    SOURCE_LOADER.get_or_init(|| {
        let mut source_factory = SourceLoader::default();
        source_factory.add_source(SourceType::File, FileSourceFactory);
        #[cfg(feature = "gcp-pubsub")]
        source_factory.add_source(SourceType::PubSub, GcpPubSubSourceFactory);
        source_factory.add_source(SourceType::IngestV1, IngestApiSourceFactory);
        source_factory.add_source(SourceType::IngestV2, IngestSourceFactory);
        #[cfg(feature = "kafka")]
        source_factory.add_source(SourceType::Kafka, KafkaSourceFactory);
        #[cfg(feature = "kinesis")]
        source_factory.add_source(SourceType::Kinesis, KinesisSourceFactory);
        #[cfg(feature = "pulsar")]
        source_factory.add_source(SourceType::Pulsar, PulsarSourceFactory);
        source_factory.add_source(SourceType::Stdin, StdinSourceFactory);
        source_factory.add_source(SourceType::Vec, VecSourceFactory);
        source_factory.add_source(SourceType::Void, VoidSourceFactory);
        source_factory
    })
}

pub async fn check_source_connectivity(
    storage_resolver: &StorageResolver,
    source_config: &SourceConfig,
) -> anyhow::Result<()> {
    match &source_config.source_params {
        SourceParams::File(FileSourceParams::Filepath(file_uri)) => {
            let (dir_uri, file_name) = dir_and_filename(file_uri)?;
            let storage = storage_resolver.resolve(&dir_uri).await?;
            storage.file_num_bytes(file_name).await?;
            Ok(())
        }
        #[allow(unused_variables)]
        SourceParams::File(FileSourceParams::Notifications(FileSourceNotification::Sqs(
            sqs_config,
        ))) => {
            #[cfg(not(feature = "sqs"))]
            anyhow::bail!("Quickwit was compiled without the `sqs` feature");

            #[cfg(feature = "sqs")]
            {
                queue_sources::sqs_queue::check_connectivity(&sqs_config.queue_url).await?;
                Ok(())
            }
        }
        #[allow(unused_variables)]
        SourceParams::Kafka(params) => {
            #[cfg(not(feature = "kafka"))]
            anyhow::bail!("Quickwit was compiled without the `kafka` feature");

            #[cfg(feature = "kafka")]
            {
                kafka_source::check_connectivity(params.clone()).await?;
                Ok(())
            }
        }
        #[allow(unused_variables)]
        SourceParams::Kinesis(params) => {
            #[cfg(not(feature = "kinesis"))]
            anyhow::bail!("Quickwit was compiled without the `kinesis` feature");

            #[cfg(feature = "kinesis")]
            {
                kinesis::check_connectivity(params.clone()).await?;
                Ok(())
            }
        }
        #[allow(unused_variables)]
        SourceParams::Pulsar(params) => {
            #[cfg(not(feature = "pulsar"))]
            anyhow::bail!("Quickwit was compiled without the `pulsar` feature");

            #[cfg(feature = "pulsar")]
            {
                pulsar_source::check_connectivity(params).await?;
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

#[derive(Debug)]
pub struct SuggestTruncate(pub SourceCheckpoint);

#[async_trait]
impl Handler<SuggestTruncate> for SourceActor {
    type Reply = ();

    async fn handle(
        &mut self,
        suggest_truncate: SuggestTruncate,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let SuggestTruncate(checkpoint) = suggest_truncate;

        if let Err(error) = self.source.suggest_truncate(checkpoint, ctx).await {
            // Failing to process suggest truncate does not
            // kill the source nor the indexing pipeline, but we log the error.
            error!(%error, "failed to process suggest truncate");
        }
        Ok(())
    }
}

pub(super) struct BatchBuilder {
    // Do not directly append documents to this vector; otherwise, in-flight metrics will be
    // incorrect. Use `add_doc` instead.
    docs: Vec<Bytes>,
    num_bytes: u64,
    checkpoint_delta: SourceCheckpointDelta,
    force_commit: bool,
    gauge_guard: GaugeGuard<'static>,
}

impl BatchBuilder {
    pub fn new(source_type: SourceType) -> Self {
        Self::with_capacity(0, source_type)
    }

    pub fn with_capacity(capacity: usize, source_type: SourceType) -> Self {
        let gauge = match source_type {
            SourceType::File => MEMORY_METRICS.in_flight.file(),
            SourceType::IngestV2 => MEMORY_METRICS.in_flight.ingest(),
            SourceType::Kafka => MEMORY_METRICS.in_flight.kafka(),
            SourceType::Kinesis => MEMORY_METRICS.in_flight.kinesis(),
            SourceType::PubSub => MEMORY_METRICS.in_flight.pubsub(),
            SourceType::Pulsar => MEMORY_METRICS.in_flight.pulsar(),
            _ => MEMORY_METRICS.in_flight.other(),
        };
        let gauge_guard = GaugeGuard::from_gauge(gauge);

        Self {
            docs: Vec::with_capacity(capacity),
            num_bytes: 0,
            checkpoint_delta: SourceCheckpointDelta::default(),
            force_commit: false,
            gauge_guard,
        }
    }

    pub fn add_doc(&mut self, doc: Bytes) {
        let num_bytes = doc.len();
        self.docs.push(doc);
        self.gauge_guard.add(num_bytes as i64);
        self.num_bytes += num_bytes as u64;
    }

    pub fn force_commit(&mut self) {
        self.force_commit = true;
    }

    pub fn build(self) -> RawDocBatch {
        RawDocBatch::new(self.docs, self.checkpoint_delta, self.force_commit)
    }

    #[cfg(feature = "kafka")]
    pub fn clear(&mut self) {
        self.docs.clear();
        self.checkpoint_delta = SourceCheckpointDelta::default();
        self.gauge_guard.sub(self.num_bytes as i64);
        self.num_bytes = 0;
    }
}

#[cfg(test)]
mod tests {

    use std::num::NonZeroUsize;

    use quickwit_config::{SourceInputFormat, VecSourceParams};
    use quickwit_metastore::IndexMetadata;
    use quickwit_metastore::checkpoint::IndexCheckpointDelta;
    use quickwit_proto::metastore::{IndexMetadataResponse, MockMetastoreService};
    use quickwit_proto::types::NodeId;

    use super::*;

    pub struct SourceRuntimeBuilder {
        index_uid: IndexUid,
        source_config: SourceConfig,
        metastore_opt: Option<MetastoreServiceClient>,
        queues_dir_path_opt: Option<PathBuf>,
    }

    impl SourceRuntimeBuilder {
        pub fn new(index_uid: IndexUid, source_config: SourceConfig) -> Self {
            SourceRuntimeBuilder {
                index_uid,
                source_config,
                metastore_opt: None,
                queues_dir_path_opt: None,
            }
        }

        pub fn build(mut self) -> SourceRuntime {
            let metastore = self
                .metastore_opt
                .take()
                .unwrap_or_else(|| self.setup_mock_metastore(None));

            let queues_dir_path = self
                .queues_dir_path_opt
                .unwrap_or_else(|| PathBuf::from("./queues"));

            SourceRuntime {
                pipeline_id: IndexingPipelineId {
                    node_id: NodeId::from("test-node"),
                    index_uid: self.index_uid,
                    source_id: self.source_config.source_id.clone(),
                    pipeline_uid: PipelineUid::for_test(0u128),
                },
                metastore,
                ingester_pool: IngesterPool::default(),
                queues_dir_path,
                source_config: self.source_config,
                storage_resolver: StorageResolver::for_test(),
                event_broker: EventBroker::default(),
                indexing_setting: IndexingSettings::default(),
            }
        }

        #[cfg(all(
            test,
            any(feature = "kafka-broker-tests", feature = "sqs-localstack-tests")
        ))]
        pub fn with_metastore(mut self, metastore: MetastoreServiceClient) -> Self {
            self.metastore_opt = Some(metastore);
            self
        }

        pub fn with_mock_metastore(
            mut self,
            source_checkpoint_delta_opt: Option<SourceCheckpointDelta>,
        ) -> Self {
            self.metastore_opt = Some(self.setup_mock_metastore(source_checkpoint_delta_opt));
            self
        }

        pub fn with_queues_dir(mut self, queues_dir_path: impl Into<PathBuf>) -> Self {
            self.queues_dir_path_opt = Some(queues_dir_path.into());
            self
        }

        fn setup_mock_metastore(
            &self,
            source_checkpoint_delta_opt: Option<SourceCheckpointDelta>,
        ) -> MetastoreServiceClient {
            let index_uid = self.index_uid.clone();
            let source_config = self.source_config.clone();

            let mut mock_metastore = MockMetastoreService::new();
            mock_metastore
                .expect_index_metadata()
                .returning(move |_request| {
                    let index_uri = format!("ram:///indexes/{}", index_uid.index_id);
                    let mut index_metadata =
                        IndexMetadata::for_test(&index_uid.index_id, &index_uri);
                    index_metadata.index_uid = index_uid.clone();

                    let source_id = source_config.source_id.clone();
                    index_metadata.add_source(source_config.clone()).unwrap();

                    if let Some(source_delta) = source_checkpoint_delta_opt.clone() {
                        let delta = IndexCheckpointDelta {
                            source_id,
                            source_delta,
                        };
                        index_metadata.checkpoint.try_apply_delta(delta).unwrap();
                    }
                    let response =
                        IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap();
                    Ok(response)
                });
            MetastoreServiceClient::from_mock(mock_metastore)
        }
    }

    #[tokio::test]
    async fn test_check_source_connectivity() -> anyhow::Result<()> {
        {
            let source_config = SourceConfig {
                source_id: "void".to_string(),
                num_pipelines: NonZeroUsize::MIN,
                enabled: true,
                source_params: SourceParams::void(),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            };
            check_source_connectivity(&StorageResolver::for_test(), &source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "vec".to_string(),
                num_pipelines: NonZeroUsize::MIN,
                enabled: true,
                source_params: SourceParams::Vec(VecSourceParams::default()),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            };
            check_source_connectivity(&StorageResolver::for_test(), &source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                num_pipelines: NonZeroUsize::MIN,
                enabled: true,
                source_params: SourceParams::file_from_str("file-does-not-exist.json").unwrap(),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            };
            assert!(
                check_source_connectivity(&StorageResolver::for_test(), &source_config)
                    .await
                    .is_err()
            );
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                num_pipelines: NonZeroUsize::MIN,
                enabled: true,
                source_params: SourceParams::file_from_str("data/test_corpus.json").unwrap(),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            };
            assert!(
                check_source_connectivity(&StorageResolver::for_test(), &source_config)
                    .await
                    .is_ok()
            );
        }
        Ok(())
    }
}

#[cfg(all(
    test,
    any(feature = "sqs-localstack-tests", feature = "kafka-broker-tests")
))]
mod test_setup_helper {

    use quickwit_config::IndexConfig;
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, PartitionId};
    use quickwit_metastore::{CreateIndexRequestExt, SplitMetadata, StageSplitsRequestExt};
    use quickwit_proto::metastore::{CreateIndexRequest, PublishSplitsRequest, StageSplitsRequest};
    use quickwit_proto::types::Position;

    use super::*;
    use crate::new_split_id;

    pub async fn setup_index(
        metastore: MetastoreServiceClient,
        index_id: &str,
        source_config: &SourceConfig,
        partition_deltas: &[(PartitionId, Position, Position)],
    ) -> IndexUid {
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let create_index_request = CreateIndexRequest::try_from_index_and_source_configs(
            &index_config,
            std::slice::from_ref(source_config),
        )
        .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        if partition_deltas.is_empty() {
            return index_uid;
        }
        let split_id = new_split_id();
        let split_metadata = SplitMetadata::for_test(split_id.clone());
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let mut source_delta = SourceCheckpointDelta::default();
        for (partition_id, from_position, to_position) in partition_deltas.iter().cloned() {
            source_delta
                .record_partition_delta(partition_id, from_position, to_position)
                .unwrap();
        }
        let checkpoint_delta = IndexCheckpointDelta {
            source_id: source_config.source_id.to_string(),
            source_delta,
        };
        let checkpoint_delta_json = serde_json::to_string(&checkpoint_delta).unwrap();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            index_checkpoint_delta_json_opt: Some(checkpoint_delta_json),
            staged_split_ids: vec![split_id.clone()],
            replaced_split_ids: Vec::new(),
            publish_token_opt: None,
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();
        index_uid
    }
}
