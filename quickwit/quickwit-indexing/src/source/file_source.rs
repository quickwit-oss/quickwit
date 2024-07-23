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
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::FileSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::SourceId;

use super::doc_file_reader::{BatchReader, ObjectUriBatchReader, StdinBatchReader};
#[cfg(feature = "sqs")]
use super::queue_sources::QueueCoordinator;
use crate::actors::DocProcessor;
use crate::source::{Source, SourceContext, SourceRuntime, TypedSourceFactory};

enum FileSourceInner {
    #[cfg(feature = "sqs")]
    Queue(QueueCoordinator),
    Stream {
        reader: Box<dyn BatchReader>,
        num_bytes_processed: u64,
        num_lines_processed: u64,
    },
}

pub struct FileSource {
    source_id: SourceId,
    inner: FileSourceInner,
    source_type: SourceType,
}

impl fmt::Debug for FileSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSource {{ source_id: {} }}", self.source_id)
    }
}

#[async_trait]
impl Source for FileSource {
    #[allow(unused_variables)]
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        match &mut self.inner {
            #[cfg(feature = "sqs")]
            FileSourceInner::Queue(coordinator) => {
                coordinator.initialize(doc_processor_mailbox, ctx).await
            }
            FileSourceInner::Stream { .. } => Ok(()),
        }
    }

    #[allow(unused_variables)]
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        match &mut self.inner {
            #[cfg(feature = "sqs")]
            FileSourceInner::Queue(coordinator) => {
                coordinator.emit_batches(doc_processor_mailbox, ctx).await?;
            }
            FileSourceInner::Stream {
                reader,
                num_bytes_processed,
                num_lines_processed,
            } => {
                let batch_builder = reader.read_batch(ctx, self.source_type).await?;
                if batch_builder.num_bytes > 0 {
                    *num_bytes_processed += batch_builder.num_bytes;
                    *num_lines_processed += batch_builder.docs.len() as u64;
                    doc_processor_mailbox
                        .send_message(batch_builder.build())
                        .await?;
                }
                if reader.is_eof() {
                    ctx.send_exit_with_success(doc_processor_mailbox).await?;
                    return Err(ActorExitStatus::Success);
                }
            }
        }
        Ok(Duration::ZERO)
    }

    fn name(&self) -> String {
        format!("{:?}", self)
    }

    #[allow(unused_variables)]
    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        match &mut self.inner {
            #[cfg(feature = "sqs")]
            FileSourceInner::Queue(coordinator) => {
                coordinator.suggest_truncate(checkpoint, ctx).await
            }
            FileSourceInner::Stream { .. } => Ok(()),
        }
    }

    fn observable_state(&self) -> serde_json::Value {
        match &self.inner {
            #[cfg(feature = "sqs")]
            FileSourceInner::Queue(coordinator) => {
                serde_json::json!({
                    "num_bytes_processed": coordinator.observable_state().num_bytes_processed,
                    "num_lines_processed": coordinator.observable_state().num_lines_processed,
                    "num_messages_processed": coordinator.observable_state().num_messages_processed,
                    "num_consecutive_empty_batches": coordinator.observable_state().num_consecutive_empty_batches,
                })
            }
            FileSourceInner::Stream {
                num_bytes_processed,
                num_lines_processed,
                ..
            } => {
                serde_json::json!({
                    "num_bytes_processed": num_bytes_processed,
                    "num_lines_processed": num_lines_processed,
                })
            }
        }
    }
}

pub struct FileSourceFactory;

#[async_trait]
impl TypedSourceFactory for FileSourceFactory {
    type Source = FileSource;
    type Params = FileSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        params: FileSourceParams,
    ) -> anyhow::Result<FileSource> {
        let source_id = source_runtime.source_config.source_id.clone();
        let source_type = source_runtime.source_config.source_type();
        let inner = match params {
            FileSourceParams::FileUri(file_uri) => {
                let partition_id = PartitionId::from(file_uri.filepath.as_str());
                let position = source_runtime
                    .fetch_checkpoint()
                    .await?
                    .position_for_partition(&partition_id)
                    .cloned()
                    .unwrap_or_default();
                let batch_reader = ObjectUriBatchReader::try_new(
                    &source_runtime.storage_resolver,
                    partition_id,
                    &file_uri.filepath,
                    position,
                )
                .await?;
                FileSourceInner::Stream {
                    reader: Box::new(batch_reader),
                    num_bytes_processed: 0,
                    num_lines_processed: 0,
                }
            }
            FileSourceParams::Stdin => FileSourceInner::Stream {
                reader: Box::new(StdinBatchReader::new()),
                num_bytes_processed: 0,
                num_lines_processed: 0,
            },
            #[cfg(feature = "sqs")]
            FileSourceParams::Sqs(sqs_config) => {
                let coordinator =
                    QueueCoordinator::try_from_sqs_config(sqs_config, source_runtime).await?;
                FileSourceInner::Queue(coordinator)
            }
            #[cfg(not(feature = "sqs"))]
            FileSourceParams::Sqs(_) => {
                anyhow::bail!("Quickwit was compiled without the `sqs` feature")
            }
        };

        Ok(FileSource {
            inner,
            source_id,
            source_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use quickwit_actors::{Command, Universe};
    use quickwit_common::uri::Uri;
    use quickwit_config::{FileSourceUri, SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpointDelta};
    use quickwit_proto::types::{IndexUid, Position};

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::doc_file_reader::file_test_helpers::{
        generate_dummy_doc_file, generate_index_doc_file, DUMMY_DOC,
    };
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{SourceActor, BATCH_NUM_BYTES_LIMIT};

    #[tokio::test]
    async fn test_file_source() {
        aux_test_file_source(false).await;
        aux_test_file_source(true).await;
    }

    async fn aux_test_file_source(gzip: bool) {
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let params = if gzip {
            FileSourceParams::from_filepath("data/test_corpus.json.gz").unwrap()
        } else {
            FileSourceParams::from_filepath("data/test_corpus.json").unwrap()
        };
        let source_config = SourceConfig {
            source_id: "test-file-source".to_string(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::File(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let file_source = FileSourceFactory::typed_create_source(source_runtime, params)
            .await
            .unwrap();
        let file_source_actor = SourceActor {
            source: Box::new(file_source),
            doc_processor_mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_builder().spawn(file_source_actor);
        let (actor_termination, counters) = file_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            counters,
            serde_json::json!({
                "num_bytes_processed": 1030u64,
                "num_lines_processed": 4u32
            })
        );
        let batch = indexer_inbox.drain_for_test();
        assert_eq!(batch.len(), 2);
        batch[0].downcast_ref::<RawDocBatch>().unwrap();
        assert!(matches!(
            batch[1].downcast_ref::<Command>().unwrap(),
            Command::ExitWithSuccess
        ));
    }

    #[tokio::test]
    async fn test_file_source_several_batch() {
        aux_test_file_source_several_batch(false).await;
        aux_test_file_source_several_batch(true).await;
    }

    async fn aux_test_file_source_several_batch(gzip: bool) {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let lines = BATCH_NUM_BYTES_LIMIT as usize / DUMMY_DOC.len() + 1;
        let (temp_file, temp_file_size) = generate_dummy_doc_file(gzip, lines).await;
        let filepath = temp_file.path().to_str().unwrap();
        let uri = Uri::from_str(filepath).unwrap();
        let params = FileSourceParams::FileUri(FileSourceUri {
            filepath: uri.clone(),
        });
        let source_config = SourceConfig {
            source_id: "test-file-source".to_string(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::File(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let file_source = FileSourceFactory::typed_create_source(source_runtime, params)
            .await
            .unwrap();
        let file_source_actor = SourceActor {
            source: Box::new(file_source),
            doc_processor_mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_builder().spawn(file_source_actor);
        let (actor_termination, counters) = file_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            counters,
            serde_json::json!({
                "num_lines_processed": lines,
                "num_bytes_processed": temp_file_size,
            })
        );
        let indexer_msgs = doc_processor_inbox.drain_for_test();
        assert_eq!(indexer_msgs.len(), 3);
        let batch1 = indexer_msgs[0].downcast_ref::<RawDocBatch>().unwrap();
        let batch2 = indexer_msgs[1].downcast_ref::<RawDocBatch>().unwrap();
        let command = indexer_msgs[2].downcast_ref::<Command>().unwrap();
        assert_eq!(
            format!("{:?}", &batch1.checkpoint_delta),
            format!(
                "∆({}:{})",
                uri, "(00000000000000000000..00000000000005242895]"
            )
        );
        assert_eq!(
            format!("{:?}", &batch2.checkpoint_delta),
            format!(
                "∆({}:{})",
                uri, "(00000000000005242895..~00000000000005397105]"
            )
        );
        assert!(matches!(command, &Command::ExitWithSuccess));
    }

    #[tokio::test]
    async fn test_file_source_resume_from_checkpoint() {
        aux_test_file_source_resume_from_checkpoint(false).await;
        aux_test_file_source_resume_from_checkpoint(true).await;
    }

    async fn aux_test_file_source_resume_from_checkpoint(gzip: bool) {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let temp_file = generate_index_doc_file(gzip, 100).await;
        let temp_file_path = temp_file.path().to_str().unwrap();
        let uri = Uri::from_str(temp_file_path).unwrap();
        let params = FileSourceParams::FileUri(FileSourceUri {
            filepath: uri.clone(),
        });
        let source_config = SourceConfig {
            source_id: "test-file-source".to_string(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::File(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let partition_id = PartitionId::from(uri.as_str());
        let source_checkpoint_delta = SourceCheckpointDelta::from_partition_delta(
            partition_id,
            Position::Beginning,
            Position::offset(4u64),
        )
        .unwrap();

        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_mock_metastore(Some(source_checkpoint_delta))
            .with_queues_dir(temp_file_path)
            .build();

        let file_source = FileSourceFactory::typed_create_source(source_runtime, params)
            .await
            .unwrap();
        let file_source_actor = SourceActor {
            source: Box::new(file_source),
            doc_processor_mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_builder().spawn(file_source_actor);
        let (actor_termination, counters) = file_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            counters,
            serde_json::json!({
                "num_bytes_processed": 286u64,
                "num_lines_processed": 98u64
            })
        );
        let indexer_messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(&indexer_messages[0].docs[0].starts_with(b"2\n"));
    }
}

#[cfg(all(test, feature = "sqs-localstack-tests"))]
mod localstack_tests {
    use std::str::FromStr;

    use quickwit_actors::Universe;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::uri::Uri;
    use quickwit_config::{FileSourceMessageType, FileSourceSqs, SourceConfig, SourceParams};
    use quickwit_metastore::metastore_for_test;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::doc_file_reader::file_test_helpers::generate_dummy_doc_file;
    use crate::source::queue_sources::sqs_queue::test_helpers::{
        create_queue, get_localstack_sqs_client, send_message,
    };
    use crate::source::test_setup_helper::setup_index;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_file_source_sqs_notifications() {
        // queue setup
        let sqs_client = get_localstack_sqs_client().await.unwrap();
        let queue_url = create_queue(&sqs_client, "check-connectivity").await;
        let (dummy_doc_file, _) = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        send_message(&sqs_client, &queue_url, test_uri.as_str()).await;

        // source setup
        let source_params = FileSourceParams::Sqs(FileSourceSqs {
            queue_url,
            wait_time_seconds: 1,
            message_type: FileSourceMessageType::RawUri,
        });
        let source_config = SourceConfig::for_test(
            "test-file-source-sqs-notifications",
            SourceParams::File(source_params.clone()),
        );
        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-sqs-index");
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let sqs_source = FileSourceFactory::typed_create_source(source_runtime, source_params)
            .await
            .unwrap();

        // actor setup
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        {
            let actor = SourceActor {
                source: Box::new(sqs_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_builder().spawn(actor);

            // run the source actor for a while
            tokio::time::timeout(Duration::from_millis(500), handle.join())
                .await
                .unwrap_err();

            let next_message = doc_processor_inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
                .map(|box_raw_doc_batch| *box_raw_doc_batch)
                .next()
                .unwrap();
            assert_eq!(next_message.docs.len(), 10);
        }
        universe.assert_quit().await;
    }
}
