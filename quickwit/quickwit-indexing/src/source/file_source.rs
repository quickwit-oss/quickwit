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

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::FileSourceParams;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{Position, SourceId};
use serde::Serialize;
use tracing::info;

use super::doc_file_reader::DocFileReader;
use super::BatchBuilder;
use crate::actors::DocProcessor;
use crate::source::{Source, SourceContext, SourceRuntime, TypedSourceFactory};

/// Number of bytes after which a new batch is cut.
pub(crate) const BATCH_NUM_BYTES_LIMIT: u64 = 500_000;

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FileSourceCounters {
    pub offset: u64,
    pub num_lines_processed: usize,
}

pub struct FileSource {
    source_id: SourceId,
    reader: DocFileReader,
    partition_id: Option<PartitionId>,
    counters: FileSourceCounters,
}

impl fmt::Debug for FileSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSource {{ source_id: {} }}", self.source_id)
    }
}

#[async_trait]
impl Source for FileSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let limit_num_bytes = self.counters.offset + BATCH_NUM_BYTES_LIMIT;
        let mut new_offset = self.counters.offset;
        let mut batch_builder = BatchBuilder::new(SourceType::File);
        let mut is_eof = false;
        while new_offset < limit_num_bytes {
            if let Some(record) = ctx.protect_future(self.reader.next_record()).await? {
                new_offset = record.next_offset;
                self.counters.num_lines_processed += 1;
                batch_builder.add_doc(record.doc);
            } else {
                is_eof = true;
                break;
            }
        }
        if new_offset > self.counters.offset {
            if let Some(partition_id) = &self.partition_id {
                batch_builder
                    .checkpoint_delta
                    .record_partition_delta(
                        partition_id.clone(),
                        Position::offset(self.counters.offset),
                        Position::offset(new_offset),
                    )
                    .unwrap();
            }
            ctx.send_message(doc_processor_mailbox, batch_builder.build())
                .await?;
            self.counters.offset = new_offset;
        }
        if is_eof {
            info!("reached end of file");
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("{:?}", self)
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::to_value(&self.counters).unwrap()
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
        let Some(uri) = &params.filepath else {
            return Ok(FileSource {
                source_id: source_runtime.source_id().to_string(),
                reader: DocFileReader::from_stdin(),
                partition_id: None,
                counters: FileSourceCounters::default(),
            });
        };

        let partition_id = PartitionId::from(uri.as_str());
        let checkpoint = source_runtime.fetch_checkpoint().await?;
        let offset = checkpoint
            .position_for_partition(&partition_id)
            .map(|position| {
                position
                    .as_usize()
                    .context("file offset should be stored as usize")
            })
            .transpose()?
            .unwrap_or(0);
        let reader = DocFileReader::from_uri(&source_runtime.storage_resolver, uri, offset).await?;
        Ok(FileSource {
            source_id: source_runtime.source_id().to_string(),
            reader,
            partition_id: Some(partition_id),
            counters: FileSourceCounters {
                offset: offset as u64,
                ..Default::default()
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use quickwit_actors::{Command, Universe};
    use quickwit_common::uri::Uri;
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpointDelta};
    use quickwit_proto::types::{IndexUid, Position};

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::doc_file_reader::file_test_helpers::{
        generate_dummy_doc_file, generate_index_doc_file,
    };
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_file_source() {
        aux_test_file_source(false).await;
        aux_test_file_source(true).await;
    }

    async fn aux_test_file_source(gzip: bool) {
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let params = if gzip {
            FileSourceParams::from_str("data/test_corpus.json.gz").unwrap()
        } else {
            FileSourceParams::from_str("data/test_corpus.json").unwrap()
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
                "offset": 1030u64,
                "num_lines_processed": 4u32
            })
        );
        let batch = indexer_inbox.drain_for_test();
        assert_eq!(batch.len(), 2);
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
        let temp_file = generate_dummy_doc_file(gzip, 20_000).await;
        let filepath = temp_file.path().to_str().unwrap();
        let params = FileSourceParams::from_str(filepath).unwrap();

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
                "offset": 700_000u64,
                "num_lines_processed": 20_000u64
            })
        );
        let indexer_msgs = doc_processor_inbox.drain_for_test();
        assert_eq!(indexer_msgs.len(), 3);
        let batch1 = indexer_msgs[0].downcast_ref::<RawDocBatch>().unwrap();
        let batch2 = indexer_msgs[1].downcast_ref::<RawDocBatch>().unwrap();
        let command = indexer_msgs[2].downcast_ref::<Command>().unwrap();
        let uri = Uri::from_str(filepath).unwrap();
        assert_eq!(
            format!("{:?}", &batch1.checkpoint_delta),
            format!(
                "∆({}:{})",
                uri, "(00000000000000000000..00000000000000500010]"
            )
        );
        assert_eq!(
            &extract_position_delta(&batch1.checkpoint_delta).unwrap(),
            "00000000000000000000..00000000000000500010"
        );
        assert_eq!(
            &extract_position_delta(&batch2.checkpoint_delta).unwrap(),
            "00000000000000500010..00000000000000700000"
        );
        assert!(matches!(command, &Command::ExitWithSuccess));
    }

    fn extract_position_delta(checkpoint_delta: &SourceCheckpointDelta) -> Option<String> {
        let checkpoint_delta_str = format!("{checkpoint_delta:?}");
        let (_left, right) =
            &checkpoint_delta_str[..checkpoint_delta_str.len() - 2].rsplit_once('(')?;
        Some(right.to_string())
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
        let params = FileSourceParams::from_str(temp_file_path).unwrap();
        let source_config = SourceConfig {
            source_id: "test-file-source".to_string(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::File(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let partition_id = PartitionId::from(temp_file_path);
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
                "offset": 290u64,
                "num_lines_processed": 98u64
            })
        );
        let indexer_messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(&indexer_messages[0].docs[0].starts_with(b"2\n"));
    }
}
