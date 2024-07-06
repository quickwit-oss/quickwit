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
use quickwit_proto::types::SourceId;
use tracing::info;

use super::doc_file_reader::{DocFileReader, ReadBatchResponse};
use crate::actors::DocProcessor;
use crate::source::{Source, SourceContext, SourceRuntime, TypedSourceFactory};

pub struct FileSource {
    source_id: SourceId,
    reader: DocFileReader,
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
        let ReadBatchResponse { batch_opt, is_eof } = self.reader.read_batch(ctx).await?;
        if let Some(batch) = batch_opt {
            ctx.send_message(doc_processor_mailbox, batch).await?;
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
        serde_json::to_value(self.reader.counters()).unwrap()
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
        let reader: DocFileReader = if let Some(filepath) = &params.filepath {
            let checkpoint = source_runtime.fetch_checkpoint().await?;
            DocFileReader::from_path(&checkpoint, filepath, &source_runtime.storage_resolver)
                .await?
        } else {
            // We cannot use the checkpoint.
            DocFileReader::from_stdin()
        };
        let file_source = FileSource {
            source_id: source_runtime.source_id().to_string(),
            reader,
        };
        Ok(file_source)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::num::NonZeroUsize;

    use quickwit_actors::{Command, Universe};
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
            FileSourceParams::file("data/test_corpus.json.gz")
        } else {
            FileSourceParams::file("data/test_corpus.json")
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
                "previous_offset": 1030u64,
                "current_offset": 1030u64,
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
        let params = FileSourceParams::file(temp_file.path());
        let filepath = params
            .filepath
            .as_ref()
            .unwrap()
            .to_string_lossy()
            .to_string();

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
                "previous_offset": 700_000u64,
                "current_offset": 700_000u64,
                "num_lines_processed": 20_000u64
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
                filepath, "(00000000000000000000..00000000000000500010]"
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
        let temp_file_path = temp_file.path();
        let params = FileSourceParams::file(temp_file_path);
        let source_config = SourceConfig {
            source_id: "test-file-source".to_string(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::File(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let partition_id = PartitionId::from(temp_file_path.to_string_lossy().borrow());
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
                "previous_offset": 290u64,
                "current_offset": 290u64,
                "num_lines_processed": 98u64
            })
        );
        let indexer_messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(&indexer_messages[0].docs[0].starts_with(b"2\n"));
    }
}
