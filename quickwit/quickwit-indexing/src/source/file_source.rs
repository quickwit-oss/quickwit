// Copyright (C) 2022 Quickwit, Inc.
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

use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, io};

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::FileSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use serde::Serialize;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncSeekExt, BufReader};
use tracing::info;

use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, SourceExecutionContext, TypedSourceFactory};

/// Number of bytes after which a new batch is cut.
pub(crate) const BATCH_NUM_BYTES_LIMIT: u64 = 500_000u64;

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FileSourceCounters {
    pub previous_offset: u64,
    pub current_offset: u64,
    pub num_lines_processed: u64,
}

pub struct FileSource {
    source_id: String,
    params: FileSourceParams,
    counters: FileSourceCounters,
    reader: BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>,
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
        // We collect batches of documents before sending them to the indexer.
        let limit_num_bytes = self.counters.previous_offset + BATCH_NUM_BYTES_LIMIT;
        let mut reached_eof = false;
        let mut doc_batch = RawDocBatch::default();
        while self.counters.current_offset < limit_num_bytes {
            let mut doc_line = String::new();
            let num_bytes = self
                .reader
                .read_line(&mut doc_line)
                .await
                .map_err(|io_err: io::Error| anyhow::anyhow!(io_err))?;
            if num_bytes == 0 {
                reached_eof = true;
                break;
            }
            doc_batch.docs.push(doc_line);
            self.counters.current_offset += num_bytes as u64;
            self.counters.num_lines_processed += 1;
        }
        if !doc_batch.docs.is_empty() {
            if let Some(filepath) = &self.params.filepath {
                let filepath_str = filepath
                    .to_str()
                    .context("Path is invalid utf-8")?
                    .to_string();
                let partition_id = PartitionId::from(filepath_str);
                doc_batch
                    .checkpoint_delta
                    .record_partition_delta(
                        partition_id,
                        Position::from(self.counters.previous_offset),
                        Position::from(self.counters.current_offset),
                    )
                    .unwrap();
            }
            self.counters.previous_offset = self.counters.current_offset;
            ctx.send_message(doc_processor_mailbox, doc_batch).await?;
        }
        if reached_eof {
            info!("EOF");
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("FileSource{{source_id={}}}", self.source_id)
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

    // TODO handle checkpoint for files.
    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: FileSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<FileSource> {
        let mut offset = 0;
        let reader: Box<dyn AsyncRead + Send + Sync + Unpin> =
            if let Some(filepath) = &params.filepath {
                let mut file = File::open(&filepath).await.with_context(|| {
                    format!("Failed to open source file `{}`.", filepath.display())
                })?;
                let partition_id = PartitionId::from(filepath.to_string_lossy().to_string());
                if let Some(Position::Offset(offset_str)) =
                    checkpoint.position_for_partition(&partition_id).cloned()
                {
                    offset = offset_str.parse::<u64>()?;
                    file.seek(SeekFrom::Start(offset)).await?;
                }
                Box::new(file)
            } else {
                // We cannot use the checkpoint.
                Box::new(tokio::io::stdin())
            };
        let file_source = FileSource {
            source_id: ctx.source_config.source_id.clone(),
            counters: FileSourceCounters {
                previous_offset: offset,
                current_offset: offset,
                num_lines_processed: 0,
            },
            reader: BufReader::new(reader),
            params,
        };
        Ok(file_source)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use quickwit_actors::{create_test_mailbox, Command, SpawnContext, Universe};
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::checkpoint::{SourceCheckpoint, SourceCheckpointDelta};
    use quickwit_metastore::metastore_for_test;

    use super::*;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_file_source() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (doc_processor_mailbox, indexer_inbox) = create_test_mailbox();
        let params = FileSourceParams::file("data/test_corpus.json");

        let metastore = metastore_for_test();
        let file_source = FileSourceFactory::typed_create_source(
            SourceExecutionContext::for_test(
                metastore,
                "test-index",
                SourceConfig {
                    source_id: "test-file-source".to_string(),
                    num_pipelines: 1,
                    source_params: SourceParams::File(params.clone()),
                },
            ),
            params,
            SourceCheckpoint::default(),
        )
        .await?;
        let file_source_actor = SourceActor {
            source: Box::new(file_source),
            doc_processor_mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_actor(file_source_actor).spawn();
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
        Ok(())
    }

    #[tokio::test]
    async fn test_file_source_several_batch() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
        use tempfile::NamedTempFile;
        let mut temp_file = NamedTempFile::new()?;
        let temp_path = temp_file.path().to_path_buf();
        for _ in 0..20_000 {
            temp_file.write_all(r#"{"body": "hello happy tax payer!"}"#.as_bytes())?;
            temp_file.write_all("\n".as_bytes())?;
        }
        temp_file.flush()?;
        let params = FileSourceParams::file(temp_path);
        let filepath = params
            .filepath
            .as_ref()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let metastore = metastore_for_test();
        let source = FileSourceFactory::typed_create_source(
            SourceExecutionContext::for_test(
                metastore,
                "test-index",
                SourceConfig {
                    source_id: "test-file-source".to_string(),
                    num_pipelines: 1,
                    source_params: SourceParams::File(params.clone()),
                },
            ),
            params,
            SourceCheckpoint::default(),
        )
        .await?;
        let file_source_actor = SourceActor {
            source: Box::new(source),
            doc_processor_mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_actor(file_source_actor).spawn();
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
        Ok(())
    }

    fn extract_position_delta(checkpoint_delta: &SourceCheckpointDelta) -> Option<String> {
        let checkpoint_delta_str = format!("{:?}", checkpoint_delta);
        let (_left, right) =
            &checkpoint_delta_str[..checkpoint_delta_str.len() - 2].rsplit_once('(')?;
        Some(right.to_string())
    }

    #[tokio::test]
    async fn test_file_source_resume_from_checkpoint() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
        use tempfile::NamedTempFile;
        let mut temp_file = NamedTempFile::new()?;
        for i in 0..100 {
            temp_file.write_all(format!("{}\n", i).as_bytes())?;
        }
        temp_file.flush()?;
        let temp_file_path = temp_file.path().canonicalize()?;
        let params = FileSourceParams::file(&temp_file_path);
        let mut checkpoint = SourceCheckpoint::default();
        let partition_id = PartitionId::from(temp_file_path.to_string_lossy().to_string());
        let checkpoint_delta = SourceCheckpointDelta::from_partition_delta(
            partition_id,
            Position::from(0u64),
            Position::from(4u64),
        );
        checkpoint.try_apply_delta(checkpoint_delta)?;

        let metastore = metastore_for_test();
        let source = FileSourceFactory::typed_create_source(
            SourceExecutionContext::for_test(
                metastore,
                "test-index",
                SourceConfig {
                    source_id: "test-file-source".to_string(),
                    num_pipelines: 1,
                    source_params: SourceParams::File(params.clone()),
                },
            ),
            params,
            checkpoint,
        )
        .await?;
        let file_source_actor = SourceActor {
            source: Box::new(source),
            doc_processor_mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_actor(file_source_actor).spawn();
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
        assert!(indexer_messages[0].docs[0].starts_with("2\n"));
        Ok(())
    }
}
