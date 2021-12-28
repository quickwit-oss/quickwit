// Copyright (C) 2021 Quickwit, Inc.
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

use std::io;
use std::io::SeekFrom;
use std::path::PathBuf;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_metastore::checkpoint::{CheckpointDelta, PartitionId, Position};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncSeekExt, BufReader};
use tracing::info;

use crate::models::{IndexerMessage, RawDocBatch};
use crate::source::{Source, SourceContext, TypedSourceFactory};

/// Cut a new batch as soon as we have read BATCH_NUM_BYTES_THRESHOLD.
const BATCH_NUM_BYTES_THRESHOLD: u64 = 500_000u64;

/// Source id used to define a stdin source.
pub const STDIN_SOURCE_ID: &str = "stdin-source";

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FileSourceCounters {
    pub previous_offset: u64,
    pub current_offset: u64,
    pub num_lines_processed: u64,
}

pub struct FileSource {
    params: FileSourceParams,
    counters: FileSourceCounters,
    reader: BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilePosition {
    pub num_bytes: u64,
}

#[async_trait]
impl Source for FileSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<IndexerMessage>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        // We collect batches of documents before sending them to the indexer.
        let limit_num_bytes = self.counters.previous_offset + BATCH_NUM_BYTES_THRESHOLD;
        let mut reached_eof = false;
        let mut docs = Vec::new();
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
            docs.push(doc_line);
            self.counters.current_offset += num_bytes as u64;
            self.counters.num_lines_processed += 1;
        }
        if !docs.is_empty() {
            let checkpoint_delta = self
                .params
                .filepath
                .as_ref()
                .map(|filepath| {
                    CheckpointDelta::from_partition_delta(
                        PartitionId::from(filepath.to_string_lossy().to_string()),
                        Position::from(self.counters.previous_offset),
                        Position::from(self.counters.current_offset),
                    )
                })
                .unwrap_or_else(CheckpointDelta::default);
            let raw_doc_batch = RawDocBatch {
                docs,
                checkpoint_delta,
            };
            self.counters.previous_offset = self.counters.current_offset;
            ctx.send_message(batch_sink, raw_doc_batch.into()).await?;
        }
        if reached_eof {
            info!("EOF");
            ctx.send_exit_with_success(batch_sink).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(())
    }

    fn name(&self) -> String {
        "FileSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::to_value(&self.counters).unwrap()
    }
}

// TODO handle log directories.
#[derive(Serialize, Deserialize, Debug)]
pub struct FileSourceParams {
    pub filepath: Option<PathBuf>, //< If None read from stdin.
}

pub struct FileSourceFactory;

#[async_trait]
impl TypedSourceFactory for FileSourceFactory {
    type Source = FileSource;

    type Params = FileSourceParams;

    // TODO handle checkpoint for files.
    async fn typed_create_source(
        params: FileSourceParams,
        checkpoint: quickwit_metastore::checkpoint::SourceCheckpoint,
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

    use quickwit_actors::{create_test_mailbox, Command, CommandOrMessage, Universe};
    use quickwit_metastore::checkpoint::SourceCheckpoint;

    use super::*;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_file_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let params = FileSourceParams {
            filepath: Some(PathBuf::from("data/test_corpus.json")),
        };
        let file_source =
            FileSourceFactory::typed_create_source(params, SourceCheckpoint::default()).await?;
        let file_source_actor = SourceActor {
            source: Box::new(file_source),
            batch_sink: mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_actor(file_source_actor).spawn_async();
        let (actor_termination, counters) = file_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            counters,
            serde_json::json!({
                "previous_offset": 70u64,
                "current_offset": 70u64,
                "num_lines_processed": 4
            })
        );
        let batch = inbox.drain_available_message_or_command_for_test();
        assert!(matches!(
            batch[1],
            CommandOrMessage::Command(Command::ExitWithSuccess)
        ));
        assert_eq!(batch.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_source_several_batch() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        use tempfile::NamedTempFile;
        let mut temp_file = NamedTempFile::new()?;
        let temp_path = temp_file.path().to_path_buf();
        for _ in 0..20_000 {
            temp_file.write_all(r#"{"body": "hello happy tax payer!"}"#.as_bytes())?;
            temp_file.write_all("\n".as_bytes())?;
        }
        temp_file.flush()?;
        let params = FileSourceParams {
            filepath: Some(temp_path.as_path().to_path_buf()),
        };
        let source =
            FileSourceFactory::typed_create_source(params, SourceCheckpoint::default()).await?;
        let file_source_actor = SourceActor {
            source: Box::new(source),
            batch_sink: mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_actor(file_source_actor).spawn_async();
        let (actor_termination, counters) = file_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            counters,
            serde_json::json!({
                "previous_offset": 700_000u64,
                "current_offset": 700_000u64,
                "num_lines_processed": 20_000
            })
        );
        let indexer_msgs = inbox.drain_available_message_or_command_for_test();
        assert_eq!(indexer_msgs.len(), 3);
        let mut msgs_it = indexer_msgs.into_iter();
        let msg1 = msgs_it.next().unwrap();
        let msg2 = msgs_it.next().unwrap();
        let msg3 = msgs_it.next().unwrap();
        let batch1 = extract_batch_from_indexer_message(msg1.message().unwrap()).unwrap();
        let batch2 = extract_batch_from_indexer_message(msg2.message().unwrap()).unwrap();
        assert_eq!(
            &extract_position_delta(&batch1.checkpoint_delta).unwrap(),
            "00000000000000000000..00000000000000500010"
        );
        assert_eq!(
            &extract_position_delta(&batch2.checkpoint_delta).unwrap(),
            "00000000000000500010..00000000000000700000"
        );
        assert!(matches!(
            &msg3,
            &CommandOrMessage::Command(Command::ExitWithSuccess)
        ));
        Ok(())
    }

    fn extract_position_delta(checkpoint_delta: &CheckpointDelta) -> Option<String> {
        let checkpoint_delta_str = format!("{:?}", checkpoint_delta);
        let (_left, right) =
            &checkpoint_delta_str[..checkpoint_delta_str.len() - 2].rsplit_once("(")?;
        Some(right.to_string())
    }

    fn extract_batch_from_indexer_message(indexer_msg: IndexerMessage) -> Option<RawDocBatch> {
        if let IndexerMessage::Batch(batch) = indexer_msg {
            Some(batch)
        } else {
            None
        }
    }

    #[tokio::test]
    async fn test_file_source_resume_from_checkpoint() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        use tempfile::NamedTempFile;
        let mut temp_file = NamedTempFile::new()?;
        for i in 0..100 {
            temp_file.write_all(format!("{}\n", i).as_bytes())?;
        }
        temp_file.flush()?;
        let temp_file_path = temp_file.path().canonicalize()?;
        let params = FileSourceParams {
            filepath: Some(temp_file_path.clone()),
        };
        let mut checkpoint = SourceCheckpoint::default();
        let partition_id = PartitionId::from(temp_file_path.to_string_lossy().to_string());
        let checkpoint_delta = CheckpointDelta::from_partition_delta(
            partition_id,
            Position::from(0u64),
            Position::from(4u64),
        );
        checkpoint.try_apply_delta(checkpoint_delta)?;
        let source = FileSourceFactory::typed_create_source(params, checkpoint).await?;
        let file_source_actor = SourceActor {
            source: Box::new(source),
            batch_sink: mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_actor(file_source_actor).spawn_async();
        let (actor_termination, counters) = file_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            counters,
            serde_json::json!({
                "previous_offset": 290u64,
                "current_offset": 290u64,
                "num_lines_processed": 98
            })
        );
        let indexer_msgs = inbox.drain_available_message_for_test();
        assert!(
            matches!(&indexer_msgs[0], IndexerMessage::Batch(raw_batch) if raw_batch.docs[0].starts_with("2\n"))
        );
        Ok(())
    }
}
