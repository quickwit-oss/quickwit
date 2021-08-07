// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use quickwit_actors::MessageProcessError;
use std::io;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;

use crate::models::RawDocBatch;

/// Cut a new batch as soon as we have read BATCH_NUM_BYTES_THRESHOLD.
const BATCH_NUM_BYTES_THRESHOLD: u64 = 500_000u64;

pub struct FileSource {
    file_position: FilePosition,
    file: BufReader<File>,
    sink: Mailbox<RawDocBatch>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilePosition {
    num_bytes: u64,
    line_num: u64,
}

impl Actor for FileSource {
    type Message = ();

    type ObservableState = FilePosition;

    fn observable_state(&self) -> Self::ObservableState {
        self.file_position
    }

    fn default_message(&self) -> Option<Self::Message> {
        Some(())
    }
}
impl FileSource {
    pub async fn try_new(path: &Path, sink: Mailbox<RawDocBatch>) -> io::Result<FileSource> {
        let file = File::open(path).await?;
        Ok(FileSource {
            file_position: FilePosition::default(),
            file: BufReader::new(file),
            sink,
        })
    }
}

#[async_trait]
impl AsyncActor for FileSource {
    async fn process_message(
        &mut self,
        _message: Self::Message,
        _context: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
        let limit_num_bytes = self.file_position.num_bytes + BATCH_NUM_BYTES_THRESHOLD;
        let mut reached_eof = false;
        let mut raw_doc_batch = RawDocBatch::default();
        while self.file_position.num_bytes < limit_num_bytes {
            let mut doc_line = String::new();
            let num_bytes = self
                .file
                .read_line(&mut doc_line)
                .await
                .map_err(|io_err: io::Error| MessageProcessError::Error(anyhow::anyhow!(io_err)))?;
            if num_bytes == 0 {
                reached_eof = true;
                break;
            }
            raw_doc_batch.docs.push(doc_line);
            self.file_position.num_bytes += num_bytes as u64;
            self.file_position.line_num += 1u64;
        }
        if !raw_doc_batch.docs.is_empty() {
            self.sink.send_async(raw_doc_batch).await?;
        }
        if reached_eof {
            return Err(MessageProcessError::Terminated);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::KillSwitch;

    use super::*;
    use quickwit_actors::ActorTermination;

    #[tokio::test]
    async fn test_file_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let (mailbox, inbox) = create_test_mailbox();
        let file_source = FileSource::try_new(Path::new("data/test_corpus.json"), mailbox).await?;
        let file_source_handle = file_source.spawn(KillSwitch::default());
        let actor_termination = file_source_handle.join().await?;
        assert!(matches!(actor_termination, ActorTermination::Disconnect));
        let batch = inbox.drain_available_message_for_test();
        assert_eq!(batch.len(), 1);
        Ok(())
    }
}
