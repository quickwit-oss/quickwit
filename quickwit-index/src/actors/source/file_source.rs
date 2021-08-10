// Quickwit
//  Copyright (C) 2021 Qu num_bytes: (), line_num: ()  num_bytes: (), line_num: ()  num_bytes: (), line_num: () ickwit Inc.
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

use crate::models::RawDocBatch;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::ActorTermination;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use std::io;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tracing::info;

/// Cut a new batch as soon as we have read BATCH_NUM_BYTES_THRESHOLD.
const BATCH_NUM_BYTES_THRESHOLD: u64 = 500_000u64;

pub struct FileSource {
    file_position: FilePosition,
    file: BufReader<File>,
    sink: Mailbox<RawDocBatch>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilePosition {
    pub num_bytes: u64,
    pub line_num: u64,
}

impl Actor for FileSource {
    type Message = ();

    type ObservableState = FilePosition;

    fn observable_state(&self) -> Self::ObservableState {
        self.file_position
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
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
        let limit_num_bytes = self.file_position.num_bytes + BATCH_NUM_BYTES_THRESHOLD;
        let mut reached_eof = false;
        let mut raw_doc_batch = RawDocBatch::default();
        while self.file_position.num_bytes < limit_num_bytes {
            let mut doc_line = String::new();
            let num_bytes = self
                .file
                .read_line(&mut doc_line)
                .await
                .map_err(|io_err: io::Error| ActorTermination::Failure(anyhow::anyhow!(io_err)))?;
            if num_bytes == 0 {
                reached_eof = true;
                break;
            }
            raw_doc_batch.docs.push(doc_line);
            self.file_position.num_bytes += num_bytes as u64;
            self.file_position.line_num += 1u64;
        }
        if !raw_doc_batch.docs.is_empty() {
            ctx.send_message(&self.sink, raw_doc_batch).await?;
        }
        if reached_eof {
            info!("EOF");
            return Err(ActorTermination::Finished);
        }
        ctx.send_self_message(()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::Universe;

    #[tokio::test]
    async fn test_file_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let file_source = FileSource::try_new(Path::new("data/test_corpus.json"), mailbox).await?;
        let (file_source_mailbox, file_source_handle) = universe.spawn(file_source);
        universe.send_message(&file_source_mailbox, ()).await?;
        let (actor_termination, file_position) = file_source_handle.join().await?;
        assert!(actor_termination.is_finished());
        assert_eq!(
            file_position,
            FilePosition {
                num_bytes: 70,
                line_num: 4
            }
        );
        let batch = inbox.drain_available_message_for_test();
        assert_eq!(batch.len(), 1);
        Ok(())
    }
}
