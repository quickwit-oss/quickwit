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

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::Progress;
use quickwit_proto::metastore::SourceType;
use tokio::io::{AsyncBufReadExt, BufReader};

use super::{BATCH_NUM_BYTES_LIMIT, BatchBuilder};
use crate::actors::DocProcessor;
use crate::source::{Source, SourceContext, SourceRuntime, TypedSourceFactory};

pub struct StdinBatchReader {
    reader: BufReader<tokio::io::Stdin>,
    is_eof: bool,
}

impl StdinBatchReader {
    pub fn new() -> Self {
        Self {
            reader: BufReader::new(tokio::io::stdin()),
            is_eof: false,
        }
    }

    async fn read_batch(&mut self, source_progress: &Progress) -> anyhow::Result<BatchBuilder> {
        let mut batch_builder = BatchBuilder::new(SourceType::Stdin);
        while batch_builder.num_bytes < BATCH_NUM_BYTES_LIMIT {
            let mut buf = String::new();
            // stdin might be slow because it's depending on external
            // input (e.g. user typing on a keyboard)
            let bytes_read = source_progress
                .protect_future(self.reader.read_line(&mut buf))
                .await?;
            if bytes_read > 0 {
                batch_builder.add_doc(buf.into());
            } else {
                self.is_eof = true;
                break;
            }
        }

        Ok(batch_builder)
    }

    fn is_eof(&self) -> bool {
        self.is_eof
    }
}

pub struct StdinSource {
    reader: StdinBatchReader,
    num_bytes_processed: u64,
    num_lines_processed: u64,
}

impl fmt::Debug for StdinSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StdinSource")
    }
}

#[async_trait]
impl Source for StdinSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let batch_builder = self.reader.read_batch(ctx.progress()).await?;
        self.num_bytes_processed += batch_builder.num_bytes;
        self.num_lines_processed += batch_builder.docs.len() as u64;
        doc_processor_mailbox
            .send_message(batch_builder.build())
            .await?;
        if self.reader.is_eof() {
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }

        Ok(Duration::ZERO)
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::json!({
            "num_bytes_processed": self.num_bytes_processed,
            "num_lines_processed": self.num_lines_processed,
        })
    }
}

pub struct StdinSourceFactory;

#[async_trait]
impl TypedSourceFactory for StdinSourceFactory {
    type Source = StdinSource;
    type Params = ();

    async fn typed_create_source(
        _source_runtime: SourceRuntime,
        _params: (),
    ) -> anyhow::Result<StdinSource> {
        Ok(StdinSource {
            reader: StdinBatchReader::new(),
            num_bytes_processed: 0,
            num_lines_processed: 0,
        })
    }
}
