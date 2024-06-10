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

use std::borrow::Borrow;
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use quickwit_common::uri::Uri;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::Position;
use quickwit_storage::StorageResolver;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use super::{BatchBuilder, SourceContext};
use crate::models::RawDocBatch;

/// Number of bytes after which a new batch is cut.
pub(crate) const BATCH_NUM_BYTES_LIMIT: u64 = 500_000u64;

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DocFileCounters {
    pub previous_offset: u64,
    pub current_offset: u64,
    pub num_lines_processed: u64,
}

/// A helper wrapper that lets you skip bytes in compressed files where you
/// cannot seek.
struct SkipReader {
    reader: BufReader<Box<dyn AsyncRead + Send + Unpin>>,
    num_bytes_to_skip: usize,
}

impl SkipReader {
    fn new(reader: Box<dyn AsyncRead + Send + Unpin>, num_bytes_to_skip: usize) -> Self {
        Self {
            reader: BufReader::new(reader),
            num_bytes_to_skip,
        }
    }

    // This function is only called for GZIP file.
    // Because they cannot be seeked into, we have to scan them to the right initial position.
    async fn skip(&mut self) -> io::Result<()> {
        // Allocate once a 64kb buffer.
        let mut buf = [0u8; 64000];
        while self.num_bytes_to_skip > 0 {
            let num_bytes_to_read = self.num_bytes_to_skip.min(buf.len());
            let num_bytes_read = self
                .reader
                .read_exact(&mut buf[..num_bytes_to_read])
                .await?;
            self.num_bytes_to_skip -= num_bytes_read;
        }
        Ok(())
    }

    async fn read_line<'a>(&mut self, buf: &'a mut String) -> io::Result<usize> {
        if self.num_bytes_to_skip > 0 {
            self.skip().await?;
        }
        self.reader.read_line(buf).await
    }
}

pub struct DocFileReader {
    reader: SkipReader,
    counters: DocFileCounters,
    partition_id: Option<PartitionId>,
}

impl DocFileReader {
    pub async fn from_path(
        checkpoint: &SourceCheckpoint,
        filepath: &PathBuf,
        storage_resolver: &StorageResolver,
    ) -> anyhow::Result<Self> {
        let partition_id = PartitionId::from(filepath.to_string_lossy().borrow());
        let offset = checkpoint
            .position_for_partition(&partition_id)
            .map(|position| {
                position
                    .as_usize()
                    .expect("file offset should be stored as usize")
            })
            .unwrap_or(0);
        let (dir_uri, file_name) = dir_and_filename(filepath)?;
        let storage = storage_resolver.resolve(&dir_uri).await?;
        let file_size = storage.file_num_bytes(file_name).await?.try_into().unwrap();
        // If it's a gzip file, we can't seek to a specific offset, we need to start from the
        // beginning of the file, decompress and skip the first `offset` bytes.
        let reader = if filepath.extension() == Some(OsStr::new("gz")) {
            let stream = storage.get_slice_stream(file_name, 0..file_size).await?;
            DocFileReader::new(
                Some(partition_id),
                Box::new(GzipDecoder::new(BufReader::new(stream))),
                offset,
                offset,
            )
        } else {
            let stream = storage
                .get_slice_stream(file_name, offset..file_size)
                .await?;
            DocFileReader::new(Some(partition_id), stream, 0, offset)
        };
        Ok(reader)
    }

    pub fn from_stdin() -> Self {
        DocFileReader::new(None, Box::new(tokio::io::stdin()), 0, 0)
    }

    fn new(
        partition_id: Option<PartitionId>,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        num_bytes_to_skip: usize,
        offset: usize,
    ) -> Self {
        Self {
            reader: SkipReader::new(reader, num_bytes_to_skip),
            counters: DocFileCounters {
                num_lines_processed: 0,
                current_offset: offset as u64,
                previous_offset: offset as u64,
            },
            partition_id,
        }
    }

    // Return true if the end of the file has been reached.
    pub async fn read_batch(
        &mut self,
        ctx: &SourceContext,
    ) -> anyhow::Result<(Option<RawDocBatch>, bool)> {
        // We collect batches of documents before sending them to the indexer.
        let limit_num_bytes = self.counters.previous_offset + BATCH_NUM_BYTES_LIMIT;
        let mut reached_eof = false;
        let mut batch_builder = BatchBuilder::new(SourceType::File);

        while self.counters.current_offset < limit_num_bytes {
            let mut doc_line = String::new();
            // guard the zone in case of slow read, such as reading from someone
            // typing to stdin
            let num_bytes = ctx
                .protect_future(self.reader.read_line(&mut doc_line))
                .await
                .map_err(anyhow::Error::from)?;
            if num_bytes == 0 {
                reached_eof = true;
                break;
            }
            batch_builder.add_doc(Bytes::from(doc_line));
            self.counters.current_offset += num_bytes as u64;
            self.counters.num_lines_processed += 1;
        }
        if !batch_builder.docs.is_empty() {
            if let Some(partition_id) = &self.partition_id {
                batch_builder
                    .checkpoint_delta
                    .record_partition_delta(
                        partition_id.clone(),
                        Position::offset(self.counters.previous_offset),
                        Position::offset(self.counters.current_offset),
                    )
                    .unwrap();
            }
            self.counters.previous_offset = self.counters.current_offset;
            Ok((Some(batch_builder.build()), reached_eof))
        } else {
            Ok((None, reached_eof))
        }
    }

    pub fn counters(&self) -> &DocFileCounters {
        &self.counters
    }
}

pub(crate) fn dir_and_filename(filepath: &Path) -> anyhow::Result<(Uri, &Path)> {
    let dir_uri: Uri = filepath
        .parent()
        .context("Parent directory could not be resolved")?
        .to_str()
        .context("Path cannot be turned to string")?
        .parse()?;
    let file_name = filepath
        .file_name()
        .context("Path does not appear to be a file")?;
    Ok((dir_uri, file_name.as_ref()))
}

#[cfg(any(test))]
pub mod file_test_helpers {
    use std::io::Write;

    use async_compression::tokio::write::GzipEncoder;
    use tempfile::NamedTempFile;

    pub const DUMMY_DOC: &[u8] = r#"{"body": "hello happy tax payer!"}"#.as_bytes();

    async fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
        let mut gzip_documents = Vec::new();
        let mut encoder = GzipEncoder::new(&mut gzip_documents);
        tokio::io::AsyncWriteExt::write_all(&mut encoder, bytes)
            .await
            .unwrap();
        // flush is not sufficient here and reading the file will raise a unexpected end of file
        // error.
        tokio::io::AsyncWriteExt::shutdown(&mut encoder)
            .await
            .unwrap();
        gzip_documents
    }

    async fn write_to_tmp(data: Vec<u8>, gzip: bool) -> NamedTempFile {
        let mut temp_file: tempfile::NamedTempFile = if gzip {
            tempfile::Builder::new().suffix(".gz").tempfile().unwrap()
        } else {
            tempfile::NamedTempFile::new().unwrap()
        };
        if gzip {
            let gzip_documents = gzip_bytes(&data).await;
            temp_file.write_all(&gzip_documents).unwrap();
        } else {
            temp_file.write_all(&data).unwrap();
        }
        temp_file.flush().unwrap();
        temp_file
    }

    pub async fn generate_dummy_doc_file(gzip: bool, lines: usize) -> NamedTempFile {
        let mut documents_bytes = Vec::with_capacity(DUMMY_DOC.len() * lines);
        for _ in 0..lines {
            documents_bytes.write_all(DUMMY_DOC).unwrap();
            documents_bytes.write_all("\n".as_bytes()).unwrap();
        }
        write_to_tmp(documents_bytes, gzip).await
    }

    pub async fn generate_index_doc_file(gzip: bool, lines: usize) -> NamedTempFile {
        let mut documents_bytes = Vec::new();
        for i in 0..lines {
            documents_bytes
                .write_all(format!("{i}\n").as_bytes())
                .unwrap();
        }
        write_to_tmp(documents_bytes, gzip).await
    }

    // let temp_file_path = temp_file.path().canonicalize().unwrap();
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use file_test_helpers::{generate_dummy_doc_file, DUMMY_DOC};
    use quickwit_actors::{ActorContext, Universe};
    use serde_json::json;
    use tokio::sync::watch;

    use super::*;

    #[tokio::test]
    async fn test_skip_reader() {
        {
            // Skip 0 bytes.
            let mut reader = SkipReader::new(Box::new("hello".as_bytes()), 0);
            let mut buf = String::new();
            reader.read_line(&mut buf).await.unwrap();
            assert_eq!(buf, "hello");
        }
        {
            // Skip 2 bytes.
            let mut reader = SkipReader::new(Box::new("hello".as_bytes()), 2);
            let mut buf = String::new();
            reader.read_line(&mut buf).await.unwrap();
            assert_eq!(buf, "llo");
        }
        {
            let input = "hello";
            let cursor = Cursor::new(input);
            let mut reader = SkipReader::new(Box::new(cursor), 5);
            let mut buf = String::new();
            assert!(reader.read_line(&mut buf).await.is_ok());
        }
        {
            let input = "hello";
            let cursor = Cursor::new(input);
            let mut reader = SkipReader::new(Box::new(cursor), 10);
            let mut buf = String::new();
            assert!(reader.read_line(&mut buf).await.is_err());
        }
        {
            let input = "hello world".repeat(10000);
            let cursor = Cursor::new(input.clone());
            let mut reader = SkipReader::new(Box::new(cursor), 64000);
            let mut buf = String::new();
            reader.read_line(&mut buf).await.unwrap();
            assert_eq!(buf, input[64000..]);
        }
        {
            let input = "hello world".repeat(10000);
            let cursor = Cursor::new(input.clone());
            let mut reader = SkipReader::new(Box::new(cursor), 64001);
            let mut buf = String::new();
            reader.read_line(&mut buf).await.unwrap();
            assert_eq!(buf, input[64001..]);
        }
    }

    fn setup_test_source_ctx() -> SourceContext {
        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        ActorContext::for_test(&universe, source_mailbox, observable_state_tx)
    }

    async fn aux_test_batch_reader(
        file: impl Into<PathBuf>,
        expected_lines: usize,
        expected_batches: usize,
    ) {
        let checkpoint = SourceCheckpoint::default();
        let storage_resolver = StorageResolver::for_test();
        let file_path = file.into();
        let mut doc_batch_reader =
            DocFileReader::from_path(&checkpoint, &file_path, &storage_resolver)
                .await
                .unwrap();
        let ctx = setup_test_source_ctx();
        let mut parsed_lines = 0;
        let mut parsed_batches = 0;
        loop {
            let (batch_opt, is_eof) = doc_batch_reader.read_batch(&ctx).await.unwrap();
            if let Some(batch) = batch_opt {
                parsed_lines += batch.docs.len();
                parsed_batches += 1;
                assert!(parsed_lines > 0);
                assert!(parsed_lines <= expected_lines);
            } else {
                assert!(is_eof);
                break;
            }
        }
        assert_eq!(parsed_lines, expected_lines);
        assert_eq!(parsed_batches, expected_batches);
    }

    #[tokio::test]
    async fn test_batch_reader_small() {
        aux_test_batch_reader("data/test_corpus.json", 4, 1).await;
    }

    #[tokio::test]
    async fn test_batch_reader_small_gz() {
        aux_test_batch_reader("data/test_corpus.json.gz", 4, 1).await;
    }

    #[tokio::test]
    async fn test_read_multiple_batches() {
        let lines = BATCH_NUM_BYTES_LIMIT as usize / DUMMY_DOC.len() + 10;
        let dummy_doc_file = generate_dummy_doc_file(false, lines).await;
        aux_test_batch_reader(dummy_doc_file.path(), lines, 2).await;
    }

    #[tokio::test]
    async fn test_read_multiple_batches_gz() {
        let lines = BATCH_NUM_BYTES_LIMIT as usize / DUMMY_DOC.len() + 10;
        let dummy_doc_file_gz = generate_dummy_doc_file(true, lines).await;
        aux_test_batch_reader(dummy_doc_file_gz.path(), lines, 2).await;
    }
}
