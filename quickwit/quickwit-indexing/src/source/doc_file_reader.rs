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

use std::io;
use std::path::Path;

use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use quickwit_common::Progress;
use quickwit_common::uri::Uri;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::Position;
use quickwit_storage::StorageResolver;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use super::{BATCH_NUM_BYTES_LIMIT, BatchBuilder};

pub struct FileRecord {
    pub next_offset: u64,
    pub doc: Bytes,
    pub is_last: bool,
}

/// A helper wrapper that lets you skip bytes in compressed files where you
/// cannot seek (e.g. gzip files).
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

    async fn skip(&mut self) -> io::Result<()> {
        // allocate on the heap to avoid stack overflows
        let mut buf = vec![0u8; 64_000];
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

    /// Reads a line and peeks into the readers buffer. Returns the number of
    /// bytes read and true the end of the file is reached.
    async fn read_line_and_peek(&mut self, buf: &mut String) -> io::Result<(usize, bool)> {
        if self.num_bytes_to_skip > 0 {
            self.skip().await?;
        }
        let line_size = self.reader.read_line(buf).await?;
        if line_size == 0 {
            return Ok((0, true));
        }
        let next_bytes = self.reader.fill_buf().await?;
        Ok((line_size, next_bytes.is_empty()))
    }
}

pub struct DocFileReader {
    reader: SkipReader,
    next_offset: u64,
}

impl DocFileReader {
    pub fn empty() -> Self {
        DocFileReader {
            reader: SkipReader::new(Box::new(tokio::io::empty()), 0),
            next_offset: 0,
        }
    }

    pub async fn from_uri(
        storage_resolver: &StorageResolver,
        uri: &Uri,
        offset: usize,
    ) -> anyhow::Result<Self> {
        let (dir_uri, file_name) = dir_and_filename(uri)?;
        let storage = storage_resolver.resolve(&dir_uri).await?;
        let file_size = storage.file_num_bytes(file_name).await?.try_into().unwrap();
        if file_size == 0 {
            return Ok(DocFileReader::empty());
        }
        // If it's a gzip file, we can't seek to a specific offset. `SkipReader`
        // starts from the beginning of the file, decompresses and skips the
        // first `offset` bytes.
        let reader = if uri.extension() == Some("gz") {
            let stream = storage.get_slice_stream(file_name, 0..file_size).await?;
            let decompressed_stream = Box::new(GzipDecoder::new(BufReader::new(stream)));
            DocFileReader {
                reader: SkipReader::new(decompressed_stream, offset),
                next_offset: offset as u64,
            }
        } else {
            let stream = storage
                .get_slice_stream(file_name, offset..file_size)
                .await?;
            DocFileReader {
                reader: SkipReader::new(stream, 0),
                next_offset: offset as u64,
            }
        };
        Ok(reader)
    }

    /// Reads the next record from the underlying file. Returns `None` when EOF
    /// is reached.
    pub async fn next_record(&mut self) -> anyhow::Result<Option<FileRecord>> {
        let mut buf = String::new();
        // TODO retry if stream is broken (#5243)
        let (bytes_read, is_last) = self.reader.read_line_and_peek(&mut buf).await?;
        if bytes_read == 0 {
            Ok(None)
        } else {
            self.next_offset += bytes_read as u64;
            Ok(Some(FileRecord {
                next_offset: self.next_offset,
                doc: Bytes::from(buf),
                is_last,
            }))
        }
    }
}

pub struct ObjectUriBatchReader {
    partition_id: PartitionId,
    reader: DocFileReader,
    current_offset: usize,
    is_eof: bool,
}

impl ObjectUriBatchReader {
    pub async fn try_new(
        storage_resolver: &StorageResolver,
        partition_id: PartitionId,
        uri: &Uri,
        position: Position,
    ) -> anyhow::Result<Self> {
        let current_offset = match position {
            Position::Beginning => 0,
            Position::Offset(offset) => offset
                .as_usize()
                .context("file offset should be stored as usize")?,
            Position::Eof(_) => {
                return Ok(ObjectUriBatchReader {
                    partition_id,
                    reader: DocFileReader::empty(),
                    current_offset: 0,
                    is_eof: true,
                });
            }
        };
        let reader = DocFileReader::from_uri(storage_resolver, uri, current_offset).await?;
        Ok(ObjectUriBatchReader {
            partition_id,
            reader,
            current_offset,
            is_eof: false,
        })
    }

    pub async fn read_batch(
        &mut self,
        source_progress: &Progress,
        source_type: SourceType,
    ) -> anyhow::Result<BatchBuilder> {
        let mut batch_builder = BatchBuilder::new(source_type);
        if self.is_eof {
            return Ok(batch_builder);
        }
        let limit_num_bytes = self.current_offset + BATCH_NUM_BYTES_LIMIT as usize;
        let mut new_offset = self.current_offset;
        while new_offset < limit_num_bytes {
            if let Some(record) = source_progress
                .protect_future(self.reader.next_record())
                .await?
            {
                new_offset = record.next_offset as usize;
                batch_builder.add_doc(record.doc);
                if record.is_last {
                    self.is_eof = true;
                    break;
                }
            } else {
                self.is_eof = true;
                break;
            }
        }
        let to_position = if self.is_eof {
            Position::eof(new_offset)
        } else {
            Position::offset(new_offset)
        };
        batch_builder.checkpoint_delta.record_partition_delta(
            self.partition_id.clone(),
            Position::offset(self.current_offset),
            to_position,
        )?;
        self.current_offset = new_offset;
        Ok(batch_builder)
    }

    pub fn is_eof(&self) -> bool {
        self.is_eof
    }
}

pub(crate) fn dir_and_filename(filepath: &Uri) -> anyhow::Result<(Uri, &Path)> {
    let dir_uri: Uri = filepath
        .parent()
        .context("Parent directory could not be resolved")?;
    let file_name = filepath
        .file_name()
        .context("Path does not appear to be a file")?;
    Ok((dir_uri, file_name))
}

#[cfg(test)]
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

    pub async fn generate_dummy_doc_file(gzip: bool, lines: usize) -> (NamedTempFile, usize) {
        let mut documents_bytes = Vec::with_capacity(DUMMY_DOC.len() * lines);
        for _ in 0..lines {
            documents_bytes.write_all(DUMMY_DOC).unwrap();
            documents_bytes.write_all("\n".as_bytes()).unwrap();
        }
        let size = documents_bytes.len();
        let file = write_to_tmp(documents_bytes, gzip).await;
        (file, size)
    }

    /// Generates a file with increasing padded numbers. Each line is 8 bytes
    /// including the newline char.
    ///
    /// 0000000\n0000001\n0000002\n...
    pub async fn generate_index_doc_file(gzip: bool, lines: usize) -> NamedTempFile {
        assert!(lines < 9999999, "each line is 7 digits + newline");
        let mut documents_bytes = Vec::new();
        for i in 0..lines {
            documents_bytes
                .write_all(format!("{i:0>7}\n").as_bytes())
                .unwrap();
        }
        write_to_tmp(documents_bytes, gzip).await
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::str::FromStr;

    use file_test_helpers::generate_index_doc_file;
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;

    use super::*;

    #[tokio::test]
    async fn test_skip_reader() {
        {
            // Skip 0 bytes.
            let mut reader = SkipReader::new(Box::new("hello".as_bytes()), 0);
            let mut buf = String::new();
            let (bytes_read, eof) = reader.read_line_and_peek(&mut buf).await.unwrap();
            assert_eq!(buf, "hello");
            assert!(eof);
            assert_eq!(bytes_read, 5)
        }
        {
            // Skip 2 bytes.
            let mut reader = SkipReader::new(Box::new("hello".as_bytes()), 2);
            let mut buf = String::new();
            let (bytes_read, eof) = reader.read_line_and_peek(&mut buf).await.unwrap();
            assert_eq!(buf, "llo");
            assert!(eof);
            assert_eq!(bytes_read, 3)
        }
        {
            let input = "hello";
            let cursor = Cursor::new(input);
            let mut reader = SkipReader::new(Box::new(cursor), 5);
            let mut buf = String::new();
            let (bytes_read, eof) = reader.read_line_and_peek(&mut buf).await.unwrap();
            assert!(eof);
            assert_eq!(bytes_read, 0)
        }
        {
            let input = "hello";
            let cursor = Cursor::new(input);
            let mut reader = SkipReader::new(Box::new(cursor), 10);
            let mut buf = String::new();
            assert!(reader.read_line_and_peek(&mut buf).await.is_err());
        }
        {
            let input = "hello world".repeat(10000);
            let cursor = Cursor::new(input.clone());
            let mut reader = SkipReader::new(Box::new(cursor), 64000);
            let mut buf = String::new();
            reader.read_line_and_peek(&mut buf).await.unwrap();
            assert_eq!(buf, input[64000..]);
        }
        {
            let input = "hello world".repeat(10000);
            let cursor = Cursor::new(input.clone());
            let mut reader = SkipReader::new(Box::new(cursor), 64001);
            let mut buf = String::new();
            reader.read_line_and_peek(&mut buf).await.unwrap();
            assert_eq!(buf, input[64001..]);
        }
    }

    async fn aux_test_full_read_record(file: impl AsRef<str>, expected_lines: usize) {
        let storage_resolver = StorageResolver::for_test();
        let uri = Uri::from_str(file.as_ref()).unwrap();
        let mut doc_reader = DocFileReader::from_uri(&storage_resolver, &uri, 0)
            .await
            .unwrap();
        let mut parsed_lines = 0;
        while doc_reader.next_record().await.unwrap().is_some() {
            parsed_lines += 1;
        }
        assert_eq!(parsed_lines, expected_lines);
    }

    #[tokio::test]
    async fn test_full_read_record() {
        aux_test_full_read_record("data/test_corpus.json", 4).await;
    }

    #[tokio::test]
    async fn test_full_read_record_gz() {
        aux_test_full_read_record("data/test_corpus.json.gz", 4).await;
    }

    #[tokio::test]
    async fn test_empty_file() {
        let empty_file = tempfile::NamedTempFile::new().unwrap();
        let empty_file_uri = empty_file.path().to_str().unwrap();
        aux_test_full_read_record(empty_file_uri, 0).await;
    }

    async fn aux_test_resumed_read_record(
        file: impl AsRef<str>,
        expected_lines: usize,
        stop_at_line: usize,
    ) {
        let storage_resolver = StorageResolver::for_test();
        let uri = Uri::from_str(file.as_ref()).unwrap();
        // read the first part of the file
        let mut first_part_reader = DocFileReader::from_uri(&storage_resolver, &uri, 0)
            .await
            .unwrap();
        let mut resume_offset = 0;
        let mut parsed_lines = 0;
        for _ in 0..stop_at_line {
            let rec = first_part_reader
                .next_record()
                .await
                .unwrap()
                .expect("EOF happened before stop_at_line");
            resume_offset = rec.next_offset as usize;
            assert_eq!(Bytes::from(format!("{parsed_lines:0>7}\n")), rec.doc);
            parsed_lines += 1;
        }
        // read the second part of the file
        let mut second_part_reader =
            DocFileReader::from_uri(&storage_resolver, &uri, resume_offset)
                .await
                .unwrap();
        while let Some(rec) = second_part_reader.next_record().await.unwrap() {
            assert_eq!(Bytes::from(format!("{parsed_lines:0>7}\n")), rec.doc);
            parsed_lines += 1;
        }
        assert_eq!(parsed_lines, expected_lines);
    }

    #[tokio::test]
    async fn test_resumed_read_record() {
        let dummy_doc_file = generate_index_doc_file(false, 1000).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 1).await;
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 40).await;
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 999).await;
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 1000).await;
    }

    #[tokio::test]
    async fn test_resumed_read_record_gz() {
        let dummy_doc_file = generate_index_doc_file(true, 1000).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 1).await;
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 40).await;
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 999).await;
        aux_test_resumed_read_record(dummy_doc_file_uri, 1000, 1000).await;
    }

    async fn aux_test_full_read_batch(
        file: impl AsRef<str>,
        expected_lines: usize,
        expected_batches: usize,
        file_size: usize,
        from: Position,
    ) {
        let progress = Progress::default();
        let storage_resolver = StorageResolver::for_test();
        let uri = Uri::from_str(file.as_ref()).unwrap();
        let partition = PartitionId::from("test");
        let mut batch_reader =
            ObjectUriBatchReader::try_new(&storage_resolver, partition.clone(), &uri, from)
                .await
                .unwrap();

        let mut parsed_lines = 0;
        let mut parsed_batches = 0;
        let mut checkpoint_delta = SourceCheckpointDelta::default();
        while !batch_reader.is_eof() {
            let batch = batch_reader
                .read_batch(&progress, SourceType::Unspecified)
                .await
                .unwrap();
            parsed_lines += batch.docs.len();
            parsed_batches += 1;
            checkpoint_delta.extend(batch.checkpoint_delta).unwrap();
        }
        assert_eq!(parsed_lines, expected_lines);
        assert_eq!(parsed_batches, expected_batches);
        let position = checkpoint_delta
            .get_source_checkpoint()
            .position_for_partition(&partition)
            .unwrap()
            .clone();
        assert_eq!(position, Position::eof(file_size))
    }

    #[tokio::test]
    async fn test_read_batch_empty_file() {
        let empty_file = tempfile::NamedTempFile::new().unwrap();
        let empty_file_uri = empty_file.path().to_str().unwrap();
        aux_test_full_read_batch(empty_file_uri, 0, 1, 0, Position::Beginning).await;
    }

    #[tokio::test]
    async fn test_full_read_single_batch() {
        let num_lines = 10;
        let dummy_doc_file = generate_index_doc_file(false, num_lines).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_full_read_batch(
            dummy_doc_file_uri,
            num_lines,
            1,
            num_lines * 8,
            Position::Beginning,
        )
        .await;
    }

    #[tokio::test]
    async fn test_full_read_single_batch_max_size() {
        let num_lines = BATCH_NUM_BYTES_LIMIT as usize / 8;
        let dummy_doc_file = generate_index_doc_file(false, num_lines).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_full_read_batch(
            dummy_doc_file_uri,
            num_lines,
            1,
            num_lines * 8,
            Position::Beginning,
        )
        .await;
    }

    #[tokio::test]
    async fn test_full_read_two_batches() {
        let num_lines = BATCH_NUM_BYTES_LIMIT as usize / 8 + 10;
        let dummy_doc_file = generate_index_doc_file(false, num_lines).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_full_read_batch(
            dummy_doc_file_uri,
            num_lines,
            2,
            num_lines * 8,
            Position::Beginning,
        )
        .await;
    }

    #[tokio::test]
    async fn test_resume_read_batches() {
        let total_num_lines = BATCH_NUM_BYTES_LIMIT as usize / 8 * 3;
        let resume_after_lines = total_num_lines / 2;
        let dummy_doc_file = generate_index_doc_file(false, total_num_lines).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_full_read_batch(
            dummy_doc_file_uri,
            total_num_lines - resume_after_lines,
            2,
            total_num_lines * 8,
            Position::offset(resume_after_lines * 8),
        )
        .await;
    }
}
