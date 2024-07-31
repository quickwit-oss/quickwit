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

use std::io;
use std::path::Path;

use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use quickwit_common::uri::Uri;
use quickwit_storage::StorageResolver;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

pub struct FileRecord {
    pub next_offset: u64,
    pub doc: Bytes,
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
        // Allocating 64KB once on the stack should be fine (<1% of the Linux stack size)
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
    next_offset: u64,
}

impl DocFileReader {
    pub async fn from_uri(
        storage_resolver: &StorageResolver,
        uri: &Uri,
        offset: usize,
    ) -> anyhow::Result<Self> {
        let (dir_uri, file_name) = dir_and_filename(uri)?;
        let storage = storage_resolver.resolve(&dir_uri).await?;
        let file_size = storage.file_num_bytes(file_name).await?.try_into().unwrap();
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

    pub fn from_stdin() -> Self {
        DocFileReader {
            reader: SkipReader::new(Box::new(tokio::io::stdin()), 0),
            next_offset: 0,
        }
    }

    /// Reads the next record from the underlying file. Returns `None` when EOF
    /// is reached.
    pub async fn next_record(&mut self) -> anyhow::Result<Option<FileRecord>> {
        let mut buf = String::new();
        let res = self.reader.read_line(&mut buf).await?;
        if res == 0 {
            Ok(None)
        } else {
            self.next_offset += res as u64;
            Ok(Some(FileRecord {
                next_offset: self.next_offset,
                doc: Bytes::from(buf),
            }))
        }
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
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::str::FromStr;

    use file_test_helpers::generate_index_doc_file;

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

    async fn aux_test_full_read(file: impl AsRef<str>, expected_lines: usize) {
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
    async fn test_full_read() {
        aux_test_full_read("data/test_corpus.json", 4).await;
    }

    #[tokio::test]
    async fn test_full_read_gz() {
        aux_test_full_read("data/test_corpus.json.gz", 4).await;
    }

    async fn aux_test_resumed_read(
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
            assert_eq!(Bytes::from(format!("{parsed_lines}\n")), rec.doc);
            parsed_lines += 1;
        }
        // read the second part of the file
        let mut second_part_reader =
            DocFileReader::from_uri(&storage_resolver, &uri, resume_offset)
                .await
                .unwrap();
        while let Some(rec) = second_part_reader.next_record().await.unwrap() {
            assert_eq!(Bytes::from(format!("{parsed_lines}\n")), rec.doc);
            parsed_lines += 1;
        }
        assert_eq!(parsed_lines, expected_lines);
    }

    #[tokio::test]
    async fn test_resume_read() {
        let dummy_doc_file = generate_index_doc_file(false, 1000).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 1).await;
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 40).await;
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 999).await;
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 1000).await;
    }

    #[tokio::test]
    async fn test_resume_read_gz() {
        let dummy_doc_file = generate_index_doc_file(true, 1000).await;
        let dummy_doc_file_uri = dummy_doc_file.path().to_str().unwrap();
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 1).await;
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 40).await;
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 999).await;
        aux_test_resumed_read(dummy_doc_file_uri, 1000, 1000).await;
    }
}
