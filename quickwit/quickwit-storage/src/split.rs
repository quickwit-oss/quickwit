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

use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use aws_sdk_s3::primitives::{ByteStream, FsBuilder, Length, SdkBody};
use futures::{Stream, StreamExt, stream};
use hyper::body::{Bytes, Frame};
use pin_project::pin_project;
use quickwit_common::shared_consts::SPLIT_FIELDS_FILE_NAME;

use crate::bundle_storage::BundleStorageFileOffsetsVersions;
use crate::{BundleStorageFileOffsets, PutPayload, VersionedComponent};

/// Payload of a split which builds the split bundle and hotcache on the fly and streams it to the
/// storage.
#[derive(Clone)]
pub struct SplitPayload {
    payloads: Vec<Box<dyn PutPayload>>,
    /// bytes range of the footer (hotcache + bundle metadata)
    pub footer_range: Range<u64>,
}

async fn range_byte_stream_from_payloads(
    payloads: &[Box<dyn PutPayload>],
    range: Range<u64>,
) -> io::Result<ByteStream> {
    let mut bytestreams: Vec<ByteStream> = Vec::new();

    let payloads_and_ranges =
        chunk_payload_ranges(payloads, range.start as usize..range.end as usize);

    for (payload, range) in payloads_and_ranges {
        bytestreams.push(
            payload
                .range_byte_stream(range.start as u64..range.end as u64)
                .await?,
        );
    }

    let body = stream::iter(bytestreams)
        .map(StreamAdaptor)
        .flatten()
        .map(|result| result.map(Frame::data));
    let stream_body = http_body_util::StreamBody::new(body);
    let concat_stream = ByteStream::new(SdkBody::from_body_1_x(stream_body));
    Ok(concat_stream)
}

// With sdk 1.0, ByteStream no longer implement Stream, despite having analogous functions
// this adaptor is just meant to make it implement Stream for places where we really need it
#[pin_project]
struct StreamAdaptor(#[pin] ByteStream);

impl Stream for StreamAdaptor {
    type Item = Result<Bytes, aws_smithy_types::byte_stream::error::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().0.poll_next(ctx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower_bound_u64, upper_bound_u64) = self.0.size_hint();
        // if conversion fails, it means lower_bound is too large to fit in an usize on this
        // platform. When that's the case, we return usize::MAX as best effort. Any value is valid,
        // but MAX is the most informative.
        let lower_bound = lower_bound_u64.try_into().unwrap_or(usize::MAX);
        // for the upperbound, if conversion fails, we just say the upper bound is unknown
        let upper_bound =
            upper_bound_u64.and_then(|upper_bound_u64| upper_bound_u64.try_into().ok());
        (lower_bound, upper_bound)
    }
}

#[async_trait]
impl PutPayload for SplitPayload {
    fn len(&self) -> u64 {
        self.payloads.iter().map(|payload| payload.len()).sum()
    }

    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream> {
        range_byte_stream_from_payloads(&self.payloads, range).await
    }
}

#[derive(Clone)]
struct FilePayload {
    len: u64,
    path: PathBuf,
}

#[async_trait]
impl PutPayload for FilePayload {
    fn len(&self) -> u64 {
        self.len
    }

    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream> {
        assert!(!range.is_empty());
        assert!(range.end <= self.len);

        let len = range.end - range.start;
        let mut fs_builder = FsBuilder::new().path(&self.path);

        if range.start > 0 {
            fs_builder = fs_builder.offset(range.start);
        }
        fs_builder = fs_builder.length(Length::Exact(len));

        fs_builder
            .build()
            .await
            .map_err(|error| io::Error::other(format!("failed to create byte stream: {error}")))
    }
}

/// SplitPayloadBuilder is used to create a `SplitPayload`.
#[derive(Default)]
pub struct SplitPayloadBuilder {
    /// File name, payload, and range of the payload in the bundle file
    /// Range could be computed on the fly, and is just kept here for convenience.
    payloads: Vec<(String, Box<dyn PutPayload>, Range<u64>)>,
    current_offset: usize,
}

impl SplitPayloadBuilder {
    /// Creates a new SplitPayloadBuilder for given files and hotcache.
    pub fn get_split_payload(
        split_files: &[PathBuf],
        serialized_split_fields: &[u8],
        hotcache: &[u8],
    ) -> anyhow::Result<SplitPayload> {
        let mut split_payload_builder = SplitPayloadBuilder::default();
        for file in split_files {
            split_payload_builder.add_file(file)?;
        }
        split_payload_builder.add_payload(
            SPLIT_FIELDS_FILE_NAME.to_string(),
            Box::new(serialized_split_fields.to_vec()),
        );
        let offsets = split_payload_builder.finalize(hotcache)?;
        Ok(offsets)
    }

    /// Adds the payload to the bundle file.
    pub fn add_payload(&mut self, file_name: String, payload: Box<dyn PutPayload>) {
        let range = self.current_offset as u64..self.current_offset as u64 + payload.len();
        self.current_offset += payload.len() as usize;
        self.payloads.push((file_name, payload, range));
    }

    /// Adds the file to the bundle file.
    pub fn add_file(&mut self, path: &Path) -> io::Result<()> {
        let file = std::fs::metadata(path)?;
        let file_name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid file name in path {path:?}"),
                )
            })?;

        let file_payload = FilePayload {
            path: path.to_owned(),
            len: file.len(),
        };

        self.add_payload(file_name, Box::new(file_payload));

        Ok(())
    }

    /// Writes the bundle file offsets metadata at the end of the bundle file,
    /// and returns the byte-range of this metadata information.
    pub fn finalize(self, hotcache: &[u8]) -> anyhow::Result<SplitPayload> {
        // Add the fields metadata to the bundle metadata.
        // Build the footer.
        let metadata_with_fixed_paths = self
            .payloads
            .iter()
            .map(|(file_name, _, range)| {
                let file_name = PathBuf::from(file_name);
                Ok((file_name, range.start..range.end))
            })
            .collect::<Result<HashMap<_, _>, anyhow::Error>>()?;

        let bundle_storage_file_offsets = BundleStorageFileOffsets {
            files: metadata_with_fixed_paths,
        };
        let metadata_json =
            BundleStorageFileOffsetsVersions::serialize(&bundle_storage_file_offsets);

        // The hotcache needs to be the next to the metadata in order to be able to read both
        // in one continuous read.
        let mut footer_bytes = Vec::new();
        footer_bytes.extend(&metadata_json);
        footer_bytes.extend((metadata_json.len() as u32).to_le_bytes());
        footer_bytes.extend(hotcache);
        footer_bytes.extend((hotcache.len() as u32).to_le_bytes());

        let mut payloads: Vec<Box<dyn PutPayload>> = self
            .payloads
            .into_iter()
            .map(|(_, payload, _)| payload)
            .collect();

        payloads.push(Box::new(footer_bytes.to_vec()));

        Ok(SplitPayload {
            payloads,
            footer_range: self.current_offset as u64
                ..self.current_offset as u64 + footer_bytes.len() as u64,
        })
    }
}

/// Returns the payloads with their absolute ranges.
fn get_payloads_with_absolute_range(
    payloads: &[Box<dyn PutPayload>],
) -> Vec<(Box<dyn PutPayload>, Range<usize>)> {
    let mut current = 0;
    payloads
        .iter()
        .map(|payload| {
            let start = current;
            current += payload.len();
            (payload.clone(), start as usize..current as usize)
        })
        .collect()
}

fn get_ranges_overlap(range1: &Range<usize>, range2: &Range<usize>) -> Range<usize> {
    range1.start.max(range2.start)..range1.end.min(range2.end)
}

// Returns payloads and their relative ranges for an absolute range.
fn chunk_payload_ranges(
    payloads: &[Box<dyn PutPayload>],
    range: Range<usize>,
) -> Vec<(Box<dyn PutPayload>, Range<usize>)> {
    let mut ranges = Vec::new();
    for (payload, payload_absolute_range) in get_payloads_with_absolute_range(payloads) {
        let absolute_range_overlap = get_ranges_overlap(&payload_absolute_range, &range);
        if !absolute_range_overlap.is_empty() {
            // Push the range relative to this payload as we will read from it.
            ranges.push((
                payload.clone(),
                (absolute_range_overlap.start - payload_absolute_range.start)
                    ..(absolute_range_overlap.end - payload_absolute_range.start),
            ));
        }
    }
    ranges
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use super::*;

    #[tokio::test]
    async fn test_split_offset_computer() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(b"hello")?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(b"world")?;

        let split_payload =
            SplitPayloadBuilder::get_split_payload(&[test_filepath1, test_filepath2], &[], b"abc")?;

        assert_eq!(split_payload.len(), 128);

        Ok(())
    }

    #[cfg(test)]
    async fn fetch_data(
        split_streamer: &SplitPayload,
        range: Range<u64>,
    ) -> anyhow::Result<Vec<u8>> {
        use tokio::io::AsyncReadExt as _;

        let mut data = Vec::new();
        split_streamer
            .range_byte_stream(range)
            .await?
            .into_async_read()
            .read_to_end(&mut data)
            .await?;
        Ok(data)
    }

    #[test]
    fn test_chunk_payloads() -> anyhow::Result<()> {
        let payloads: Vec<Box<dyn PutPayload>> = vec![
            Box::new(vec![1, 2, 3]),
            Box::new(vec![4, 5, 6]),
            Box::new(vec![7, 8, 9, 10]),
        ];

        assert_eq!(
            chunk_payload_ranges(&payloads, 0..1)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..1]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 0..2)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..2]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 1..2)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![1..2]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 2..3)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 0..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..3, 0..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 0..5)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..3, 0..2]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 3..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 4..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![1..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 5..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 2..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3, 0..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 2..5)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3, 0..2]
        );

        assert_eq!(
            chunk_payload_ranges(&payloads, 7..8)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![1..2]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_split_streamer() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("a");
        let test_filepath2 = temp_dir.path().join("b");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[],
            &[1, 2, 3],
        )?;

        // border case 1 exact start of first block
        assert_eq!(fetch_data(&split_streamer, 0..1).await?, vec![123]);
        assert_eq!(fetch_data(&split_streamer, 0..2).await?, vec![123, 76]);
        assert_eq!(fetch_data(&split_streamer, 0..3).await?, vec![123, 76, 99]);

        // border 2 case skip and take cross adjacent blocks
        assert_eq!(fetch_data(&split_streamer, 1..3).await?, vec![76, 99]);

        // border 3 case skip and take in separate blocks with full block between
        assert_eq!(
            fetch_data(&split_streamer, 1..6).await?,
            vec![76, 99, 55, 44, 174]
        );

        // border case 4 exact middle block
        assert_eq!(fetch_data(&split_streamer, 2..5).await?, vec![99, 55, 44]);

        // border case 5, no skip but take in middle block
        assert_eq!(fetch_data(&split_streamer, 2..4).await?, vec![99, 55]);

        // border case 6 skip and take in middle block
        assert_eq!(fetch_data(&split_streamer, 3..4).await?, vec![55]);

        // border case 7 start exact last block - footer
        assert_eq!(
            fetch_data(&split_streamer, 5..10).await?,
            vec![174, 190, 18, 24, 1]
        );
        // border case 8 skip and take in last block  - footer
        assert_eq!(
            fetch_data(&split_streamer, 6..10).await?,
            vec![190, 18, 24, 1]
        );

        let total_len = split_streamer.len();
        let all_data = fetch_data(&split_streamer, 0..total_len).await?;

        // last 8 bytes are the length of the hotcache bytes
        assert_eq!(all_data[all_data.len() - 4..], 3_u32.to_le_bytes());
        Ok(())
    }
}
