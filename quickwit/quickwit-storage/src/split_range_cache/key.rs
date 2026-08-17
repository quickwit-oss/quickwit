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

#![allow(dead_code)]

use std::io::{Read, Write};
use std::ops::Range;

/// Exact cache key for a split object URI and a half-open byte range.
///
/// The on-disk layout is portable little-endian `u64` fields so recovered
/// entries stay valid across 32-bit and 64-bit processes:
/// `[uri_len][uri_bytes][range_start][range_end]`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SplitRangeCacheKey {
    pub object_uri: String,
    pub byte_range: Range<usize>,
}

impl SplitRangeCacheKey {
    pub(crate) fn new(object_uri: String, byte_range: Range<usize>) -> Self {
        debug_assert!(
            byte_range.start <= byte_range.end,
            "split range cache keys use half-open ranges"
        );
        Self {
            object_uri,
            byte_range,
        }
    }
}

const U64_LEN: usize = 8;
/// Object URIs come from `Storage::uri().join(path)`. S3 object keys max out
/// at 1024 bytes; 8 KiB leaves room for scheme, bucket, and prefix.
const MAX_OBJECT_URI_LEN: usize = 8 * 1024;

fn ensure_uri_len_in_range(uri_len: usize) -> foyer::Result<()> {
    if uri_len > MAX_OBJECT_URI_LEN {
        return Err(foyer::Error::new(
            foyer::ErrorKind::Parse,
            "object URI is too long",
        ));
    }
    Ok(())
}

fn write_u64(writer: &mut impl Write, value: u64) -> foyer::Result<()> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(foyer::Error::io_error)
}

fn read_u64(reader: &mut impl Read) -> foyer::Result<u64> {
    let mut buf = [0u8; U64_LEN];
    reader
        .read_exact(&mut buf)
        .map_err(foyer::Error::io_error)?;
    Ok(u64::from_le_bytes(buf))
}

fn u64_from_usize(value: usize, message: &'static str) -> foyer::Result<u64> {
    u64::try_from(value)
        .map_err(|error| foyer::Error::new(foyer::ErrorKind::Parse, message).with_source(error))
}

fn usize_from_u64(value: u64, message: &'static str) -> foyer::Result<usize> {
    usize::try_from(value)
        .map_err(|error| foyer::Error::new(foyer::ErrorKind::Parse, message).with_source(error))
}

/// Manual `Code` impl: keep Foyer's serde feature off so bincode is not pulled
/// in for keys or values. Foyer 0.22.3 already implements `Code` for `Bytes`,
/// so only this key needs a codec. Enabling serde would encode each value byte
/// as an integer; for a 15 MiB payload that is millions of serializer visits
/// instead of one `write_all`.
impl foyer::Code for SplitRangeCacheKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let uri_bytes = self.object_uri.as_bytes();
        ensure_uri_len_in_range(uri_bytes.len())?;
        let uri_len = u64_from_usize(uri_bytes.len(), "object URI is too long")?;
        write_u64(writer, uri_len)?;
        writer
            .write_all(uri_bytes)
            .map_err(foyer::Error::io_error)?;
        let start = u64_from_usize(self.byte_range.start, "range start is too large")?;
        let end = u64_from_usize(self.byte_range.end, "range end is too large")?;
        write_u64(writer, start)?;
        write_u64(writer, end)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let uri_len = usize_from_u64(read_u64(reader)?, "encoded object URI is too long")?;
        ensure_uri_len_in_range(uri_len)?;
        let mut uri_bytes = vec![0; uri_len];
        reader
            .read_exact(&mut uri_bytes)
            .map_err(foyer::Error::io_error)?;
        let object_uri = String::from_utf8(uri_bytes).map_err(|error| {
            foyer::Error::new(foyer::ErrorKind::Parse, "object URI is not UTF-8").with_source(error)
        })?;
        let start = usize_from_u64(read_u64(reader)?, "range start does not fit usize")?;
        let end = usize_from_u64(read_u64(reader)?, "range end does not fit usize")?;
        if start > end {
            return Err(foyer::Error::new(
                foyer::ErrorKind::Parse,
                "range start is greater than range end",
            ));
        }
        Ok(Self::new(object_uri, start..end))
    }

    fn estimated_size(&self) -> usize {
        U64_LEN + self.object_uri.len() + U64_LEN + U64_LEN
    }
}

#[cfg(test)]
mod tests {
    use foyer::Code;

    use super::*;

    #[test]
    fn test_split_range_cache_key_codec_round_trip() {
        let expected = SplitRangeCacheKey::new("s3://bucket/prefix/a.split".to_string(), 10..42);
        let mut encoded = Vec::new();
        expected.encode(&mut encoded).unwrap();
        assert_eq!(
            SplitRangeCacheKey::decode(&mut encoded.as_slice()).unwrap(),
            expected
        );
        assert_eq!(expected.estimated_size(), encoded.len());
    }

    #[test]
    fn test_split_range_cache_key_codec_empty_uri() {
        let expected = SplitRangeCacheKey::new(String::new(), 0..0);
        let mut encoded = Vec::new();
        expected.encode(&mut encoded).unwrap();
        assert_eq!(
            SplitRangeCacheKey::decode(&mut encoded.as_slice()).unwrap(),
            expected
        );
        assert_eq!(encoded.len(), 24);
    }

    #[test]
    fn test_split_range_cache_key_codec_rejects_truncated_buffer() {
        let key = SplitRangeCacheKey::new("s3://bucket/a.split".to_string(), 1..2);
        let mut encoded = Vec::new();
        key.encode(&mut encoded).unwrap();
        encoded.pop();
        let error = SplitRangeCacheKey::decode(&mut encoded.as_slice()).unwrap_err();
        assert_eq!(error.kind(), foyer::ErrorKind::Io);
    }

    #[test]
    fn test_split_range_cache_key_codec_rejects_inverted_range() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&0u64.to_le_bytes());
        encoded.extend_from_slice(&10u64.to_le_bytes());
        encoded.extend_from_slice(&4u64.to_le_bytes());
        let error = SplitRangeCacheKey::decode(&mut encoded.as_slice()).unwrap_err();
        assert_eq!(error.kind(), foyer::ErrorKind::Parse);
        assert!(
            error
                .message()
                .contains("range start is greater than range end")
        );
    }

    #[test]
    fn test_split_range_cache_key_codec_rejects_oversized_uri_len() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&((MAX_OBJECT_URI_LEN as u64) + 1).to_le_bytes());
        let error = SplitRangeCacheKey::decode(&mut encoded.as_slice()).unwrap_err();
        assert_eq!(error.kind(), foyer::ErrorKind::Parse);
        assert!(error.message().contains("object URI is too long"));
    }

    #[test]
    fn test_split_range_cache_key_codec_rejects_oversized_uri_on_encode() {
        let key = SplitRangeCacheKey::new("a".repeat(MAX_OBJECT_URI_LEN + 1), 0..1);
        let error = key.encode(&mut Vec::new()).unwrap_err();
        assert_eq!(error.kind(), foyer::ErrorKind::Parse);
        assert!(error.message().contains("object URI is too long"));
    }

    #[test]
    fn test_split_range_cache_key_codec_accepts_max_uri_len() {
        let expected = SplitRangeCacheKey::new("a".repeat(MAX_OBJECT_URI_LEN), 0..1);
        let mut encoded = Vec::new();
        expected.encode(&mut encoded).unwrap();
        assert_eq!(
            SplitRangeCacheKey::decode(&mut encoded.as_slice()).unwrap(),
            expected
        );
    }
}
