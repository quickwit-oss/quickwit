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

use std::io::{Read, Write};
use std::ops::Range;

/// Exact cache key for a split object URI and a half-open byte range.
///
/// The on-disk layout is little-endian `u64` fields:
/// `[uri_len][uri_bytes][range_start][range_end]`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SplitRangeCacheKey {
    pub object_uri: String,
    pub byte_range: Range<usize>,
}

const U64_LEN: usize = 8;

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

/// Manual `Code` impl: keep Foyer's serde feature off so bincode is not pulled
/// in for keys or values. Foyer 0.22.3 already implements `Code` for `Bytes`,
/// so only this key needs a codec. Enabling serde would encode each value byte
/// as an integer; for a 15 MiB payload that is millions of serializer visits
/// instead of one `write_all`.
impl foyer::Code for SplitRangeCacheKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let uri_bytes = self.object_uri.as_bytes();
        write_u64(writer, uri_bytes.len() as u64)?;
        writer
            .write_all(uri_bytes)
            .map_err(foyer::Error::io_error)?;
        write_u64(writer, self.byte_range.start as u64)?;
        write_u64(writer, self.byte_range.end as u64)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let uri_len = read_u64(reader)? as usize;
        let mut uri_bytes = vec![0; uri_len];
        reader
            .read_exact(&mut uri_bytes)
            .map_err(foyer::Error::io_error)?;
        let object_uri = String::from_utf8(uri_bytes).map_err(|error| {
            foyer::Error::new(foyer::ErrorKind::Parse, "object URI is not UTF-8").with_source(error)
        })?;
        let start = read_u64(reader)? as usize;
        let end = read_u64(reader)? as usize;
        Ok(Self {
            object_uri,
            byte_range: start..end,
        })
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
        let expected = SplitRangeCacheKey {
            object_uri: "s3://bucket/prefix/a.split".to_string(),
            byte_range: 10..42,
        };
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
        let expected = SplitRangeCacheKey {
            object_uri: String::new(),
            byte_range: 0..0,
        };
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
        let key = SplitRangeCacheKey {
            object_uri: "s3://bucket/a.split".to_string(),
            byte_range: 1..2,
        };
        let mut encoded = Vec::new();
        key.encode(&mut encoded).unwrap();
        encoded.pop();
        let error = SplitRangeCacheKey::decode(&mut encoded.as_slice()).unwrap_err();
        assert_eq!(error.kind(), foyer::ErrorKind::Io);
    }
}
