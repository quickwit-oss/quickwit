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

//! On-disk format for the split fields metadata.
//!
//! Two formats are supported on read:
//! - Version 2 (legacy): single zstd-compressed prost-encoded `ListFields` blob. Forces a full
//!   download + decode just to look up a single field.
//! - Version 3 (current): a `tantivy_sstable::Dictionary`, where
//!     - key   = `field_name_bytes` followed by 1 byte holding the `ListFieldType` discriminant.
//!     - value = 1 byte bitpacking `(searchable: bit 0, aggregatable: bit 1)`.
//!
//!   The SSTable's own block index lives at the end of the payload, so a reader
//!   with byte-range access can locate and fetch only the blocks matching a
//!   field-name prefix instead of pulling the whole file.
//!
//! Two reader paths exist:
//! - `deserialize_split_fields`: eager full-scan from an in-memory byte stream. Used for the legacy
//!   v2 format and for any caller that already has the bytes in memory.
//! - `SplitFieldsReader::open` + `read_all_async`: async, byte-range-aware reader that opens the
//!   SSTable over a `FileHandle` and streams entries via async fetches. Currently still streams
//!   every entry; the next step is to take an `Automaton` (built from list-fields patterns) and
//!   fetch only the SSTable blocks that match it — see `Dictionary::search(automaton)` in
//!   `tantivy_sstable`.

use std::fmt;
use std::io::{self, Read};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use tantivy_common::file_slice::{FileHandle, FileSlice};
use tantivy_common::{HasLen, OwnedBytes};
use tantivy_sstable::value::{ValueReader, ValueWriter};
use tantivy_sstable::{Dictionary, SSTable};

use super::{ListFieldType, ListFields, ListFieldsEntryResponse};

const FORMAT_VERSION_V2_ZSTD_PROST: u8 = 2;
const FORMAT_VERSION_V3_SSTABLE: u8 = 3;

const SEARCHABLE_BIT: u8 = 0b01;
const AGGREGATABLE_BIT: u8 = 0b10;

/// SSTable type used to store the split fields.
///
/// Key encoding: `field_name_bytes` || `field_type_byte`.
/// Value encoding: `searchable << 0 | aggregatable << 1`.
#[derive(Debug)]
pub struct FieldMetadataSSTable;

impl SSTable for FieldMetadataSSTable {
    type Value = u8;
    type ValueReader = FieldMetadataValueReader;
    type ValueWriter = FieldMetadataValueWriter;
}

#[derive(Default)]
pub struct FieldMetadataValueWriter {
    vals: Vec<u8>,
}

impl ValueWriter for FieldMetadataValueWriter {
    type Value = u8;

    fn write(&mut self, val: &u8) {
        self.vals.push(*val);
    }

    fn serialize_block(&self, output: &mut Vec<u8>) {
        // Self-describing block: vint(count) || count flag bytes.
        // We piggy-back on tantivy_sstable's vint helper via a small inline copy
        // to avoid leaking that internal module.
        let mut len_buf = [0u8; 10];
        let len_bytes = encode_vint_u64(self.vals.len() as u64, &mut len_buf);
        output.extend_from_slice(&len_buf[..len_bytes]);
        output.extend_from_slice(&self.vals);
    }

    fn clear(&mut self) {
        self.vals.clear();
    }
}

#[derive(Default)]
pub struct FieldMetadataValueReader {
    vals: Vec<u8>,
}

impl ValueReader for FieldMetadataValueReader {
    type Value = u8;

    #[inline(always)]
    fn value(&self, idx: usize) -> &u8 {
        &self.vals[idx]
    }

    fn load(&mut self, data: &[u8]) -> io::Result<usize> {
        self.vals.clear();
        let (len, len_bytes) = decode_vint_u64(data)?;
        let payload_end = len_bytes + len as usize;
        if data.len() < payload_end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "split fields value block truncated",
            ));
        }
        self.vals.extend_from_slice(&data[len_bytes..payload_end]);
        Ok(payload_end)
    }
}

fn encode_vint_u64(mut val: u64, buf: &mut [u8; 10]) -> usize {
    let mut i = 0;
    while val >= 0x80 {
        buf[i] = (val as u8) | 0x80;
        val >>= 7;
        i += 1;
    }
    buf[i] = val as u8;
    i + 1
}

fn decode_vint_u64(data: &[u8]) -> io::Result<(u64, usize)> {
    let mut val: u64 = 0;
    let mut shift: u32 = 0;
    for (i, byte) in data.iter().copied().enumerate() {
        val |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((val, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "split fields value vint overflow",
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "split fields value vint truncated",
    ))
}

fn pack_flags(searchable: bool, aggregatable: bool) -> u8 {
    let mut flags = 0u8;
    if searchable {
        flags |= SEARCHABLE_BIT;
    }
    if aggregatable {
        flags |= AGGREGATABLE_BIT;
    }
    flags
}

fn unpack_flags(flags: u8) -> (bool, bool) {
    (flags & SEARCHABLE_BIT != 0, flags & AGGREGATABLE_BIT != 0)
}

fn encode_key(field_name: &str, field_type: ListFieldType) -> Vec<u8> {
    let name_bytes = field_name.as_bytes();
    let mut key = Vec::with_capacity(name_bytes.len() + 1);
    key.extend_from_slice(name_bytes);
    key.push(field_type as u8);
    key
}

fn decode_key(key: &[u8]) -> io::Result<(&str, ListFieldType)> {
    let Some((&type_byte, name_bytes)) = key.split_last() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "split fields key is empty",
        ));
    };
    let field_type = ListFieldType::try_from(type_byte as i32).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown field type byte: {type_byte}"),
        )
    })?;
    let field_name = std::str::from_utf8(name_bytes).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("split fields key is not valid utf-8: {err}"),
        )
    })?;
    Ok((field_name, field_type))
}

/// Serializes the split fields to the v3 SSTable format.
///
/// `list_fields.fields` must be sorted by (field_name, field_type) with no
/// duplicates. This matches the invariant already maintained upstream in the
/// packager.
pub fn serialize_split_fields(list_fields: ListFields) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(FORMAT_VERSION_V3_SSTABLE);

    let mut builder =
        Dictionary::<FieldMetadataSSTable>::builder(&mut out).expect("writing to Vec cannot fail");
    for entry in &list_fields.fields {
        let field_type = ListFieldType::try_from(entry.field_type).unwrap_or(ListFieldType::Str);
        let key = encode_key(&entry.field_name, field_type);
        let flags = pack_flags(entry.searchable, entry.aggregatable);
        builder
            .insert(&key, &flags)
            .expect("writing to Vec cannot fail");
    }
    builder.finish().expect("writing to Vec cannot fail");
    out
}

/// Reads the split fields from a stream of bytes.
///
/// Supports both the legacy v2 (zstd + prost) and the current v3 (SSTable)
/// formats. The returned `ListFields` is fully materialized; the index_ids
/// vectors are left empty (they are only populated by the cross-index merge
/// in `quickwit-search`).
pub fn deserialize_split_fields<R: Read>(mut reader: R) -> io::Result<ListFields> {
    let mut version_buf = [0u8; 1];
    reader.read_exact(&mut version_buf)?;
    match version_buf[0] {
        FORMAT_VERSION_V2_ZSTD_PROST => deserialize_v2(reader),
        FORMAT_VERSION_V3_SSTABLE => deserialize_v3(reader),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported split fields format version: {other}"),
        )),
    }
}

fn deserialize_v2<R: Read>(reader: R) -> io::Result<ListFields> {
    let decoder = zstd::Decoder::new(reader)?;
    read_v2_payload(decoder)
}

#[allow(clippy::unbuffered_bytes)]
fn read_v2_payload<R: Read>(reader: R) -> io::Result<ListFields> {
    let all_bytes: Vec<u8> = reader.bytes().collect::<io::Result<_>>()?;
    let list_fields: ListFields = prost::Message::decode(&all_bytes[..])?;
    Ok(list_fields)
}

fn deserialize_v3<R: Read>(mut reader: R) -> io::Result<ListFields> {
    let mut payload = Vec::new();
    reader.read_to_end(&mut payload)?;
    let dictionary = Dictionary::<FieldMetadataSSTable>::from_bytes(OwnedBytes::new(payload))?;
    let mut stream = dictionary.stream()?;
    let mut fields = Vec::with_capacity(dictionary.num_terms());
    while stream.advance() {
        let (field_name, field_type) = decode_key(stream.key())?;
        let flags = *stream.value();
        let (searchable, aggregatable) = unpack_flags(flags);
        fields.push(ListFieldsEntryResponse {
            field_name: field_name.to_string(),
            field_type: field_type as i32,
            searchable,
            aggregatable,
            index_ids: Vec::new(),
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
        });
    }
    Ok(ListFields { fields })
}

/// Async reader over a v3 split-fields file.
///
/// Pre-warms the version byte, the SSTable footer (last 20 bytes), and the
/// SSTable index region via three async fetches in `open()`. After that,
/// streaming through the dictionary issues additional async range reads only
/// for the data blocks that need to be visited.
///
/// The legacy v2 (zstd + prost) format is **not** supported by this reader —
/// callers facing a v2 split must fall back to the eager `deserialize_split_fields`
/// path. The version byte is checked up-front so the caller can dispatch.
#[derive(Debug)]
pub struct SplitFieldsReader {
    dictionary: Dictionary<FieldMetadataSSTable>,
}

/// Size of the trailing SSTable footer (index_offset:u64 | num_terms:u64 | version:u32).
const SSTABLE_FOOTER_LEN: usize = 20;
/// Size of the version byte that prefixes the SSTable payload on disk.
const VERSION_HEADER_LEN: usize = 1;

impl SplitFieldsReader {
    /// Opens an SSTable-backed split fields file via an async `FileHandle`.
    ///
    /// `total_len` must be the full file length (version byte + SSTable). Three
    /// async fetches are issued before `Dictionary::open` is called:
    ///   - version byte at offset 0
    ///   - SSTable footer at `[total_len - 20 .. total_len)`
    ///   - SSTable index region at `[1 + index_offset .. total_len - 20)`
    /// Their bytes are stuffed into an internal cache so that `Dictionary::open`'s
    /// synchronous footer/index reads do not fault.
    pub async fn open(file_handle: Arc<dyn FileHandle>, total_len: usize) -> io::Result<Self> {
        if total_len < VERSION_HEADER_LEN + SSTABLE_FOOTER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "split fields file too short: {total_len} < {}",
                    VERSION_HEADER_LEN + SSTABLE_FOOTER_LEN
                ),
            ));
        }

        let version_bytes = file_handle.read_bytes_async(0..VERSION_HEADER_LEN).await?;
        let version = version_bytes[0];
        if version != FORMAT_VERSION_V3_SSTABLE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "SplitFieldsReader requires v3 SSTable format, found v{version}; use \
                     deserialize_split_fields for legacy v2"
                ),
            ));
        }

        let sstable_end = total_len;
        let footer_range = sstable_end - SSTABLE_FOOTER_LEN..sstable_end;
        let footer_bytes = file_handle.read_bytes_async(footer_range.clone()).await?;
        let index_offset = u64::from_le_bytes(footer_bytes[0..8].try_into().unwrap()) as usize;
        // The index_offset stored in the footer is sstable-relative (i.e. excludes the version
        // byte). Translate to a file-relative offset.
        let index_range = (VERSION_HEADER_LEN + index_offset)..(sstable_end - SSTABLE_FOOTER_LEN);
        if index_range.start > index_range.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "split fields footer reports index_offset past end of file \
                     (index_offset={index_offset}, file_len={total_len})"
                ),
            ));
        }
        let index_bytes = file_handle.read_bytes_async(index_range.clone()).await?;

        let prewarmed = Arc::new(PrewarmedFileHandle {
            underlying: file_handle,
            cached: vec![(footer_range, footer_bytes), (index_range, index_bytes)],
            len: total_len,
        });
        // Drop the version byte by slicing the FileSlice from offset 1 — the SSTable layer
        // does not know about our 1-byte version header.
        let file_slice = FileSlice::new_with_num_bytes(prewarmed, total_len)
            .slice(VERSION_HEADER_LEN..total_len);
        let dictionary = Dictionary::<FieldMetadataSSTable>::open(file_slice)?;
        Ok(SplitFieldsReader { dictionary })
    }

    /// Number of entries in the underlying SSTable.
    pub fn num_fields(&self) -> usize {
        self.dictionary.num_terms()
    }

    /// Streams every entry from the SSTable, materializing them into a
    /// `Vec<ListFieldsEntryResponse>`. Block bytes are fetched lazily via
    /// `read_bytes_async` on the underlying `FileHandle`.
    pub async fn read_all_async(&self) -> io::Result<Vec<ListFieldsEntryResponse>> {
        let mut stream = self.dictionary.range().into_stream_async().await?;
        let mut fields = Vec::with_capacity(self.num_fields());
        while stream.advance() {
            let (field_name, field_type) = decode_key(stream.key())?;
            let flags = *stream.value();
            let (searchable, aggregatable) = unpack_flags(flags);
            fields.push(ListFieldsEntryResponse {
                field_name: field_name.to_string(),
                field_type: field_type as i32,
                searchable,
                aggregatable,
                index_ids: Vec::new(),
                non_searchable_index_ids: Vec::new(),
                non_aggregatable_index_ids: Vec::new(),
            });
        }
        Ok(fields)
    }
}

/// `FileHandle` adapter that serves a small set of pre-fetched byte ranges from
/// memory and delegates everything else to an underlying async handle.
///
/// Used to satisfy `Dictionary::open`'s synchronous footer/index reads when the
/// underlying storage is async-only (the sync `read_bytes` path otherwise has
/// to fault or panic).
struct PrewarmedFileHandle {
    underlying: Arc<dyn FileHandle>,
    /// Cached ranges, in the original file's coordinate system.
    cached: Vec<(Range<usize>, OwnedBytes)>,
    len: usize,
}

impl PrewarmedFileHandle {
    fn lookup_cached(&self, range: &Range<usize>) -> Option<OwnedBytes> {
        for (cached_range, bytes) in &self.cached {
            if cached_range.start <= range.start && range.end <= cached_range.end {
                let offset = range.start - cached_range.start;
                let len = range.end - range.start;
                return Some(bytes.slice(offset..offset + len));
            }
        }
        None
    }
}

impl HasLen for PrewarmedFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

impl fmt::Debug for PrewarmedFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrewarmedFileHandle")
            .field("len", &self.len)
            .field(
                "cached_ranges",
                &self.cached.iter().map(|(r, _)| r).collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[async_trait]
impl FileHandle for PrewarmedFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.lookup_cached(&range) {
            return Ok(bytes);
        }
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "synchronous read_bytes on a range not pre-warmed (range={range:?}, file_len={})",
                self.len
            ),
        ))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.lookup_cached(&range) {
            return Ok(bytes);
        }
        self.underlying.read_bytes_async(range).await
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn vint_roundtrip() {
        for val in [0u64, 1, 127, 128, 255, 16_383, 16_384, u64::MAX] {
            let mut buf = [0u8; 10];
            let n = encode_vint_u64(val, &mut buf);
            let (decoded, consumed) = decode_vint_u64(&buf[..n]).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(consumed, n);
        }
    }

    #[test]
    fn flag_pack_roundtrip() {
        for (s, a) in [(false, false), (true, false), (false, true), (true, true)] {
            assert_eq!(unpack_flags(pack_flags(s, a)), (s, a));
        }
    }

    #[test]
    fn key_roundtrip() {
        let key = encode_key("attributes.http.method", ListFieldType::Str);
        let (name, ty) = decode_key(&key).unwrap();
        assert_eq!(name, "attributes.http.method");
        assert_eq!(ty, ListFieldType::Str);
    }

    #[test]
    fn key_lex_order_groups_by_prefix() {
        // Two fields sharing the `attributes.http.` prefix should be adjacent
        // in lex order regardless of type byte, so a prefix block scan can
        // grab them with one range fetch.
        let mut keys = [
            encode_key("attributes.http.method", ListFieldType::Str),
            encode_key("attributes.http.status", ListFieldType::U64),
            encode_key("attributes.db.system", ListFieldType::Str),
            encode_key("body", ListFieldType::Str),
        ];
        keys.sort();
        let names: Vec<_> = keys
            .iter()
            .map(|k| decode_key(k).unwrap().0.to_string())
            .collect();
        assert_eq!(
            names,
            vec![
                "attributes.db.system",
                "attributes.http.method",
                "attributes.http.status",
                "body",
            ]
        );
    }

    fn entry(
        name: &str,
        ty: ListFieldType,
        searchable: bool,
        aggregatable: bool,
    ) -> ListFieldsEntryResponse {
        ListFieldsEntryResponse {
            field_name: name.to_string(),
            field_type: ty as i32,
            searchable,
            aggregatable,
            index_ids: Vec::new(),
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
        }
    }

    #[test]
    fn v3_roundtrip() {
        let original = ListFields {
            fields: vec![
                entry("attributes.db.system", ListFieldType::Str, true, false),
                entry("attributes.http.method", ListFieldType::Str, true, true),
                entry("attributes.http.status", ListFieldType::U64, false, true),
                entry("body", ListFieldType::Str, true, false),
            ],
        };
        let bytes = serialize_split_fields(original.clone());
        assert_eq!(bytes[0], FORMAT_VERSION_V3_SSTABLE);
        let decoded = deserialize_split_fields(&bytes[..]).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn v2_back_compat_read() {
        // Hand-build a v2 blob the way the legacy writer did: 0x02 || zstd(prost(ListFields)).
        let list_fields = ListFields {
            fields: vec![entry("body", ListFieldType::Str, true, false)],
        };
        let payload = list_fields.encode_to_vec();
        let compressed = zstd::stream::encode_all(&payload[..], 3).unwrap();
        let mut blob = vec![FORMAT_VERSION_V2_ZSTD_PROST];
        blob.extend_from_slice(&compressed);
        let decoded = deserialize_split_fields(&blob[..]).unwrap();
        assert_eq!(decoded, list_fields);
    }

    #[test]
    fn unknown_version_errors() {
        let blob = [99u8, 0, 0, 0];
        let err = deserialize_split_fields(&blob[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn split_fields_reader_round_trip() {
        let original = ListFields {
            fields: vec![
                entry("attributes.db.system", ListFieldType::Str, true, false),
                entry("attributes.http.method", ListFieldType::Str, true, true),
                entry("attributes.http.status", ListFieldType::U64, false, true),
                entry("body", ListFieldType::Str, true, false),
            ],
        };
        let bytes = serialize_split_fields(original.clone());
        let total_len = bytes.len();
        let file_handle: Arc<dyn FileHandle> = Arc::new(OwnedBytes::new(bytes));

        let reader = SplitFieldsReader::open(file_handle, total_len)
            .await
            .unwrap();
        assert_eq!(reader.num_fields(), original.fields.len());
        let decoded = reader.read_all_async().await.unwrap();
        assert_eq!(decoded, original.fields);
    }

    #[tokio::test]
    async fn split_fields_reader_rejects_v2() {
        let list_fields = ListFields {
            fields: vec![entry("body", ListFieldType::Str, true, false)],
        };
        let payload = list_fields.encode_to_vec();
        let compressed = zstd::stream::encode_all(&payload[..], 3).unwrap();
        let mut blob = vec![FORMAT_VERSION_V2_ZSTD_PROST];
        blob.extend_from_slice(&compressed);
        let total_len = blob.len();
        let file_handle: Arc<dyn FileHandle> = Arc::new(OwnedBytes::new(blob));

        let err = SplitFieldsReader::open(file_handle, total_len)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
