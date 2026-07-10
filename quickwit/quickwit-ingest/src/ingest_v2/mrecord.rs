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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message as _;
use quickwit_proto::ingest::{
    CommitMRecord, DocBatchV2, DocCompression, DocMRecord, MRecordBatch, MRecordV1, m_record_v1,
};

use super::metrics::{
    RECORDS_COMPRESSED_BYTES_TOTAL, RECORDS_UNCOMPRESSED_BYTES_TOTAL, SKIPPED_MRECORDS_MALFORMED,
    SKIPPED_MRECORDS_UNKNOWN_HEADER_VERSION, SKIPPED_MRECORDS_UNKNOWN_RECORD_TYPE,
};

/// zstd compression level used for per-document record compression. Level 0 selects zstd's default
/// level, matching the metastore's compressed JSON blobs.
const ZSTD_COMPRESSION_LEVEL: i32 = 0;

/// Compresses each document of `doc_batch` individually with zstd, rebuilding `doc_buffer` and
/// `doc_lengths` and stamping `doc_compression`. The document count, order, `doc_uids` and
/// `doc_format` are preserved, so the per-document WAL positions assigned downstream are unchanged.
///
/// zstd compression of an in-memory buffer does not fail in practice; should it ever fail, the
/// document batch is returned uncompressed (with a warning) rather than dropping documents.
pub(super) fn compress_doc_batch(doc_batch: DocBatchV2) -> DocBatchV2 {
    match try_compress_doc_batch(&doc_batch) {
        Some(compressed_doc_batch) => compressed_doc_batch,
        None => doc_batch,
    }
}

/// Attempts to compress every document of `doc_batch`. Returns `None` (leaving the caller to fall
/// back to the uncompressed batch) if any document fails to compress. Borrows `doc_batch` so the
/// caller keeps ownership of the original for that fallback.
fn try_compress_doc_batch(doc_batch: &DocBatchV2) -> Option<DocBatchV2> {
    let num_docs = doc_batch.num_docs();
    let uncompressed_len = doc_batch.doc_buffer.len();
    let mut doc_uids = Vec::with_capacity(num_docs);
    let mut doc_lengths = Vec::with_capacity(num_docs);
    // Compressed output is usually smaller than the input; reserve the uncompressed size as a hint.
    let mut doc_buffer = BytesMut::with_capacity(uncompressed_len);
    for (doc_uid, doc) in doc_batch.docs() {
        let compressed_doc = match zstd::encode_all(&doc[..], ZSTD_COMPRESSION_LEVEL) {
            Ok(compressed_doc) => compressed_doc,
            Err(error) => {
                quickwit_common::rate_limited_warn!(
                    limit_per_min = 6,
                    "failed to compress document, sending the batch uncompressed: {error}"
                );
                return None;
            }
        };
        doc_lengths.push(compressed_doc.len() as u32);
        doc_buffer.put_slice(&compressed_doc);
        doc_uids.push(doc_uid);
    }
    RECORDS_UNCOMPRESSED_BYTES_TOTAL.inc_by(uncompressed_len as u64);
    RECORDS_COMPRESSED_BYTES_TOTAL.inc_by(doc_buffer.len() as u64);
    Some(DocBatchV2 {
        doc_buffer: doc_buffer.freeze(),
        doc_lengths,
        doc_uids,
        doc_format: doc_batch.doc_format,
        doc_compression: DocCompression::Zstd as i32,
    })
}

/// Decompresses a document previously compressed with `compression`. Returns `None` when decoding
/// fails so the caller can skip the record instead of panicking.
pub(super) fn decompress_doc(doc: Bytes, compression: DocCompression) -> Option<Bytes> {
    match compression {
        DocCompression::None => Some(doc),
        DocCompression::Zstd => match zstd::decode_all(&doc[..]) {
            Ok(decompressed) => Some(Bytes::from(decompressed)),
            Err(_) => None,
        },
    }
}

/// Encodes an already-compressed document as a [`HeaderVersion::V1`] `DocMRecord` carrying the
/// codec marker. The document bytes are stored verbatim (they are already compressed); this only
/// frames them so the reader knows to decompress with `compression`.
pub(super) fn encode_compressed_doc_v1(
    compressed_doc: Bytes,
    compression: DocCompression,
) -> Bytes {
    let mrecord_v1 = MRecordV1 {
        mrecord: Some(m_record_v1::Mrecord::Doc(DocMRecord {
            doc: compressed_doc,
            compression: compression as i32,
        })),
    };
    let mut buf = BytesMut::with_capacity(1 + mrecord_v1.encoded_len());
    buf.put_u8(HeaderVersion::V1 as u8);
    // Encoding a prost message into a `BytesMut` cannot fail: it only errors on insufficient
    // capacity, and `BytesMut` grows on demand.
    mrecord_v1
        .encode(&mut buf)
        .expect("encoding an mrecord into a `BytesMut` should never fail");
    buf.freeze()
}

/// The first byte of an encoded [`MRecord`] is the version of the record header. It selects how the
/// remaining bytes are interpreted, which makes the on-disk format forward and backward compatible:
/// an older binary that does not know a header version skips the record instead of misreading it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HeaderVersion {
    /// Version 0, introduced in Quickwit 0.7.0. The header is two bytes: the header version
    /// followed by one byte encoding the record type (`Doc = 0`, `Commit = 1`). For a `Doc`, the
    /// remaining bytes are the document verbatim.
    V0 = 0,
    /// Version 1. The header is a single byte (the header version) followed by a protobuf-encoded
    /// [`MRecordV1`] message.
    V1 = 1,
}

/// Length of the header of a [`HeaderVersion::V0`] [`MRecord`] in bytes.
pub(super) const MRECORD_HEADER_LEN: usize = 2;

/// `Doc` header v0 composed of the header version and the `Doc = 0` record type.
const DOC_HEADER_V0: &[u8; MRECORD_HEADER_LEN] = &[HeaderVersion::V0 as u8, 0];

/// `Commit` header v0 composed of the header version and the `Commit = 1` record type.
const COMMIT_HEADER_V0: &[u8; MRECORD_HEADER_LEN] = &[HeaderVersion::V0 as u8, 1];

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MRecord {
    Doc(Bytes),
    Commit,
}

/// Reason an encoded mrecord could not be decoded into a known [`MRecord`]. Each variant maps to a
/// metric label so a downgrade reading records written by a newer binary is observable rather than
/// silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipReason {
    /// The buffer is truncated or the protobuf payload failed to decode.
    Malformed,
    /// The header version byte is not known to this binary (record written by a newer binary).
    UnknownHeaderVersion,
    /// The header version is known but the record type / `mrecord` variant is not (newer binary).
    UnknownRecordType,
}

/// Outcome of decoding a single encoded mrecord. Unknown records are reported as [`Self::Skipped`]
/// instead of being silently dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodeOutcome {
    Decoded(MRecord),
    Skipped(SkipReason),
}

impl MRecord {
    /// Encodes the record using the legacy `HeaderVersion::V0` format. This remains the default
    /// write format: it is understood by every released binary, so emitting it keeps a rollback
    /// safe. Switching the write path to [`Self::encode_v1`] must only happen once every binary in
    /// the rollback window can decode v1.
    pub fn encode(&self) -> impl Buf + use<> {
        match &self {
            Self::Doc(doc) => DOC_HEADER_V0.chain(doc.clone()),
            Self::Commit => COMMIT_HEADER_V0.chain(Bytes::new()),
        }
    }

    /// Encodes the record using the extensible `HeaderVersion::V1` format (a protobuf
    /// `MRecordV1` payload). Not yet used by the write path; see [`Self::encode`].
    pub fn encode_v1(&self) -> Bytes {
        let mrecord = match self {
            Self::Doc(doc) => m_record_v1::Mrecord::Doc(DocMRecord {
                doc: doc.clone(),
                compression: DocCompression::None as i32,
            }),
            Self::Commit => m_record_v1::Mrecord::Commit(CommitMRecord {}),
        };
        let mrecord_v1 = MRecordV1 {
            mrecord: Some(mrecord),
        };
        let mut buf = BytesMut::with_capacity(1 + mrecord_v1.encoded_len());
        buf.put_u8(HeaderVersion::V1 as u8);
        // Encoding a prost message into a `BytesMut` cannot fail: it only errors on insufficient
        // capacity, and `BytesMut` grows on demand.
        mrecord_v1
            .encode(&mut buf)
            .expect("encoding an mrecord into a `BytesMut` should never fail");
        buf.freeze()
    }

    pub fn decode(buf: impl Buf) -> Option<Self> {
        match Self::decode_inner(buf) {
            DecodeOutcome::Decoded(mrecord) => Some(mrecord),
            DecodeOutcome::Skipped(_) => None,
        }
    }

    fn decode_inner(mut buf: impl Buf) -> DecodeOutcome {
        if buf.remaining() < 1 {
            return DecodeOutcome::Skipped(SkipReason::Malformed);
        }
        let header_version = buf.get_u8();

        if header_version == HeaderVersion::V0 as u8 {
            Self::decode_v0(buf)
        } else if header_version == HeaderVersion::V1 as u8 {
            Self::decode_v1(buf)
        } else {
            DecodeOutcome::Skipped(SkipReason::UnknownHeaderVersion)
        }
    }

    fn decode_v0(mut buf: impl Buf) -> DecodeOutcome {
        if buf.remaining() < 1 {
            return DecodeOutcome::Skipped(SkipReason::Malformed);
        }
        match buf.get_u8() {
            0 => {
                let doc = buf.copy_to_bytes(buf.remaining());
                DecodeOutcome::Decoded(Self::Doc(doc))
            }
            1 => DecodeOutcome::Decoded(Self::Commit),
            _ => DecodeOutcome::Skipped(SkipReason::UnknownRecordType),
        }
    }

    fn decode_v1(buf: impl Buf) -> DecodeOutcome {
        let mrecord_v1 = match MRecordV1::decode(buf) {
            Ok(mrecord_v1) => mrecord_v1,
            Err(_) => return DecodeOutcome::Skipped(SkipReason::Malformed),
        };
        match mrecord_v1.mrecord {
            Some(m_record_v1::Mrecord::Doc(doc_mrecord)) => {
                // `compression` may hold a codec minted by a newer binary; treat an unknown value
                // as an unknown record type rather than misreading the bytes as uncompressed.
                let compression = match DocCompression::try_from(doc_mrecord.compression) {
                    Ok(compression) => compression,
                    Err(_) => return DecodeOutcome::Skipped(SkipReason::UnknownRecordType),
                };
                match decompress_doc(doc_mrecord.doc, compression) {
                    Some(doc) => DecodeOutcome::Decoded(Self::Doc(doc)),
                    None => DecodeOutcome::Skipped(SkipReason::Malformed),
                }
            }
            Some(m_record_v1::Mrecord::Commit(CommitMRecord {})) => {
                DecodeOutcome::Decoded(Self::Commit)
            }
            // An `mrecord` variant written by a newer binary that this one does not know.
            None => DecodeOutcome::Skipped(SkipReason::UnknownRecordType),
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn new_doc(doc: impl Into<Bytes>) -> Self {
        Self::Doc(doc.into())
    }
}

pub fn decoded_mrecords(mrecord_batch: &MRecordBatch) -> impl Iterator<Item = MRecord> + '_ {
    mrecord_batch
        .encoded_mrecords()
        .filter_map(
            |encoded_mrecord| match MRecord::decode_inner(encoded_mrecord) {
                DecodeOutcome::Decoded(mrecord) => Some(mrecord),
                DecodeOutcome::Skipped(reason) => {
                    on_skipped_mrecord(reason);
                    None
                }
            },
        )
}

/// Records a skipped mrecord on its metric and emits a rate-limited warning. A burst of skips
/// typically means a binary is reading records written by a newer one (e.g. after a downgrade).
fn on_skipped_mrecord(reason: SkipReason) {
    match reason {
        SkipReason::Malformed => {
            SKIPPED_MRECORDS_MALFORMED.inc();
            quickwit_common::rate_limited_warn!(limit_per_min = 6, "skipped malformed mrecord");
        }
        SkipReason::UnknownHeaderVersion => {
            SKIPPED_MRECORDS_UNKNOWN_HEADER_VERSION.inc();
            quickwit_common::rate_limited_warn!(
                limit_per_min = 6,
                "skipped mrecord with an unknown header version, it was likely written by a newer \
                 binary"
            );
        }
        SkipReason::UnknownRecordType => {
            SKIPPED_MRECORDS_UNKNOWN_RECORD_TYPE.inc();
            quickwit_common::rate_limited_warn!(
                limit_per_min = 6,
                "skipped mrecord with an unknown record type, it was likely written by a newer \
                 binary"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_invalid_mrecord() {
        // Truncated buffers.
        assert_eq!(
            MRecord::decode_inner(&b""[..]),
            DecodeOutcome::Skipped(SkipReason::Malformed)
        );
        assert_eq!(
            MRecord::decode_inner(&[HeaderVersion::V0 as u8][..]),
            DecodeOutcome::Skipped(SkipReason::Malformed)
        );
        // Unknown header version (e.g. a record written by a newer binary).
        assert_eq!(
            MRecord::decode_inner(&b"a"[..]),
            DecodeOutcome::Skipped(SkipReason::UnknownHeaderVersion)
        );
        // Known v0 header, unknown record type.
        assert_eq!(
            MRecord::decode_inner(&[HeaderVersion::V0 as u8, 19u8][..]),
            DecodeOutcome::Skipped(SkipReason::UnknownRecordType)
        );
    }

    #[test]
    fn test_mrecord_doc_roundtrip() {
        let record = MRecord::new_doc("hello");
        let encoded_record = record.encode();
        let decoded_record = MRecord::decode(encoded_record).unwrap();
        assert_eq!(record, decoded_record);
    }

    #[test]
    fn test_mrecord_commit_roundtrip() {
        let record = MRecord::Commit;
        let encoded_record = record.encode();
        let decoded_record = MRecord::decode(encoded_record).unwrap();
        assert_eq!(record, decoded_record);
    }

    #[test]
    fn test_mrecord_doc_roundtrip_v1() {
        let record = MRecord::new_doc("hello");
        let encoded_record = record.encode_v1();
        // First byte selects the v1 format.
        assert_eq!(encoded_record[0], HeaderVersion::V1 as u8);
        let decoded_record = MRecord::decode(encoded_record).unwrap();
        assert_eq!(record, decoded_record);
    }

    #[test]
    fn test_mrecord_commit_roundtrip_v1() {
        let record = MRecord::Commit;
        let encoded_record = record.encode_v1();
        assert_eq!(encoded_record[0], HeaderVersion::V1 as u8);
        let decoded_record = MRecord::decode(encoded_record).unwrap();
        assert_eq!(record, decoded_record);
    }

    #[test]
    fn test_mrecord_v1_forward_compatible_unknown_field() {
        // A `DocMRecord` written by a newer binary carrying an extra, unknown field (here field 3,
        // a varint — e.g. a future arrival timestamp; field 2 is `compression`). An older binary
        // must still recover the doc.
        let doc_mrecord_with_unknown_field: &[u8] = &[
            0x0A, 0x05, b'h', b'e', b'l', b'l', b'o', // field 1 (doc): len-delimited "hello"
            0x18, 0x2A, // field 3 (unknown): varint 42
        ];
        let mut mrecord_v1 = vec![HeaderVersion::V1 as u8];
        // Wrap it in the `MRecordV1` oneof as field 1 (doc), len-delimited.
        mrecord_v1.push(0x0A);
        mrecord_v1.push(doc_mrecord_with_unknown_field.len() as u8);
        mrecord_v1.extend_from_slice(doc_mrecord_with_unknown_field);

        let decoded_record = MRecord::decode(&mrecord_v1[..]).unwrap();
        assert_eq!(decoded_record, MRecord::new_doc("hello"));
    }

    #[test]
    fn test_mrecord_v1_unknown_variant_is_skipped() {
        // An `MRecordV1` whose `mrecord` oneof is set to an unknown field number (3), as a newer
        // binary minting a new record type would produce. An older binary skips it.
        let mrecord_v1: &[u8] = &[
            HeaderVersion::V1 as u8,
            0x1A, // field 3, len-delimited
            0x00, // length 0
        ];
        assert_eq!(
            MRecord::decode_inner(mrecord_v1),
            DecodeOutcome::Skipped(SkipReason::UnknownRecordType)
        );
    }

    #[test]
    fn test_mrecord_v1_malformed_payload_is_skipped() {
        // A v1 header followed by bytes that are not a valid protobuf message.
        let mrecord_v1: &[u8] = &[HeaderVersion::V1 as u8, 0xFF, 0xFF, 0xFF];
        assert_eq!(
            MRecord::decode_inner(mrecord_v1),
            DecodeOutcome::Skipped(SkipReason::Malformed)
        );
    }

    #[test]
    fn test_compress_doc_batch_roundtrip() {
        let doc_batch = DocBatchV2::for_test([
            "hello world hello world hello world",
            "another document with some repetition repetition",
        ]);
        let compressed_doc_batch = compress_doc_batch(doc_batch.clone());
        assert_eq!(compressed_doc_batch.doc_compression(), DocCompression::Zstd);
        assert_eq!(compressed_doc_batch.num_docs(), doc_batch.num_docs());
        assert_eq!(compressed_doc_batch.doc_uids, doc_batch.doc_uids);

        // Each compressed document, framed as a v1 mrecord, decodes back to the original.
        let original_docs: Vec<Bytes> = doc_batch.docs().map(|(_doc_uid, doc)| doc).collect();
        for ((_doc_uid, compressed_doc), original_doc) in
            compressed_doc_batch.docs().zip(original_docs)
        {
            let encoded_mrecord = encode_compressed_doc_v1(compressed_doc, DocCompression::Zstd);
            assert_eq!(encoded_mrecord[0], HeaderVersion::V1 as u8);
            let decoded_mrecord = MRecord::decode(encoded_mrecord).unwrap();
            assert_eq!(decoded_mrecord, MRecord::Doc(original_doc));
        }
    }

    #[test]
    fn test_decode_v1_malformed_compressed_doc_is_skipped() {
        // The record claims zstd compression but the payload is not valid zstd.
        let encoded_mrecord =
            encode_compressed_doc_v1(Bytes::from_static(b"not zstd"), DocCompression::Zstd);
        assert_eq!(
            MRecord::decode_inner(encoded_mrecord),
            DecodeOutcome::Skipped(SkipReason::Malformed)
        );
    }

    #[test]
    fn test_decode_v1_unknown_compression_is_skipped() {
        // A `DocMRecord` whose `compression` field (2) holds a codec (99) minted by a newer binary.
        // An older binary must skip it rather than misread the bytes as uncompressed.
        let doc_mrecord: &[u8] = &[
            0x0A, 0x03, b'a', b'b', b'c', // field 1 (doc): len-delimited "abc"
            0x10, 0x63, // field 2 (compression): varint 99
        ];
        let mut mrecord_v1 = vec![HeaderVersion::V1 as u8, 0x0A, doc_mrecord.len() as u8];
        mrecord_v1.extend_from_slice(doc_mrecord);
        assert_eq!(
            MRecord::decode_inner(&mrecord_v1[..]),
            DecodeOutcome::Skipped(SkipReason::UnknownRecordType)
        );
    }
}
