// Copyright (C) 2023 Quickwit, Inc.
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

use bytes::{Buf, Bytes};
use quickwit_proto::ingest::MRecordBatch;

/// The first byte of a [`MRecord`] is the version of the record header.
#[derive(Debug)]
#[repr(u8)]
pub enum HeaderVersion {
    /// Version 0, introduced in Quickwit 0.7.0, it uses one byte to encode the record type.
    V0 = 0,
}

/// `Doc` header v0 composed of the header version and the `Doc = 0` record type.
const DOC_HEADER_V0: &[u8; 2] = &[HeaderVersion::V0 as u8, 0];

/// `Commit` header v0 composed of the header version and the `Commit = 1` record type.
const COMMIT_HEADER_V0: &[u8; 2] = &[HeaderVersion::V0 as u8, 1];

/// `Eof` header v0 composed of the header version and the `Eof = 2` record type.
const EOF_HEADER_V0: &[u8; 2] = &[HeaderVersion::V0 as u8, 2];

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MRecord {
    Doc(Bytes),
    Commit,
    Eof,
    Unknown,
}

impl MRecord {
    pub fn encode(&self) -> impl Buf {
        match &self {
            Self::Doc(doc) => DOC_HEADER_V0.chain(doc.clone()),
            Self::Commit => COMMIT_HEADER_V0.chain(Bytes::new()),
            Self::Eof => EOF_HEADER_V0.chain(Bytes::new()),
            Self::Unknown => panic!("unknown mrecord type should not be encoded"),
        }
    }

    pub fn decode(mut buf: impl Buf) -> Self {
        let header_version = buf.get_u8();

        if header_version != HeaderVersion::V0 as u8 {
            return Self::Unknown;
        }
        match buf.get_u8() {
            0 => {
                let doc = buf.copy_to_bytes(buf.remaining());
                Self::Doc(doc)
            }
            1 => Self::Commit,
            2 => Self::Eof,
            _ => Self::Unknown,
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn new_doc(doc: impl Into<Bytes>) -> Self {
        Self::Doc(doc.into())
    }
}

pub fn decoded_mrecords(mrecord_batch: &MRecordBatch) -> impl Iterator<Item = MRecord> + '_ {
    mrecord_batch.encoded_mrecords().map(MRecord::decode)
}

pub(super) fn is_eof_mrecord(mrecord: &[u8]) -> bool {
    mrecord == EOF_HEADER_V0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mrecord_doc_roundtrip() {
        let record = MRecord::new_doc("hello");
        let encoded_record = record.encode();
        let decoded_record = MRecord::decode(encoded_record);
        assert_eq!(record, decoded_record);
    }

    #[test]
    fn test_mrecord_commit_roundtrip() {
        let record = MRecord::Commit;
        let encoded_record = record.encode();
        let decoded_record = MRecord::decode(encoded_record);
        assert_eq!(record, decoded_record);
    }

    #[test]
    fn test_mrecord_eof_roundtrip() {
        let record = MRecord::Eof;
        let encoded_record = record.encode();
        let decoded_record = MRecord::decode(encoded_record);
        assert_eq!(record, decoded_record);
    }

    #[test]
    fn test_mrecord_is_eof_mrecord() {
        assert!(is_eof_mrecord(EOF_HEADER_V0));
        assert!(!is_eof_mrecord(COMMIT_HEADER_V0));
    }
}
