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

use bytes::{Buf, Bytes};
use quickwit_proto::ingest::MRecordBatch;
use tracing::warn;

/// The first byte of a [`MRecord`] is the version of the record header.
#[derive(Debug)]
#[repr(u8)]
pub enum HeaderVersion {
    /// Version 0, introduced in Quickwit 0.7.0, it uses one byte to encode the record type.
    V0 = 0,
}

/// Length of the header of a [`MRecord`] in bytes.
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

impl MRecord {
    pub fn encode(&self) -> impl Buf + use<> {
        match &self {
            Self::Doc(doc) => DOC_HEADER_V0.chain(doc.clone()),
            Self::Commit => COMMIT_HEADER_V0.chain(Bytes::new()),
        }
    }

    pub fn decode(mut buf: impl Buf) -> Option<Self> {
        if buf.remaining() < 2 {
            return None;
        }

        let header_version = buf.get_u8();

        if header_version != HeaderVersion::V0 as u8 {
            warn!("unknown mrecord header version `{header_version}`");
            return None;
        }

        let mrecord = match buf.get_u8() {
            0 => {
                let doc = buf.copy_to_bytes(buf.remaining());
                Self::Doc(doc)
            }
            1 => Self::Commit,
            other => {
                warn!("unknown mrecord type `{other}`");
                return None;
            }
        };
        Some(mrecord)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn new_doc(doc: impl Into<Bytes>) -> Self {
        Self::Doc(doc.into())
    }
}

pub fn decoded_mrecords(mrecord_batch: &MRecordBatch) -> impl Iterator<Item = MRecord> + '_ {
    mrecord_batch.encoded_mrecords().flat_map(MRecord::decode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_invalid_mrecord() {
        assert!(MRecord::decode(&b""[..]).is_none());
        assert!(MRecord::decode(&b"a"[..]).is_none());
        assert!(MRecord::decode(&[HeaderVersion::V0 as u8][..]).is_none());
        assert!(MRecord::decode(&[HeaderVersion::V0 as u8, 19u8][..]).is_none());
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
}
