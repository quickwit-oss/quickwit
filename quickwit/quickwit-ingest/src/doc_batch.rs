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

use bytes::buf::Writer;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;

use crate::DocBatch;

#[derive(Debug)]
/// Represents a command that can be stored in a [`DocBatch`].
pub enum DocCommand<T>
where T: Buf
{
    Ingest { payload: T },
    Commit,
    // ... more to come?
}

/// We can use this byte to track both commands and their version changes
/// If serialization protocol changes, we can just use the next number
#[derive(Debug)]
#[repr(u8)]
pub enum DocCommandCode {
    IngestV1 = 0,
    CommitV1 = 1,
}

impl From<u8> for DocCommandCode {
    fn from(value: u8) -> Self {
        match value {
            0 => DocCommandCode::IngestV1,
            1 => DocCommandCode::CommitV1,
            other => panic!("Encountered unknown command: code {other}"),
        }
    }
}

impl<T> DocCommand<T>
where T: Buf + Default
{
    /// Returns the binary serialization code for the current version of this command.
    pub fn code(&self) -> DocCommandCode {
        match self {
            DocCommand::Ingest { payload: _ } => DocCommandCode::IngestV1,
            DocCommand::Commit => DocCommandCode::CommitV1,
        }
    }

    /// Builds a command for bytes::Buf
    pub fn read(mut buf: T) -> Self {
        match buf.get_u8().into() {
            DocCommandCode::IngestV1 => DocCommand::Ingest { payload: buf },
            DocCommandCode::CommitV1 => DocCommand::Commit,
        }
    }

    /// Copies the command to the end of bytes::BufMut while returning the number of bytes copied
    pub fn write(self, buf: &mut impl BufMut) -> usize {
        let self_buf = self.into_buf();
        let len = self_buf.remaining();
        buf.put(self_buf);
        len
    }

    pub fn into_buf(self) -> impl Buf {
        self.code_chunk().chain(match self {
            DocCommand::Ingest { payload } => payload,
            DocCommand::Commit => T::default(),
        })
    }

    fn code_chunk(&self) -> &'static [u8; 1] {
        match self {
            DocCommand::Ingest { payload: _ } => &[DocCommandCode::IngestV1 as u8],
            DocCommand::Commit => &[DocCommandCode::CommitV1 as u8],
        }
    }
}

/// Builds DocBatch from individual commands
pub struct DocBatchBuilder {
    index_id: String,
    concat_docs: BytesMut,
    doc_lens: Vec<u64>,
}

impl DocBatchBuilder {
    /// Creates a new batch builder with the given index name
    pub fn new(index_id: String) -> Self {
        Self {
            index_id,
            concat_docs: BytesMut::new(),
            doc_lens: Vec::new(),
        }
    }

    /// Adds an ingest command to the batch
    pub fn ingest_doc(&mut self, payload: impl Buf + Default) -> usize {
        let command = DocCommand::Ingest { payload };
        self.command(command)
    }

    /// Adds a commit command to the batch
    pub fn commit(&mut self) -> usize {
        let command: DocCommand<Bytes> = DocCommand::Commit;
        self.command(command)
    }

    /// Adds a parsed command to the batch
    pub fn command<T>(&mut self, command: DocCommand<T>) -> usize
    where T: Buf + Default {
        let len = command.write(&mut self.concat_docs);
        self.doc_lens.push(len as u64);
        len
    }

    /// Adds a list of bytes representing a command to the batch
    pub fn command_from_buf(&mut self, raw: impl Buf) -> usize {
        let len = raw.remaining();
        self.concat_docs.put(raw);
        self.doc_lens.push(len as u64);
        len
    }

    /// Creates another batch builder capable of processing a Serialize structs instead of commands
    pub fn json_writer(self) -> JsonDocBatchBuilder {
        JsonDocBatchBuilder {
            index_id: self.index_id,
            concat_docs: self.concat_docs.writer(),
            doc_lens: self.doc_lens,
        }
    }

    /// Builds the batch
    pub fn build(self) -> DocBatch {
        DocBatch {
            index_id: self.index_id,
            concat_docs: self.concat_docs.freeze(),
            doc_lens: self.doc_lens,
        }
    }
}

/// A wrapper around batch builder that can add a Serialize structs

pub struct JsonDocBatchBuilder {
    index_id: String,
    concat_docs: Writer<BytesMut>,
    doc_lens: Vec<u64>,
}

impl JsonDocBatchBuilder {
    /// Adds an ingest command to the batch for a Serialize struct
    pub fn ingest_doc(&mut self, payload: impl Serialize) -> serde_json::Result<usize> {
        let old_len = self.concat_docs.get_ref().len();
        self.concat_docs
            .get_mut()
            .put_u8(DocCommandCode::IngestV1 as u8);
        let res = serde_json::to_writer(&mut self.concat_docs, &payload);
        let new_len = self.concat_docs.get_ref().len();
        if let Err(err) = res {
            Err(err)
        } else {
            let len = new_len - old_len;
            self.doc_lens.push(len as u64);
            Ok(len)
        }
    }

    /// Returns the underlying batch builder
    pub fn into_inner(self) -> DocBatchBuilder {
        DocBatchBuilder {
            index_id: self.index_id,
            concat_docs: self.concat_docs.into_inner(),
            doc_lens: self.doc_lens,
        }
    }

    /// Builds the batch
    pub fn build(self) -> DocBatch {
        self.into_inner().build()
    }
}

impl DocBatch {
    /// Returns an iterator over the document payloads within a doc_batch.
    pub fn iter(&self) -> impl Iterator<Item = DocCommand<Bytes>> + '_ {
        self.iter_raw().map(DocCommand::read)
    }

    /// Returns an iterator over the document payloads within a doc_batch.
    pub fn iter_raw(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.doc_lens
            .iter()
            .cloned()
            .scan(0, |current_offset, doc_num_bytes| {
                let start = *current_offset;
                let end = start + doc_num_bytes as usize;
                *current_offset = end;
                Some(self.concat_docs.slice(start..end))
            })
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.doc_lens.is_empty()
    }

    /// Returns the total number of bytes in the batch.
    pub fn num_bytes(&self) -> usize {
        self.concat_docs.len()
    }

    /// Returns the number of documents in the batch.
    pub fn num_docs(&self) -> usize {
        self.doc_lens.len()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn commands_eq<L, R>(l: DocCommand<L>, r: DocCommand<R>) -> bool
    where
        L: Buf,
        R: Buf,
    {
        match (l, r) {
            (
                DocCommand::Ingest {
                    payload: mut l_payload,
                },
                DocCommand::Ingest {
                    payload: mut r_payload,
                },
            ) => {
                l_payload.copy_to_bytes(l_payload.remaining())
                    == r_payload.copy_to_bytes(r_payload.remaining())
            }
            (DocCommand::Commit, DocCommand::Commit) => true,
            _ => false,
        }
    }

    macro_rules! test_command_roundtrip {
        ($command:expr) => {
            let original = $command;
            let expected = $command;
            let mut buf = BytesMut::new();
            let size = original.write(&mut buf);
            assert!(size > 0);
            let copy = DocCommand::read(buf);
            assert!(commands_eq(expected, copy));
        };
    }

    #[test]
    fn test_commands_eq() {
        assert!(commands_eq(
            DocCommand::Ingest {
                payload: &b"hello"[..]
            },
            DocCommand::Ingest {
                payload: Bytes::from("hello")
            }
        ));
        assert!(commands_eq(
            DocCommand::Commit::<Bytes>,
            DocCommand::Commit::<&[u8]>
        ));
        assert!(!commands_eq(
            DocCommand::Ingest {
                payload: Bytes::from("hello")
            },
            DocCommand::Ingest {
                payload: Bytes::from("world")
            }
        ));
        assert!(!commands_eq(
            DocCommand::Ingest {
                payload: Bytes::from("hello")
            },
            DocCommand::Commit::<Bytes>
        ));
    }

    #[test]
    fn test_commands_roundtrip() {
        test_command_roundtrip!(DocCommand::Ingest {
            payload: &b"hello"[..]
        });
        test_command_roundtrip!(DocCommand::Ingest {
            payload: Bytes::from("hello")
        });
        test_command_roundtrip!(DocCommand::Commit::<Bytes>);
        test_command_roundtrip!(DocCommand::Commit::<&[u8]>);
    }

    #[test]
    fn test_batch_builder() {
        let mut batch = DocBatchBuilder::new("test".to_string());
        batch.ingest_doc(&b"hello"[..]);
        batch.ingest_doc(&b" "[..]);
        batch.command(DocCommand::Ingest {
            payload: Bytes::from("world"),
        });
        batch.commit();

        let batch = batch.build();
        assert_eq!(batch.index_id, "test");
        assert_eq!(batch.num_docs(), 4);
        assert_eq!(batch.num_bytes(), 5 + 1 + 5 + 4);

        let mut iter = batch.iter();
        assert!(commands_eq(
            iter.next().unwrap(),
            DocCommand::Ingest {
                payload: Bytes::from("hello")
            }
        ));
        assert!(commands_eq(
            iter.next().unwrap(),
            DocCommand::Ingest {
                payload: Bytes::from(" ")
            }
        ));
        assert!(commands_eq(
            iter.next().unwrap(),
            DocCommand::Ingest {
                payload: Bytes::from("world")
            }
        ));
        assert!(commands_eq(
            iter.next().unwrap(),
            DocCommand::Commit::<Bytes>
        ));
        assert!(iter.next().is_none());

        let mut copied_batch = DocBatchBuilder::new("test".to_string());
        for raw_buf in batch.iter_raw() {
            copied_batch.command_from_buf(raw_buf);
        }
        let copied_batch = copied_batch.build();

        assert_eq!(batch, copied_batch);
    }

    #[test]
    fn test_json_batch_builder() {
        let mut batch = DocBatchBuilder::new("test".to_string()).json_writer();
        batch.ingest_doc(json!({"test":"a"})).unwrap();
        batch.ingest_doc(json!({"test":"b"})).unwrap();

        let mut batch = batch.into_inner();
        batch.commit();

        let batch = batch.build();
        assert_eq!(batch.index_id, "test");
        assert_eq!(batch.num_docs(), 3);
        assert_eq!(batch.num_bytes(), 12 + 12 + 3);

        let mut iter = batch.iter();
        assert!(commands_eq(
            iter.next().unwrap(),
            DocCommand::Ingest {
                payload: Bytes::from(json!({"test": "a"}).to_string())
            }
        ));
        assert!(commands_eq(
            iter.next().unwrap(),
            DocCommand::Ingest {
                payload: Bytes::from(json!({"test": "b"}).to_string())
            }
        ));
        assert!(commands_eq(
            iter.next().unwrap(),
            DocCommand::Commit::<Bytes>
        ));
    }
}
