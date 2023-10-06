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

mod fetch;
mod gc;
mod ingest_metastore;
mod ingester;
mod models;
mod replication;
mod router;
mod shard_table;
#[cfg(test)]
mod test_utils;

use bytes::{BufMut, BytesMut};
use quickwit_common::tower::Pool;
use quickwit_proto::ingest::ingester::IngesterServiceClient;
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::types::NodeId;

pub use self::fetch::MultiFetchStream;
pub use self::ingest_metastore::IngestMetastore;
pub use self::ingester::Ingester;
pub use self::router::IngestRouter;

pub type IngesterPool = Pool<NodeId, IngesterServiceClient>;

/// Identifies an ingester client, typically a source, for logging and debugging purposes.
pub type ClientId = String;

#[derive(Default)]
pub(crate) struct DocBatchBuilderV2 {
    doc_buffer: BytesMut,
    doc_lengths: Vec<u32>,
}

impl DocBatchBuilderV2 {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            doc_buffer: BytesMut::with_capacity(capacity),
            doc_lengths: Vec::new(),
        }
    }

    pub fn add_doc(&mut self, doc: &[u8]) {
        self.doc_lengths.push(doc.len() as u32);
        self.doc_buffer.put(doc);
    }

    pub fn build(self) -> DocBatchV2 {
        DocBatchV2 {
            doc_buffer: self.doc_buffer.freeze(),
            doc_lengths: self.doc_lengths,
        }
    }

    /// Returns the capacity of the underlying buffer, expressed in bytes.
    pub fn capacity(&self) -> usize {
        self.doc_buffer.capacity()
    }

    fn num_bytes(&self) -> usize {
        self.doc_buffer.len()
    }
}
