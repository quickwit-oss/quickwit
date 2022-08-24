// Copyright (C) 2022 Quickwit, Inc.
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

mod errors;
mod ingest_api_service;
mod position;
mod queue;

use std::path::Path;

use anyhow::Context;
pub use errors::IngestApiError;
use errors::Result;
pub use ingest_api_service::IngestApiService;
use once_cell::sync::OnceCell;
pub use position::Position;
pub use queue::Queues;
use quickwit_actors::{Mailbox, Universe};
use quickwit_proto::ingest_api::DocBatch;
use tracing::info;

pub static INGEST_API_SERVICE_INSTANCE: OnceCell<Mailbox<IngestApiService>> = OnceCell::new();

/// Initializes the [`IngestApiService`] singleton.
pub fn init_ingest_api(
    universe: &Universe,
    queue_path: &Path,
) -> anyhow::Result<Mailbox<IngestApiService>> {
    let ingest_api_service = INGEST_API_SERVICE_INSTANCE
        .get_or_try_init(|| spawn_ingest_api_actor(universe, queue_path))
        .context("Failed to initialize the ingest API service.")?;
    Ok(ingest_api_service.clone())
}

/// Gets the instance of the single IngestApiService via a copy of it's Mailbox.
pub fn get_ingest_api_service() -> Option<Mailbox<IngestApiService>> {
    INGEST_API_SERVICE_INSTANCE.get().cloned()
}

/// Creates a ingest API service actor.
pub fn spawn_ingest_api_actor(
    universe: &Universe,
    queue_path: &Path,
) -> anyhow::Result<Mailbox<IngestApiService>> {
    info!(queue_path=?queue_path, "Spawning ingest API actor");
    let ingest_api_actor = IngestApiService::with_queue_path(queue_path)?;
    let (ingest_api_mailbox, _ingest_api_handle) = universe.spawn_actor(ingest_api_actor).spawn();
    Ok(ingest_api_mailbox)
}

/// Adds a document raw bytes to a [`DocBatch`]
pub fn add_doc(payload: &[u8], fetch_resp: &mut DocBatch) -> usize {
    fetch_resp.concat_docs.extend_from_slice(payload);
    fetch_resp.doc_lens.push(payload.len() as u64);
    payload.len()
}

/// Returns an iterator over the document payloads within a doc_batch.
pub fn iter_doc_payloads(doc_batch: &DocBatch) -> impl Iterator<Item = &[u8]> {
    doc_batch
        .doc_lens
        .iter()
        .cloned()
        .scan(0, |current_offset, doc_num_bytes| {
            let start = *current_offset;
            let end = start + doc_num_bytes as usize;
            *current_offset = end;
            Some(&doc_batch.concat_docs[start..end])
        })
}
