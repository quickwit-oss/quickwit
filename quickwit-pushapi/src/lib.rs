// Copyright (C) 2021 Quickwit, Inc.
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
mod oltp;
mod position;
mod push_api_service;
mod queue;

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
pub use errors::PushApiError;
use errors::Result;
pub use oltp::OltpService;
use once_cell::sync::OnceCell;
pub use position::Position;
use queue::Queues;
use quickwit_actors::{Mailbox, Universe};
use quickwit_metastore::Metastore;
use quickwit_proto::push_api::DocBatch;
use tracing::info;

pub use crate::push_api_service::PushApiService;

pub static PUSH_API_SERVICE_INSTANCE: OnceCell<Mailbox<PushApiService>> = OnceCell::new();

/// Initializes the PushApiService single instance.
pub fn init_push_api(
    universe: &Universe,
    queue_path: &Path,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<Mailbox<PushApiService>> {
    let push_api_service = PUSH_API_SERVICE_INSTANCE
        .get_or_try_init(|| spawn_push_api_actor(universe, queue_path, metastore))
        .context("Failed to initialize the PushApi")?;
    Ok(push_api_service.clone())
}

/// Gets the instance of the single PushApiService via a copy of it's Mailbox.
pub fn get_push_api_service() -> Option<Mailbox<PushApiService>> {
    PUSH_API_SERVICE_INSTANCE.get().cloned()
}

/// Creates a push api service actor.
pub fn spawn_push_api_actor(
    universe: &Universe,
    queue_path: &Path,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<Mailbox<PushApiService>> {
    info!(queue_path=?queue_path, "Spawning push api actor");
    let push_api_actor = PushApiService::with_queue_path(queue_path, metastore)?;
    let (push_api_mailbox, _push_api_handle) = universe.spawn_actor(push_api_actor).spawn();
    Ok(push_api_mailbox)
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
