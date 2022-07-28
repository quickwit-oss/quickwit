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

mod actors;
mod client;
mod error;
mod grpc_adapter;
mod test_utils;
use std::sync::Arc;

pub use actors::IndexManagementService;
pub use client::{create_index_management_client, IndexManagementClient};
pub use error::IndexManagementError;
pub use test_utils::create_index_management_client_for_test;
pub use grpc_adapter::GrpcIndexManagementServiceAdapter;
use quickwit_actors::{Mailbox, Universe};
use quickwit_common::uri::Uri;
use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::FileEntry;
use quickwit_storage::StorageUriResolver;

/// Starting [`IndexManagementService`].
pub async fn start_control_plane_service(
    universe: &Universe,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    default_index_root_uri: Uri,
) -> anyhow::Result<Mailbox<IndexManagementService>> {
    let index_service =
        IndexManagementService::new(metastore, storage_resolver, default_index_root_uri);
    let (index_service_mailbox, _) = universe.spawn_actor(index_service).spawn();
    Ok(index_service_mailbox)
}

// TODO: add docs.
pub fn split_metadata_to_file_entry(split_metadata: &SplitMetadata) -> FileEntry {
    FileEntry {
        file_name: quickwit_common::split_file(split_metadata.split_id()),
        file_size_in_bytes: split_metadata.footer_offsets.end,
    }
}
