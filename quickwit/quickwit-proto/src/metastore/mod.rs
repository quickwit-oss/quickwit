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

use crate::{IndexUid, SourceId, SplitId};

#[path = "../codegen/quickwit/quickwit.metastore.rs"]
mod codegen;

pub use codegen::metastore_service_client::MetastoreServiceClient;
pub use codegen::metastore_service_server::{MetastoreService, MetastoreServiceServer};
pub use codegen::*;

// Index API

impl IndexMetadataRequest {
    pub fn new(index_id: impl Into<String>) -> Self {
        Self {
            index_id: index_id.into(),
            incarnation_id: None,
        }
    }

    pub fn strict(index_uid: IndexUid) -> Self {
        Self {
            index_id: index_uid.index_id().to_string(),
            incarnation_id: Some(index_uid.incarnation_id().to_string()),
        }
    }

    pub fn index_uid(&self) -> Option<IndexUid> {
        self.incarnation_id
            .as_ref()
            .map(|incarnation_id| IndexUid::from_parts(&self.index_id, incarnation_id))
    }
}

impl DeleteIndexRequest {
    pub fn new(index_uid: impl Into<String>) -> Self {
        Self {
            index_uid: index_uid.into(),
        }
    }
}

impl ListIndexesResponse {
    pub fn num_indexes(&self) -> usize {
        self.indexes_metadata_json.len()
    }
}

// Source API

impl ResetSourceCheckpointRequest {
    pub fn new(index_uid: impl Into<String>, source_id: impl Into<SourceId>) -> Self {
        Self {
            index_uid: index_uid.into(),
            source_id: source_id.into(),
        }
    }
}

impl ToggleSourceRequest {
    pub fn new(index_uid: impl Into<String>, source_id: impl Into<SourceId>, enable: bool) -> Self {
        Self {
            index_uid: index_uid.into(),
            source_id: source_id.into(),
            enable,
        }
    }
}

impl DeleteSourceRequest {
    pub fn new(index_uid: impl Into<String>, source_id: impl Into<SourceId>) -> Self {
        Self {
            index_uid: index_uid.into(),
            source_id: source_id.into(),
        }
    }
}

// Split API

impl ListSplitsRequest {
    pub fn all(index_uid: impl Into<IndexUid>) -> Self {
        Self {
            index_uid: index_uid.into().into(),
            list_splits_query_json: None,
        }
    }
}

impl PublishSplitsRequest {
    pub fn new(
        index_uid: impl Into<String>,
        staged_split_ids: impl IntoIterator<Item = impl Into<SplitId>>,
        replaced_split_ids: impl IntoIterator<Item = impl Into<SplitId>>,
        checkpoint_delta: Option<SourceCheckpointDelta>,
    ) -> Self {
        Self {
            index_uid: index_uid.into(),
            staged_split_ids: staged_split_ids
                .into_iter()
                .map(|split_id| split_id.into())
                .collect(),
            replaced_split_ids: replaced_split_ids
                .into_iter()
                .map(|split_id| split_id.into())
                .collect(),
            checkpoint_delta,
        }
    }
}

impl MarkSplitsForDeletionRequest {
    pub fn new(
        index_uid: impl Into<String>,
        split_ids: impl IntoIterator<Item = impl Into<SplitId>>,
    ) -> Self {
        Self {
            index_uid: index_uid.into(),
            split_ids: split_ids
                .into_iter()
                .map(|split_id| split_id.into())
                .collect(),
        }
    }
}

impl DeleteSplitsRequest {
    pub fn new(
        index_uid: impl Into<String>,
        split_ids: impl IntoIterator<Item = impl Into<SplitId>>,
    ) -> Self {
        Self {
            index_uid: index_uid.into(),
            split_ids: split_ids
                .into_iter()
                .map(|split_id| split_id.into())
                .collect(),
        }
    }
}

// Delete task API

impl ListDeleteTasksRequest {
    pub fn new(index_uid: impl Into<String>, opstamp_start: u64) -> Self {
        Self {
            index_uid: index_uid.into(),
            opstamp_start,
        }
    }
}

impl UpdateSplitsDeleteOpstampRequest {
    pub fn new(
        index_uid: impl Into<String>,
        split_ids: impl IntoIterator<Item = impl Into<SplitId>>,
        delete_opstamp: u64,
    ) -> Self {
        Self {
            index_uid: index_uid.into(),
            split_ids: split_ids
                .into_iter()
                .map(|split_id| split_id.into())
                .collect(),
            delete_opstamp,
        }
    }
}
