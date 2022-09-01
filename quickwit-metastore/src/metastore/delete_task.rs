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

use quickwit_proto::metastore_api::{DeleteQueryRequest, DeleteTaskResponse};
use serde::{Deserialize, Serialize};

/// A delete query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteQuery {
    /// Index ID.
    pub index_id: String,
    /// If set, restrict search to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restrict search to documents with a `timestamp < end_timestamp``.
    pub end_timestamp: Option<i64>,
    /// Query text. The query language is that of tantivy.
    pub query: String,
    /// Search fields.
    pub search_fields: Vec<String>,
}

/// A delete task.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteTask {
    /// Create timestamp.
    pub create_timestamp: i64,
    /// Opstamp.
    pub opstamp: u64,
    /// Delete query.
    pub delete_query: DeleteQuery,
}

impl From<DeleteQuery> for DeleteQueryRequest {
    fn from(delete_query: DeleteQuery) -> Self {
        Self {
            index_id: delete_query.index_id,
            start_timestamp: delete_query.start_timestamp,
            end_timestamp: delete_query.end_timestamp,
            query: delete_query.query,
            search_fields: delete_query.search_fields,
        }
    }
}

impl From<DeleteQueryRequest> for DeleteQuery {
    fn from(delete_query: DeleteQueryRequest) -> Self {
        Self {
            index_id: delete_query.index_id,
            start_timestamp: delete_query.start_timestamp,
            end_timestamp: delete_query.end_timestamp,
            query: delete_query.query,
            search_fields: delete_query.search_fields,
        }
    }
}

impl From<DeleteTaskResponse> for DeleteTask {
    fn from(delete_task: DeleteTaskResponse) -> Self {
        let delete_query = delete_task
            .delete_query
            .expect("DeleteTaskProto must have a delete query.");
        Self {
            create_timestamp: delete_task.create_timestamp,
            opstamp: delete_task.opstamp,
            delete_query: delete_query.into(),
        }
    }
}

impl From<DeleteTask> for DeleteTaskResponse {
    fn from(delete_task: DeleteTask) -> Self {
        let delete_query = delete_task.delete_query.into();
        Self {
            create_timestamp: delete_task.create_timestamp,
            opstamp: delete_task.opstamp,
            delete_query: Some(delete_query),
        }
    }
}
