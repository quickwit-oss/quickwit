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

use quickwit_doc_mapper::QueryParserError;
use quickwit_proto::metastore::{EntityKind, MetastoreError};
use quickwit_proto::{tonic, ServiceError, ServiceErrorCode};
use quickwit_storage::StorageResolverError;
use serde::{Deserialize, Serialize};
use tantivy::TantivyError;
use thiserror::Error;
use tokio::task::JoinError;

/// Possible SearchError
#[allow(missing_docs)]
#[derive(Error, Debug, Serialize, Deserialize, Clone)]
pub enum SearchError {
    #[error("could not find indexes matching the IDs or patterns `{index_id_patterns:?}`")]
    IndexesNotFound { index_id_patterns: Vec<String> },
    #[error("internal error: `{0}`")]
    Internal(String),
    #[error("invalid aggregation request: {0}")]
    InvalidAggregationRequest(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0}")]
    InvalidQuery(String),
    #[error("storage not found: `{0}`)")]
    StorageResolver(#[from] StorageResolverError),
}

impl ServiceError for SearchError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            SearchError::IndexesNotFound { .. } => ServiceErrorCode::NotFound,
            SearchError::Internal(_) => ServiceErrorCode::Internal,
            SearchError::InvalidAggregationRequest(_) => ServiceErrorCode::BadRequest,
            SearchError::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            SearchError::InvalidQuery(_) => ServiceErrorCode::BadRequest,
            SearchError::StorageResolver(_) => ServiceErrorCode::BadRequest,
        }
    }
}

impl From<SearchError> for tonic::Status {
    fn from(error: SearchError) -> Self {
        error.grpc_error()
    }
}

/// Parse tonic error and returns `SearchError`.
pub fn parse_grpc_error(grpc_error: &tonic::Status) -> SearchError {
    serde_json::from_str(grpc_error.message())
        .unwrap_or_else(|_| SearchError::Internal(grpc_error.message().to_string()))
}

impl From<TantivyError> for SearchError {
    fn from(tantivy_error: TantivyError) -> Self {
        SearchError::Internal(format!("Tantivy error: {tantivy_error}"))
    }
}

impl From<postcard::Error> for SearchError {
    fn from(error: postcard::Error) -> Self {
        SearchError::Internal(format!("Postcard error: {error}"))
    }
}

impl From<serde_json::Error> for SearchError {
    fn from(serde_error: serde_json::Error) -> Self {
        SearchError::Internal(format!("Serde error: {serde_error}"))
    }
}

impl From<anyhow::Error> for SearchError {
    fn from(any_error: anyhow::Error) -> Self {
        SearchError::Internal(any_error.to_string())
    }
}

impl From<QueryParserError> for SearchError {
    fn from(query_parser_error: QueryParserError) -> Self {
        SearchError::InvalidQuery(query_parser_error.to_string())
    }
}

impl From<MetastoreError> for SearchError {
    fn from(metastore_error: MetastoreError) -> SearchError {
        match metastore_error {
            MetastoreError::NotFound(EntityKind::Index { index_id }) => {
                SearchError::IndexesNotFound {
                    index_id_patterns: vec![index_id],
                }
            }
            MetastoreError::NotFound(EntityKind::Indexes { index_ids }) => {
                SearchError::IndexesNotFound {
                    index_id_patterns: index_ids,
                }
            }
            _ => SearchError::Internal(metastore_error.to_string()),
        }
    }
}

impl From<JoinError> for SearchError {
    fn from(join_error: JoinError) -> SearchError {
        SearchError::Internal(format!("spawned task in root join failed: {join_error}"))
    }
}

impl From<std::convert::Infallible> for SearchError {
    fn from(infallible: std::convert::Infallible) -> SearchError {
        match infallible {}
    }
}

#[derive(Debug)]
pub struct NodeSearchError {
    pub search_error: SearchError,
    pub split_ids: Vec<String>,
}
