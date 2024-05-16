// Copyright (C) 2024 Quickwit, Inc.
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
use quickwit_proto::error::grpc_error_to_grpc_status;
use quickwit_proto::metastore::{EntityKind, MetastoreError};
use quickwit_proto::{tonic, GrpcServiceError, ServiceError, ServiceErrorCode};
use quickwit_storage::StorageResolverError;
use serde::{Deserialize, Serialize};
use tantivy::TantivyError;
use thiserror::Error;
use tokio::task::JoinError;

/// Possible SearchError
#[allow(missing_docs)]
#[derive(Error, Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SearchError {
    #[error("could not find indexes matching the IDs `{index_ids:?}`")]
    IndexesNotFound { index_ids: Vec<String> },
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
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ServiceError for SearchError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::IndexesNotFound { .. } => ServiceErrorCode::NotFound,
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::InvalidAggregationRequest(_) => ServiceErrorCode::BadRequest,
            Self::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            Self::InvalidQuery(_) => ServiceErrorCode::BadRequest,
            Self::StorageResolver(_) => ServiceErrorCode::Internal,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for SearchError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        Self::TooManyRequests
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }
}

impl From<SearchError> for tonic::Status {
    fn from(error: SearchError) -> Self {
        grpc_error_to_grpc_status(error)
    }
}

/// Parse tonic error and returns `SearchError`.
pub fn parse_grpc_error(grpc_error: &tonic::Status) -> SearchError {
    // TODO: the serialization to JSON part is missing.
    serde_json::from_str(grpc_error.message())
        .unwrap_or_else(|_| SearchError::Internal(grpc_error.message().to_string()))
}

impl From<TantivyError> for SearchError {
    fn from(tantivy_error: TantivyError) -> Self {
        SearchError::Internal(format!("tantivy error: {tantivy_error}"))
    }
}

impl From<postcard::Error> for SearchError {
    fn from(error: postcard::Error) -> Self {
        SearchError::Internal(format!("Postcard error: {error}"))
    }
}

impl From<serde_json::Error> for SearchError {
    fn from(serde_error: serde_json::Error) -> Self {
        SearchError::Internal(format!("serde error: {serde_error}"))
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
                    index_ids: vec![index_id],
                }
            }
            MetastoreError::NotFound(EntityKind::Indexes { index_ids }) => {
                SearchError::IndexesNotFound { index_ids }
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
