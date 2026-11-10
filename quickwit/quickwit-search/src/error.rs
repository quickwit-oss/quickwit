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

use std::io;

use itertools::Itertools;
use quickwit_common::rate_limited_error;
use quickwit_common::retry::Retryable;
use quickwit_doc_mapper::QueryParserError;
use quickwit_proto::error::grpc_error_to_grpc_status;
use quickwit_proto::metastore::{EntityKind, MetastoreError};
use quickwit_proto::search::SplitSearchError;
use quickwit_proto::{GrpcServiceError, ServiceError, ServiceErrorCode, tonic};
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
    /// The split search would have gone over a warmup memory limit.
    #[error("{0}")]
    OutOfWarmupMemory(String),
}

impl SearchError {
    /// Whether searching the failed split on another searcher could succeed.
    ///
    /// Not to be confused with [`Retryable::is_retryable`], which tells whether the same request
    /// can be retried after a backoff.
    pub fn is_retryable_on_another_searcher(&self) -> bool {
        match self {
            // A split search that ran out of warmup memory would likely run out of memory again
            // on another searcher, and make that searcher's other searches fail too.
            SearchError::OutOfWarmupMemory(_) => false,
            // The request and the indexes are the same on every searcher, so these errors would
            // happen again.
            SearchError::IndexesNotFound { .. }
            | SearchError::InvalidAggregationRequest(_)
            | SearchError::InvalidArgument(_)
            | SearchError::InvalidQuery(_) => false,
            SearchError::Internal(_)
            | SearchError::StorageResolver(_)
            | SearchError::Timeout(_)
            | SearchError::TooManyRequests
            | SearchError::Unavailable(_) => true,
        }
    }

    /// Prefixes the error message with `context`, keeping the variant.
    ///
    /// Only `Internal` and `OutOfWarmupMemory` get the context. The other variants are returned
    /// unchanged.
    pub fn with_context(self, context: &str) -> SearchError {
        match self {
            SearchError::Internal(message) => {
                SearchError::Internal(format!("{context}: {message}"))
            }
            SearchError::OutOfWarmupMemory(message) => {
                SearchError::OutOfWarmupMemory(format!("{context}: {message}"))
            }
            other => other,
        }
    }

    /// Creates an internal `SearchError` from a list of split search errors.
    pub fn from_split_errors(failed_splits: &[SplitSearchError]) -> Option<SearchError> {
        let first_failing_split = failed_splits.first()?;
        let failed_splits = failed_splits
            .iter()
            .map(|failed_split| &failed_split.split_id)
            .join(", ");
        let error_msg = format!(
            "search failed for the following splits: {failed_splits:}. For instance, split {} \
             failed with the following error message: {}",
            first_failing_split.split_id, first_failing_split.error,
        );
        Some(SearchError::Internal(error_msg))
    }
}

impl ServiceError for SearchError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::IndexesNotFound { .. } => ServiceErrorCode::NotFound,
            Self::Internal(error_msg) => {
                rate_limited_error!(limit_per_min = 6, "search internal error: {error_msg}");
                ServiceErrorCode::Internal
            }
            Self::InvalidAggregationRequest(_) => ServiceErrorCode::BadRequest,
            Self::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            Self::InvalidQuery(_) => ServiceErrorCode::BadRequest,
            Self::StorageResolver(storage_err) => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "search's storager resolver internal error: {storage_err}"
                );
                ServiceErrorCode::Internal
            }
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
            Self::OutOfWarmupMemory(_) => ServiceErrorCode::Internal,
        }
    }
}

impl quickwit_common::tower::GrpcStatusCode for SearchError {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
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
    serde_json::from_str(grpc_error.message()).unwrap_or_else(|_| {
        SearchError::Internal(format!(
            "tonic error: {} ({})",
            grpc_error.message(),
            grpc_error.code()
        ))
    })
}

impl From<TantivyError> for SearchError {
    fn from(tantivy_error: TantivyError) -> Self {
        // Reading data can run out of warmup memory, for instance when opening the inverted index
        // of a field.
        if let TantivyError::IoError(io_error) = &tantivy_error
            && io_error.kind() == io::ErrorKind::OutOfMemory
        {
            return SearchError::OutOfWarmupMemory(io_error.to_string());
        }
        SearchError::Internal(format!("tantivy error: {tantivy_error}"))
    }
}

/// Hidden contract: in the search path, only the `CachingDirectory` returns
/// `io::ErrorKind::OutOfMemory`, when the split search would go over a warmup memory limit. Its
/// message tells which limit. If another source ever returned that kind, the split would not be
/// retried either, which is fine for a memory problem.
impl From<io::Error> for SearchError {
    fn from(io_error: io::Error) -> Self {
        if io_error.kind() == io::ErrorKind::OutOfMemory {
            return SearchError::OutOfWarmupMemory(io_error.to_string());
        }
        SearchError::Internal(format!("io error: {io_error}"))
    }
}

impl From<tokio::time::error::Elapsed> for SearchError {
    fn from(_elapsed: tokio::time::error::Elapsed) -> Self {
        SearchError::Timeout("timeout exceeded".to_string())
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
        // The alternate format includes the whole chain of causes, not only the outermost
        // context. For instance: "failed to load automaton: out of warmup memory".
        SearchError::Internal(format!("{any_error:#}"))
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

impl Retryable for SearchError {
    fn is_retryable(&self) -> bool {
        matches!(self, SearchError::TooManyRequests | SearchError::Timeout(_))
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

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use quickwit_directories::OutOfMemoryError;
    use tantivy::TantivyError;

    use super::SearchError;

    #[test]
    fn test_out_of_memory_io_error_is_not_retryable() {
        let io_error = io::Error::from(OutOfMemoryError::SplitBudgetExceeded);
        let search_error = SearchError::from(io_error);
        let SearchError::OutOfWarmupMemory(message) = &search_error else {
            panic!("expected OutOfWarmupMemory, got {search_error:?}");
        };
        assert_eq!(message, "out of warmup memory: split budget exceeded");
        assert!(!search_error.is_retryable_on_another_searcher());
    }

    #[test]
    fn test_other_io_error_is_retryable() {
        let io_error = io::Error::new(io::ErrorKind::ConnectionReset, "connection reset");
        let search_error = SearchError::from(io_error);
        assert!(matches!(search_error, SearchError::Internal(_)));
        assert!(search_error.is_retryable_on_another_searcher());
    }

    #[test]
    fn test_out_of_memory_tantivy_error_is_not_retryable() {
        let io_error = io::Error::from(OutOfMemoryError::GlobalBudgetExceeded);
        let tantivy_error = TantivyError::IoError(Arc::new(io_error));
        let search_error = SearchError::from(tantivy_error);
        let SearchError::OutOfWarmupMemory(message) = &search_error else {
            panic!("expected OutOfWarmupMemory, got {search_error:?}");
        };
        assert_eq!(message, "out of warmup memory: global budget exceeded");
        assert!(!search_error.is_retryable_on_another_searcher());

        let tantivy_error = TantivyError::InternalError("boom".to_string());
        let search_error = SearchError::from(tantivy_error);
        assert!(matches!(search_error, SearchError::Internal(_)));
        assert!(search_error.is_retryable_on_another_searcher());
    }

    #[test]
    fn test_with_context_keeps_the_variant() {
        let search_error =
            SearchError::from(io::Error::from(OutOfMemoryError::SplitBudgetExceeded))
                .with_context("failed to load automaton");
        let SearchError::OutOfWarmupMemory(message) = &search_error else {
            panic!("expected OutOfWarmupMemory, got {search_error:?}");
        };
        assert_eq!(
            message,
            "failed to load automaton: out of warmup memory: split budget exceeded"
        );

        let search_error =
            SearchError::Internal("boom".to_string()).with_context("failed to load automaton");
        let SearchError::Internal(message) = &search_error else {
            panic!("expected Internal, got {search_error:?}");
        };
        assert_eq!(message, "failed to load automaton: boom");

        let search_error = SearchError::InvalidQuery("bad".to_string()).with_context("ignored");
        let SearchError::InvalidQuery(message) = &search_error else {
            panic!("expected InvalidQuery, got {search_error:?}");
        };
        assert_eq!(message, "bad");
    }
}
