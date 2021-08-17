/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use serde::{Deserialize, Serialize};
use tantivy::TantivyError;
use thiserror::Error;
use tokio::{sync::AcquireError, task::JoinError};

use quickwit_index_config::QueryParserError;
use quickwit_metastore::MetastoreError;
use quickwit_storage::StorageResolverError;

/// Possible SearchError
#[allow(missing_docs)]
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum SearchError {
    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist { index_id: String },
    #[error("Internal error: `{0}`.")]
    InternalError(String),
    #[error("Ressources exhausted: `{0}`.")]
    RessourcesExhausted(String),
    #[error("Storage not found: `{0}`)")]
    StorageResolverError(#[from] StorageResolverError),
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}

impl SearchError {
    fn convert_to_tonic_status_code(search_error: &SearchError) -> tonic::Code {
        match search_error {
            SearchError::IndexDoesNotExist { .. } => tonic::Code::NotFound,
            SearchError::InternalError(_) => tonic::Code::Internal,
            SearchError::StorageResolverError(_) => tonic::Code::Internal,
            SearchError::InvalidQuery(_) => tonic::Code::InvalidArgument,
            SearchError::RessourcesExhausted(_) => tonic::Code::ResourceExhausted,
        }
    }

    /// Convert quickwit search error to tonic status.
    pub fn convert_to_tonic_status(search_error: SearchError) -> tonic::Status {
        let error_json = serde_json::to_string_pretty(&search_error)
            .unwrap_or_else(|_| "Failed to serialize error".to_string());
        let code = SearchError::convert_to_tonic_status_code(&search_error);
        tonic::Status::new(code, error_json)
    }
}

pub fn parse_grpc_error(grpc_error: &tonic::Status) -> SearchError {
    match serde_json::from_str(grpc_error.message()) {
        Ok(search_error) => search_error,
        Err(_) => SearchError::InternalError(grpc_error.message().to_string()),
    }
}

impl From<TantivyError> for SearchError {
    fn from(tantivy_err: TantivyError) -> Self {
        SearchError::InternalError(format!("{}", tantivy_err))
    }
}

impl From<anyhow::Error> for SearchError {
    fn from(any_err: anyhow::Error) -> Self {
        SearchError::InternalError(format!("{}", any_err))
    }
}

impl From<QueryParserError> for SearchError {
    fn from(query_parser_error: QueryParserError) -> Self {
        SearchError::InvalidQuery(format!("{}", query_parser_error))
    }
}

impl From<MetastoreError> for SearchError {
    fn from(metastore_error: MetastoreError) -> SearchError {
        match metastore_error {
            MetastoreError::IndexDoesNotExist { index_id } => {
                SearchError::IndexDoesNotExist { index_id }
            }
            _ => SearchError::InternalError(format!("{}", metastore_error)),
        }
    }
}

impl From<JoinError> for SearchError {
    fn from(join_error: JoinError) -> SearchError {
        SearchError::InternalError(format!("Spawned task in root join failed: {}", join_error))
    }
}

impl From<AcquireError> for SearchError {
    fn from(_: AcquireError) -> SearchError {
        SearchError::RessourcesExhausted(String::from(
            "could not acquire ressource to complete leaf search",
        ))
    }
}
