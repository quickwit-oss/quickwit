use std::convert::TryFrom;

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
use quickwit_index_config::QueryParserError;
use quickwit_metastore::MetastoreError;
use quickwit_storage::StorageResolverError;
use thiserror::Error;

/// Possible SearchError
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum SearchError {
    #[error("IndexDoesNotExist(`{index_id}`)")]
    IndexDoesNotExist { index_id: String },
    #[error("InternalError(`{0}`)")]
    InternalError(#[from] anyhow::Error),
    #[error("StorageResolverError(`{0}`)")]
    StorageResolverError(#[from] StorageResolverError),
    #[error("InvalidQuery(`{0}`)")]
    InvalidQuery(#[from] QueryParserError),
}

impl From<MetastoreError> for SearchError {
    fn from(metastore_error: MetastoreError) -> SearchError {
        match metastore_error {
            MetastoreError::IndexDoesNotExist { index_id } => {
                SearchError::IndexDoesNotExist { index_id }
            }
            _ => SearchError::InternalError(From::from(metastore_error)),
        }
    }
}

impl From<&str> for SearchError {
    /// Deserialize error message return by tonic into a SearchError.
    // TODO: this looks like a hack with random strings put here to choose the right search error.
    // These strings are actually defiend in function `convert_error_to_tonic_status`, we need
    // to make all this consistent and not error prone.
    fn from(message: &str) -> SearchError {
        if message.starts_with("Index not found: ") {
            message
                .split_once("Index not found: ")
                .map(|(_, index)| SearchError::IndexDoesNotExist {
                    index_id: index.to_owned(),
                })
                .unwrap()
        } else if message.starts_with("Internal error: ") {
            message
                .split_once("Internal error: ")
                .map(|(_, error)| SearchError::InternalError(anyhow::anyhow!(error.to_owned())))
                .unwrap()
        } else if message.starts_with("Storage resolver error: ") {
            message
                .split_once("Storage resolver error: ")
                .map(|(_, error)| StorageResolverError::try_from(error))
                .unwrap()
                .map(SearchError::StorageResolverError)
                .unwrap_or_else(|_| SearchError::InternalError(anyhow::anyhow!(message.to_owned())))
        } else if message.starts_with("Invalid query: ") {
            message
                .split_once("Invalid query: ")
                .map(|(_, error)| {
                    let message = error
                        .split_once("QueryParserError(")
                        .map(|(_, suffix)| suffix.strip_suffix(")"))
                        .flatten()
                        .unwrap_or(error);
                    SearchError::InvalidQuery(QueryParserError::from(anyhow::anyhow!(
                        message.to_owned()
                    )))
                })
                .unwrap()
        } else {
            SearchError::InternalError(anyhow::anyhow!(message.to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_invalid_query_message() {
        let message = "Invalid query: QueryParserError(Field does not exists: '\"myfield\"')";
        let error: SearchError = SearchError::from(message);
        match error {
            SearchError::InvalidQuery(error) => {
                assert_eq!(
                    error.to_string(),
                    "QueryParserError(`Field does not exists: '\"myfield\"'`)"
                );
            }
            _ => panic!("deserialize wrong search error type"),
        }
    }

    #[test]
    fn test_deserialize_index_not_found() {
        let message = "Index not found: my index";
        let error: SearchError = SearchError::from(message);
        match error {
            SearchError::IndexDoesNotExist { index_id } => {
                assert_eq!(index_id, "my index");
            }
            _ => panic!("deserialize wrong search error type"),
        }
    }
}
