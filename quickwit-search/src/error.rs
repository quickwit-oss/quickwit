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
use quickwit_doc_mapping::QueryParserError;
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
