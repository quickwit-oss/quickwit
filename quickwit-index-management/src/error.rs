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

use quickwit_metastore::MetastoreError;
use quickwit_storage::{StorageError, StorageResolverError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexManagementError {
    #[error("Failed to resolve the storage `{0}`.")]
    StorageResolverError(#[from] StorageResolverError),
    #[error("Failed to delete splits from storage: '{0:?}'.")]
    StorageError(Vec<(String, StorageError)>),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
    #[error("Invalid index config: {0}.")]
    InvalidIndexConfig(String),
    #[error("Internal error `{0}`.")]
    InternalError(String),
}
