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

use std::path::{Path, PathBuf};

use quickwit_proto::metastore::{serde_utils, EntityKind, MetastoreError, MetastoreResult};
use quickwit_storage::{Storage, StorageError, StorageErrorKind};

use crate::metastore::file_backed::file_backed_index::FileBackedIndex;

/// Index metastore file managed by [`FileBackedMetastore`](crate::FileBackedMetastore).
pub(super) const METASTORE_FILE_NAME: &str = "metastore.json";

/// Path to the metadata file from the given index ID.
pub(super) fn metastore_filepath(index_id: &str) -> PathBuf {
    Path::new(index_id).join(METASTORE_FILE_NAME)
}

fn convert_error(index_id: &str, storage_error: StorageError) -> MetastoreError {
    match storage_error.kind() {
        StorageErrorKind::NotFound => MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }),
        StorageErrorKind::Unauthorized => MetastoreError::Forbidden(
            "the request credentials do not allow for this operation".to_string(),
        ),
        _ => MetastoreError::Internal("".to_string()),
    }
}

pub(super) async fn load_index(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<FileBackedIndex> {
    let metastore_filepath = metastore_filepath(index_id);

    let content = storage
        .get_all(&metastore_filepath)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

    let index: FileBackedIndex = serde_utils::from_json_bytes(&content)?;

    if index.index_id() != index_id {
        return Err(MetastoreError::Internal("".to_string()));
    }
    Ok(index)
}

pub(super) async fn index_exists(storage: &dyn Storage, index_id: &str) -> MetastoreResult<bool> {
    let metastore_filepath = metastore_filepath(index_id);
    let exists = storage
        .exists(&metastore_filepath)
        .await
        .map_err(|storage_error| convert_error(index_id, storage_error))?;
    Ok(exists)
}

/// Serializes the `Index` object and stores the data on the storage.
///
/// Do not call this method. Instead, call `put_index`.
/// The point of having two methods here is just to make it usable in a unit test.
pub(super) async fn put_index_given_index_id(
    storage: &dyn Storage,
    index: &FileBackedIndex,
    index_id: &str,
) -> MetastoreResult<()> {
    // Serialize Index.
    let content: Vec<u8> = serde_utils::to_json_bytes_pretty(index)?;
    let metastore_filepath = metastore_filepath(index_id);
    // Put data back into storage.
    storage
        .put(&metastore_filepath, Box::new(content))
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;
    Ok(())
}

/// Serializes the `Index` object and stores the data on the storage.
pub(super) async fn put_index(
    storage: &dyn Storage,
    index: &FileBackedIndex,
) -> MetastoreResult<()> {
    put_index_given_index_id(storage, index, index.index_id()).await
}

/// Serializes the Index and stores the data on the storage.
pub(super) async fn delete_index(storage: &dyn Storage, index_id: &str) -> MetastoreResult<()> {
    let metastore_filepath = metastore_filepath(index_id);

    let file_exists = storage
        .exists(&metastore_filepath)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

    if !file_exists {
        return Err(MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }));
    }
    // Put data back into storage.
    storage
        .delete(&metastore_filepath)
        .await
        .map_err(|storage_error| match storage_error.kind() {
            StorageErrorKind::Unauthorized => MetastoreError::Forbidden(
                "the request credentials do not allow for this operation".to_string(),
            ),
            _ => MetastoreError::Internal("foo".to_string()),
        })?;
    Ok(())
}
