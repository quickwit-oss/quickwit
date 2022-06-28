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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use quickwit_storage::{Storage, StorageError, StorageErrorKind};
use serde::{Deserialize, Serialize};

use super::{IndexState, LazyFileBackedIndex};
use crate::metastore::file_backed_metastore::file_backed_index::FileBackedIndex;
use crate::{MetastoreError, MetastoreResult};

/// Indexes states file managed by [`FileBackedMetastore`](crate::FileBackedMetastore).
const INDEXES_STATES_FILENAME: &str = "indexes_states.json";

/// Index metadata file managed by [`FileBackedMetastore`](crate::FileBackedMetastore).
const META_FILENAME: &str = "metastore.json";

/// Index state used for serialization/deserialization only.
#[derive(Serialize, Deserialize)]
enum IndexStateValue {
    Creating,
    Alive,
    Deleting,
}

impl From<&IndexState> for IndexStateValue {
    fn from(index_state: &IndexState) -> IndexStateValue {
        match index_state {
            IndexState::Creating => IndexStateValue::Creating,
            IndexState::Deleting => IndexStateValue::Deleting,
            IndexState::Alive(_) => IndexStateValue::Alive,
        }
    }
}

/// Path to the metadata file from the given index ID.
pub(crate) fn meta_path(index_id: &str) -> PathBuf {
    Path::new(index_id).join(META_FILENAME)
}

fn convert_error(index_id: &str, storage_err: StorageError) -> MetastoreError {
    match storage_err.kind() {
        StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
            index_id: index_id.to_string(),
        },
        StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
            message: "The request credentials do not allow for this operation.".to_string(),
        },
        _ => MetastoreError::InternalError {
            message: "Failed to get index files.".to_string(),
            cause: anyhow::anyhow!(storage_err),
        },
    }
}

/// Fetch `INDEXES_STATES_FILENAME` file and build the map (index, state).
/// If the file does not exist, return an empty map.
pub(crate) async fn fetch_and_build_indexes_states(
    storage: Arc<dyn Storage>,
    polling_interval_opt: Option<Duration>,
) -> MetastoreResult<HashMap<String, IndexState>> {
    let indexes_list_path = Path::new(INDEXES_STATES_FILENAME);
    let exists = storage
        .exists(indexes_list_path)
        .await
        .map_err(|storage_err| convert_error("indexes", storage_err))?;
    if !exists {
        return Ok(HashMap::default());
    }
    let content = storage
        .get_all(indexes_list_path)
        .await
        .map_err(|storage_err| MetastoreError::InternalError {
            message: format!("Failed to get {INDEXES_STATES_FILENAME} file."),
            cause: anyhow::anyhow!(storage_err),
        })?;
    let indexes_states_deserialized: HashMap<String, IndexStateValue> =
        serde_json::from_slice(&content[..])
            .map_err(|serde_err| MetastoreError::InvalidManifest { cause: serde_err })?;
    Ok(indexes_states_deserialized
        .into_iter()
        .map(|(index_id, index_state)| match index_state {
            IndexStateValue::Creating => (index_id, IndexState::Creating),
            IndexStateValue::Deleting => (index_id, IndexState::Deleting),
            IndexStateValue::Alive => {
                let lazy_index = LazyFileBackedIndex::new(
                    storage.clone(),
                    index_id.clone(),
                    polling_interval_opt,
                    None,
                );
                (index_id, IndexState::Alive(lazy_index))
            }
        })
        .collect())
}

pub(crate) async fn put_indexes_states(
    storage: &dyn Storage,
    indexes_states: &HashMap<String, IndexState>,
) -> MetastoreResult<()> {
    let indexes_states_serializable: HashMap<String, IndexStateValue> = indexes_states
        .iter()
        .map(|(index_id, index_state)| (index_id.clone(), IndexStateValue::from(index_state)))
        .collect();
    let indexes_list_path = Path::new(INDEXES_STATES_FILENAME);
    let content: Vec<u8> =
        serde_json::to_vec_pretty(&indexes_states_serializable).map_err(|serde_err| {
            MetastoreError::InternalError {
                message: "Failed to serialize indexes map".to_string(),
                cause: anyhow::anyhow!(serde_err),
            }
        })?;
    storage
        .put(indexes_list_path, Box::new(content))
        .await
        .map_err(|storage_err| MetastoreError::InternalError {
            message: "Failed to put {INDEXES_STATES_FILENAME} file.".to_string(),
            cause: anyhow::anyhow!(storage_err),
        })?;
    Ok(())
}

pub(crate) async fn fetch_index(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<FileBackedIndex> {
    let metadata_path = meta_path(index_id);
    let content = storage
        .get_all(&metadata_path)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

    let index: FileBackedIndex = serde_json::from_slice(&content[..])
        .map_err(|serde_err| MetastoreError::InvalidManifest { cause: serde_err })?;

    if index.index_id() != index_id {
        return Err(MetastoreError::InternalError {
            message: "Inconsistent manifest: index_id mismatch.".to_string(),
            cause: anyhow::anyhow!(
                "Expected index_id `{}`, but found `{}`",
                index_id,
                index.index_id()
            ),
        });
    }
    Ok(index)
}

pub(crate) async fn index_exists(storage: &dyn Storage, index_id: &str) -> MetastoreResult<bool> {
    let metadata_path = meta_path(index_id);
    let exists = storage
        .exists(&metadata_path)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;
    Ok(exists)
}

/// Serializes the `Index` object and stores the data on the storage.
///
/// Do not call this method. Instead, call `put_index`.
/// The point of having two methods here is just to make it usable in a unit test.
pub(crate) async fn put_index_given_index_id(
    storage: &dyn Storage,
    index: &FileBackedIndex,
    index_id: &str,
) -> MetastoreResult<()> {
    // Serialize Index.
    let content: Vec<u8> =
        serde_json::to_vec_pretty(&index).map_err(|serde_err| MetastoreError::InternalError {
            message: "Failed to serialize Metadata set".to_string(),
            cause: anyhow::anyhow!(serde_err),
        })?;

    let metadata_path = meta_path(index_id);
    // Put data back into storage.
    storage
        .put(&metadata_path, Box::new(content))
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;
    Ok(())
}

/// Serializes the `Index` object and stores the data on the storage.
pub(crate) async fn put_index(
    storage: &dyn Storage,
    index: &FileBackedIndex,
) -> MetastoreResult<()> {
    put_index_given_index_id(storage, index, index.index_id()).await
}

/// Serializes the Index and stores the data on the storage.
pub(crate) async fn delete_index(storage: &dyn Storage, index_id: &str) -> MetastoreResult<()> {
    let metadata_path = meta_path(index_id);

    let file_exists = storage
        .exists(&metadata_path)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

    if !file_exists {
        return Err(MetastoreError::IndexDoesNotExist {
            index_id: index_id.to_string(),
        });
    }

    // Put data back into storage.
    storage
        .delete(&metadata_path)
        .await
        .map_err(|storage_err| match storage_err.kind() {
            StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                message: "The request credentials do not allow for this operation.".to_string(),
            },
            _ => MetastoreError::InternalError {
                message: format!(
                    "Failed to write metastore file to `{}`.",
                    metadata_path.display()
                ),
                cause: anyhow::anyhow!(storage_err),
            },
        })?;

    Ok(())
}
