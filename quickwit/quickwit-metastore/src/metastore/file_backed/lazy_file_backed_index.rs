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

use std::sync::{Arc, Weak};
use std::time::Duration;

use quickwit_proto::metastore::{EntityKind, MetastoreError, MetastoreResult};
use quickwit_proto::types::IndexId;
use quickwit_storage::Storage;
use tokio::sync::{Mutex, OnceCell};
use tracing::error;

use super::file_backed_index::FileBackedIndex;
use super::store_operations::{load_index, METASTORE_FILE_NAME};

/// Lazy [`FileBackedIndex`]. It loads a `FileBackedIndex` on demand. When the index is first
/// loaded, it optionally spawns a task to periodically poll the storage and update the index.
pub(crate) struct LazyFileBackedIndex {
    index_id: IndexId,
    storage: Arc<dyn Storage>,
    polling_interval_opt: Option<Duration>,
    lazy_index: OnceCell<Arc<Mutex<FileBackedIndex>>>,
}

impl LazyFileBackedIndex {
    /// Create `LazyFileBackedIndex`.
    pub fn new(
        storage: Arc<dyn Storage>,
        index_id: IndexId,
        polling_interval_opt: Option<Duration>,
        file_backed_index: Option<FileBackedIndex>,
    ) -> Self {
        let index_mutex_opt = file_backed_index.map(|index| Arc::new(Mutex::new(index)));
        // If the polling interval is configured and the index is already loaded,
        // spawn immediately the polling task
        if let Some(index_mutex) = &index_mutex_opt {
            if let Some(polling_interval) = polling_interval_opt {
                spawn_index_metadata_polling_task(
                    storage.clone(),
                    index_id.clone(),
                    Arc::downgrade(index_mutex),
                    polling_interval,
                );
            }
        }
        Self {
            index_id,
            storage,
            polling_interval_opt,
            lazy_index: OnceCell::new_with(index_mutex_opt),
        }
    }

    /// Gets a synchronized `FileBackedIndex`. If the index wasn't provided on creation, we load it
    /// lazily on the first call of this method.
    pub async fn get(&self) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
        self.lazy_index
            .get_or_try_init(|| async move {
                let index = load_index(&*self.storage, &self.index_id).await?;
                let index_mutex = Arc::new(Mutex::new(index));
                // When the index is loaded lazily, the polling task is not started in the
                // constructor so we do it here when the index is actually loaded.
                if let Some(polling_interval) = self.polling_interval_opt {
                    spawn_index_metadata_polling_task(
                        self.storage.clone(),
                        self.index_id.clone(),
                        Arc::downgrade(&index_mutex),
                        polling_interval,
                    );
                }
                Ok(index_mutex)
            })
            .await
            .cloned()
    }
}

async fn poll_index_metadata_once(
    storage: &dyn Storage,
    index_id: &str,
    index_mutex: &Mutex<FileBackedIndex>,
) {
    let mut locked_index = index_mutex.lock().await;
    if locked_index.flip_recently_modified_down() {
        return;
    }
    let load_index_result = load_index(storage, index_id).await;

    match load_index_result {
        Ok(index) => {
            *locked_index = index;
        }
        Err(MetastoreError::NotFound(EntityKind::Index { .. })) => {
            // The index has been deleted by the file-backed metastore holding a reference to this
            // index. When it removes an index, it does so without holding the lock on the target
            // index. As a result, the associated polling task may run for one
            // more iteration before exiting and `load_index` returns a `NotFound` error.
        }
        Err(metastore_error) => {
            error!(
                error=%metastore_error,
                "failed to load index metadata from metastore file located at `{}/{index_id}/{METASTORE_FILE_NAME}`",
                storage.uri()
            );
        }
    }
}

fn spawn_index_metadata_polling_task(
    storage: Arc<dyn Storage>,
    index_id: IndexId,
    metastore_weak: Weak<Mutex<FileBackedIndex>>,
    polling_interval: Duration,
) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(polling_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; //< this is to prevent fetch right after the first population of the data.

        while let Some(metadata_mutex) = metastore_weak.upgrade() {
            interval.tick().await;
            poll_index_metadata_once(&*storage, &index_id, &metadata_mutex).await;
        }
    });
}
