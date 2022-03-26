// Copyright (C) 2021 Quickwit, Inc.
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

//! [`FileBackedIndex`] module. It is public so that the crate `quickwit-backward-compat` can
//! import [`FiledBackedIndex`] and run backward-compatibility tests. You should not have to import
//! anything from here directly.

use std::sync::{Arc, Weak};
use std::time::Duration;

use quickwit_storage::Storage;
use tokio::sync::{Mutex, OnceCell};
use tracing::error;

use super::file_backed_index::FileBackedIndex;
use super::store_operations::fetch_index;
use crate::MetastoreResult;

/// Lazy `FileBackedIndex`. It loads a `FileBackedIndex`
/// on demand and optionaly spawns a task to poll
/// regularly the storage and update the index.
pub struct LazyFileBackedIndex {
    index_id: String,
    storage: Arc<dyn Storage>,
    polling_interval_opt: Option<Duration>,
    lazy_index: OnceCell<Arc<Mutex<FileBackedIndex>>>,
}

impl LazyFileBackedIndex {
    /// Create `LazyFileBackedIndex`.
    pub fn new(
        storage: Arc<dyn Storage>,
        index_id: String,
        polling_interval_opt: Option<Duration>,
        file_backed_index: Option<FileBackedIndex>,
    ) -> Self {
        let index_mutex_opt = file_backed_index.map(|index| Arc::new(Mutex::new(index)));
        // If an index is given and a polling interval is given,
        // spawn immediately the polling task.
        if let Some(index_mutex) = index_mutex_opt.as_ref() {
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

    /// Get `FileBackedIndex`.
    pub async fn get(&self) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
        self.lazy_index
            .get_or_try_init(|| {
                load_file_backed_index(
                    self.storage.clone(),
                    self.index_id.clone(),
                    self.polling_interval_opt,
                )
            })
            .await
            .map(|index| index.clone())
    }
}

async fn poll_index_metadata_once(
    storage: &dyn Storage,
    index_id: &str,
    metadata_mutex: &Mutex<FileBackedIndex>,
) {
    let index_fetch_res = fetch_index(&*storage, index_id).await;
    match index_fetch_res {
        Ok(index) => {
            *metadata_mutex.lock().await = index;
        }
        Err(fetch_error) => {
            error!(error=?fetch_error, "fetch-metadata-error");
        }
    }
}

fn spawn_index_metadata_polling_task(
    storage: Arc<dyn Storage>,
    index_id: String,
    metastore_weak: Weak<Mutex<FileBackedIndex>>,
    polling_interval: Duration,
) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(polling_interval);
        interval.tick().await; //< this is to prevent fetch right after the first population of the data.
        while let Some(metadata_mutex) = metastore_weak.upgrade() {
            interval.tick().await;
            poll_index_metadata_once(&*storage, &index_id, &*metadata_mutex).await;
        }
    });
}

async fn load_file_backed_index(
    storage: Arc<dyn Storage>,
    index_id: String,
    polling_interval_opt: Option<Duration>,
) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
    let index = fetch_index(&*storage, &index_id).await?;
    let index_mutex = Arc::new(Mutex::new(index));
    if let Some(polling_interval) = polling_interval_opt {
        spawn_index_metadata_polling_task(
            storage.clone(),
            index_id.clone(),
            Arc::downgrade(&index_mutex),
            polling_interval,
        );
    }
    Ok(index_mutex)
}
