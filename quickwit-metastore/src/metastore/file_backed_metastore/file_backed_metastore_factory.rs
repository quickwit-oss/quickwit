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

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use once_cell::sync::OnceCell;
use quickwit_common::uri::Uri;
use quickwit_storage::{quickwit_storage_uri_resolver, StorageResolverError, StorageUriResolver};
use regex::Regex;
use tokio::sync::Mutex;
use tracing::debug;

use crate::{
    FileBackedMetastore, Metastore, MetastoreError, MetastoreFactory, MetastoreResolverError,
};

/// A file-backed metastore factory.
///
/// The implementation ensures that there is only
/// one living instance of `FileBasedMetastore` per metastore-uri.
/// As a result, within a same process as long as we keep a single
/// FileBasedMetastoreFactory, it is safe to use the file based
/// metastore, even from different threads.
#[derive(Clone)]
pub struct FileBackedMetastoreFactory {
    storage_uri_resolver: StorageUriResolver,
    // We almost never garbage collect the dangling Weak pointers
    // here. This is judged to not be much of a problem however.
    //
    // In a normal run, this cache will contain a single Metastore.
    cache: Arc<Mutex<HashMap<Uri, Weak<dyn Metastore>>>>,
}

impl Default for FileBackedMetastoreFactory {
    fn default() -> Self {
        FileBackedMetastoreFactory {
            storage_uri_resolver: quickwit_storage_uri_resolver().clone(),
            cache: Default::default(),
        }
    }
}

fn extract_polling_interval_from_uri(uri: &str) -> (String, Option<Duration>) {
    static URI_FRAGMENT_PATTERN: OnceCell<Regex> = OnceCell::new();
    if let Some(captures) = URI_FRAGMENT_PATTERN
        .get_or_init(|| Regex::new("(.*)#polling_interval=([1-9][0-9]{0,8})s").unwrap())
        .captures(uri)
    {
        let uri_without_fragment = captures.get(1).unwrap().as_str().to_string();
        let polling_interval_in_secs: u64 =
            captures.get(2).unwrap().as_str().parse::<u64>().unwrap();
        (
            uri_without_fragment,
            Some(Duration::from_secs(polling_interval_in_secs)),
        )
    } else {
        (uri.to_string(), None)
    }
}

impl FileBackedMetastoreFactory {
    async fn get_from_cache(&self, uri: &Uri) -> Option<Arc<dyn Metastore>> {
        let cache_lock = self.cache.lock().await;
        cache_lock.get(uri).and_then(Weak::upgrade)
    }

    /// If there is a valid entry in the cache to begin with, we ignore the new
    /// metastore and return the old one.
    ///
    /// This way we make sure that we keep only one instance associated
    /// to the key `uri` outside of this struct.
    async fn cache_metastore(&self, uri: Uri, metastore: Arc<dyn Metastore>) -> Arc<dyn Metastore> {
        let mut cache_lock = self.cache.lock().await;
        if let Some(metastore_weak) = cache_lock.get(&uri) {
            if let Some(metastore_arc) = metastore_weak.upgrade() {
                return metastore_arc.clone();
            }
        }
        cache_lock.insert(uri, Arc::downgrade(&metastore));
        metastore
    }
}

#[async_trait]
impl MetastoreFactory for FileBackedMetastoreFactory {
    async fn resolve(&self, uri: Uri) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let (uri_stripped, polling_interval_opt) = extract_polling_interval_from_uri(uri.to_str());
        // The Uri has the benefit of canonicalizing our path.
        let uri = quickwit_common::uri::Uri::try_new(&uri_stripped).map_err(|parse_uri_err| {
            MetastoreResolverError::InvalidUri(format!("Invalid uri: {}. {:?}", uri, parse_uri_err))
        })?;
        if let Some(metastore) = self.get_from_cache(&uri).await {
            debug!("using metastore from cache");
            return Ok(metastore);
        }
        debug!("metastore not found in cache");
        let storage = self
            .storage_uri_resolver
            .resolve(uri.as_ref())
            .map_err(|err| match err {
                StorageResolverError::InvalidUri { message } => {
                    MetastoreResolverError::InvalidUri(message)
                }
                StorageResolverError::ProtocolUnsupported { protocol } => {
                    MetastoreResolverError::ProtocolUnsupported(protocol)
                }
                StorageResolverError::FailedToOpenStorage { kind, message } => {
                    MetastoreResolverError::FailedToOpenMetastore(MetastoreError::InternalError {
                        message: format!("Failed to open metastore file `{}`.", uri),
                        cause: anyhow::anyhow!("StorageError {:?}: {}.", kind, message),
                    })
                }
            })?;
        let file_backed_metastore = FileBackedMetastore::try_new(storage, polling_interval_opt)
            .await
            .map_err(MetastoreResolverError::FailedToOpenMetastore)?;
        let unique_metastore_for_uri = self
            .cache_metastore(uri, Arc::new(file_backed_metastore))
            .await;
        Ok(unique_metastore_for_uri)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::metastore::file_backed_metastore::file_backed_metastore_factory::extract_polling_interval_from_uri;

    #[test]
    fn test_extract_polling_interval_from_uri() {
        assert_eq!(
            extract_polling_interval_from_uri("file://some-uri#polling_interval=23s"),
            ("file://some-uri".to_string(), Some(Duration::from_secs(23)))
        );
        assert_eq!(
            extract_polling_interval_from_uri(
                "file://some-uri#polling_interval=18446744073709551616s"
            ),
            (
                "file://some-uri#polling_interval=18446744073709551616s".to_string(),
                None
            )
        );
        assert_eq!(
            extract_polling_interval_from_uri("file://some-uri#polling_interval=0s"),
            ("file://some-uri#polling_interval=0s".to_string(), None)
        );
        assert_eq!(
            extract_polling_interval_from_uri("file://some-uri#otherfragment#polling_interval=10s"),
            (
                "file://some-uri#otherfragment".to_string(),
                Some(Duration::from_secs(10))
            )
        );
    }
}
