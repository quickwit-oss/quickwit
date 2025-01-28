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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use once_cell::sync::OnceCell;
use quickwit_common::uri::Uri;
use quickwit_config::{MetastoreBackend, MetastoreConfig};
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_storage::{StorageResolver, StorageResolverError};
use regex::Regex;
use tokio::sync::Mutex;
use tracing::debug;

use crate::{FileBackedMetastore, MetastoreFactory, MetastoreResolverError};

/// A file-backed metastore factory.
///
/// The implementation ensures that there is only
/// one living instance of `FileBasedMetastore` per metastore URI.
/// As a result, within a same process as long as we keep a single
/// FileBasedMetastoreFactory, it is safe to use the file based
/// metastore, even from different threads.
#[derive(Clone)]
pub struct FileBackedMetastoreFactory {
    storage_resolver: StorageResolver,
    // We never garbage collect unused metastore client instances. This should not be a problem
    // because during normal use this cache will hold at most a single instance.
    cache: Arc<Mutex<HashMap<Uri, MetastoreServiceClient>>>,
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
    /// Creates a new [`FileBackedMetastoreFactory`].
    pub fn new(storage_resolver: StorageResolver) -> Self {
        Self {
            storage_resolver,
            cache: Default::default(),
        }
    }

    async fn get_from_cache(&self, uri: &Uri) -> Option<MetastoreServiceClient> {
        self.cache.lock().await.get(uri).cloned()
    }

    /// If there is a valid entry in the cache to begin with, we ignore the new
    /// metastore and return the old one.
    ///
    /// This way we make sure that we keep only one instance associated
    /// to the key `uri` outside of this struct.
    async fn cache_metastore(
        &self,
        uri: Uri,
        metastore: MetastoreServiceClient,
    ) -> MetastoreServiceClient {
        self.cache
            .lock()
            .await
            .entry(uri)
            .or_insert(metastore)
            .clone()
    }
}

#[async_trait]
impl MetastoreFactory for FileBackedMetastoreFactory {
    fn backend(&self) -> MetastoreBackend {
        MetastoreBackend::File
    }

    async fn resolve(
        &self,
        _metastore_config: &MetastoreConfig,
        uri: &Uri,
    ) -> Result<MetastoreServiceClient, MetastoreResolverError> {
        let (uri_stripped, polling_interval_opt) = extract_polling_interval_from_uri(uri.as_str());
        let uri = Uri::from_str(&uri_stripped).map_err(|_| {
            MetastoreResolverError::InvalidConfig(format!("invalid URI: `{uri_stripped}`"))
        })?;
        if let Some(metastore) = self.get_from_cache(&uri).await {
            debug!("using metastore from cache");
            return Ok(metastore);
        }
        debug!("metastore not found in cache");
        let storage = self
            .storage_resolver
            .resolve(&uri)
            .await
            .map_err(|err| match err {
                StorageResolverError::InvalidConfig(message) => {
                    MetastoreResolverError::InvalidConfig(message)
                }
                StorageResolverError::InvalidUri(message) => {
                    MetastoreResolverError::InvalidUri(message)
                }
                StorageResolverError::UnsupportedBackend(message) => {
                    MetastoreResolverError::UnsupportedBackend(message)
                }
                StorageResolverError::FailedToOpenStorage { kind, message } => {
                    MetastoreResolverError::Initialization(MetastoreError::Internal {
                        message: format!("failed to open metastore file `{uri}`"),
                        cause: format!("StorageError {kind:?}: {message}"),
                    })
                }
            })?;
        let file_backed_metastore = FileBackedMetastore::try_new(storage, polling_interval_opt)
            .await
            .map(MetastoreServiceClient::new)
            .map_err(MetastoreResolverError::Initialization)?;
        let unique_metastore_for_uri = self.cache_metastore(uri, file_backed_metastore).await;
        Ok(unique_metastore_for_uri)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::metastore::file_backed::file_backed_metastore_factory::extract_polling_interval_from_uri;

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
