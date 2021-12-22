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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use once_cell::sync::OnceCell;
use quickwit_storage::{quickwit_storage_uri_resolver, StorageResolverError, StorageUriResolver};
use regex::Regex;

use crate::{
    FileBackedMetastore, Metastore, MetastoreError, MetastoreFactory, MetastoreResolverError,
};

/// A file-backed metastore factory
#[derive(Clone)]
pub struct FileBackedMetastoreFactory {
    storage_uri_resolver: StorageUriResolver,
}

impl Default for FileBackedMetastoreFactory {
    fn default() -> Self {
        FileBackedMetastoreFactory {
            storage_uri_resolver: quickwit_storage_uri_resolver().clone(),
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

#[async_trait]
impl MetastoreFactory for FileBackedMetastoreFactory {
    async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let (uri_stripped, polling_interval_opt) = extract_polling_interval_from_uri(uri);
        let storage =
            self.storage_uri_resolver
                .resolve(&uri_stripped)
                .map_err(|err| match err {
                    StorageResolverError::InvalidUri { message } => {
                        MetastoreResolverError::InvalidUri(message)
                    }
                    StorageResolverError::ProtocolUnsupported { protocol } => {
                        MetastoreResolverError::ProtocolUnsupported(protocol)
                    }
                    StorageResolverError::FailedToOpenStorage { kind, message } => {
                        MetastoreResolverError::FailedToOpenMetastore(
                            MetastoreError::InternalError {
                                message: format!("Failed to open metastore file `{}`.", uri),
                                cause: anyhow::anyhow!("StorageError {:?}: {}.", kind, message),
                            },
                        )
                    }
                })?;
        let mut file_backed_metastore = FileBackedMetastore::new(storage);
        file_backed_metastore.set_polling_interval(polling_interval_opt);
        Ok(Arc::new(file_backed_metastore))
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
