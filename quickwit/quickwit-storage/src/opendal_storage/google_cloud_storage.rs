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

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use once_cell::sync::OnceCell;
use quickwit_common::uri::Uri;
use quickwit_config::{GoogleCloudStorageConfig, StorageBackend};
use regex::Regex;

use super::OpendalStorage;
use crate::debouncer::DebouncedStorage;
use crate::{Storage, StorageFactory, StorageResolverError};

/// Google cloud storage resolver.
pub struct GoogleCloudStorageFactory {
    storage_config: GoogleCloudStorageConfig,
}

impl GoogleCloudStorageFactory {
    /// Create a new google cloud storage factory via config.
    pub fn new(storage_config: GoogleCloudStorageConfig) -> Self {
        Self { storage_config }
    }
}

#[async_trait]
impl StorageFactory for GoogleCloudStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Google
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = from_uri(&self.storage_config, uri)?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

/// Creates an emulated storage for testing.
#[cfg(feature = "integration-testsuite")]
pub fn new_emulated_google_cloud_storage(
    uri: &Uri,
) -> Result<OpendalStorage, StorageResolverError> {
    let (bucket, root) = parse_google_uri(uri).expect("must be valid google uri");

    let mut cfg = opendal::services::Gcs::default();
    cfg.bucket(&bucket);
    cfg.root(&root.to_string_lossy());
    // The default port for the fake gcs server is 4443.
    cfg.endpoint("http://127.0.0.1:4443");

    #[derive(Debug)]
    struct DummyTokenLoader;
    #[async_trait]
    impl reqsign::GoogleTokenLoad for DummyTokenLoader {
        async fn load(&self, _: reqwest::Client) -> anyhow::Result<Option<reqsign::GoogleToken>> {
            Ok(Some(reqsign::GoogleToken::new(
                "dummy",
                86400,
                "https://www.googleapis.com/auth/devstorage.full_control",
            )))
        }
    }
    cfg.customed_token_loader(Box::new(DummyTokenLoader));

    let store = OpendalStorage::new_google_cloud_storage(uri.clone(), cfg)?;
    Ok(store)
}

fn from_uri(
    google_cloud_storage_config: &GoogleCloudStorageConfig,
    uri: &Uri,
) -> Result<OpendalStorage, StorageResolverError> {
    let credential_path = google_cloud_storage_config
        .resolve_credential_path()
        .ok_or_else(|| {
            let message = format!(
                "could not find Google credential path in environment variable `{}` or storage \
                 config",
                GoogleCloudStorageConfig::GOOGLE_CLOUD_STORAGE_CREDENTIAL_PATH_ENV_VAR
            );
            StorageResolverError::InvalidConfig(message)
        })?;
    let (bucket_name, prefix) = parse_google_uri(uri).ok_or_else(|| {
        let message = format!("failed to extract bucket name from google URI: {uri}");
        StorageResolverError::InvalidUri(message)
    })?;

    let mut cfg = opendal::services::Gcs::default();
    cfg.credential_path(&credential_path);
    cfg.bucket(&bucket_name);
    cfg.root(&prefix.to_string_lossy());

    let store = OpendalStorage::new_google_cloud_storage(uri.clone(), cfg)?;
    Ok(store)
}

fn parse_google_uri(uri: &Uri) -> Option<(String, PathBuf)> {
    // Ex: gs://bucket/prefix.
    static URI_PTN: OnceCell<Regex> = OnceCell::new();

    let captures = URI_PTN
        .get_or_init(|| {
            Regex::new(r"gs(\+[^:]+)?://(?P<bucket>[^/]+)(/(?P<prefix>.*))?$")
                .expect("The regular expression should compile.")
        })
        .captures(uri.as_str())?;

    let bucket = captures.name("bucket")?.as_str().to_string();
    let prefix = captures
        .name("prefix")
        .map(|prefix_match| PathBuf::from(prefix_match.as_str()))
        .unwrap_or_default();
    Some((bucket, prefix))
}

#[cfg(test)]
mod tests {
    use quickwit_common::uri::Uri;

    use super::parse_google_uri;

    #[test]
    fn test_parse_google_uri() {
        assert!(parse_google_uri(&Uri::for_test("gs://")).is_none());

        let (bucket, prefix) = parse_google_uri(&Uri::for_test("gs://test-bucket")).unwrap();
        assert_eq!(bucket, "test-bucket");
        assert!(prefix.to_str().unwrap().is_empty());

        let (bucket, prefix) = parse_google_uri(&Uri::for_test("gs://test-bucket/")).unwrap();
        assert_eq!(bucket, "test-bucket");
        assert!(prefix.to_str().unwrap().is_empty());

        let (bucket, prefix) =
            parse_google_uri(&Uri::for_test("gs://test-bucket/indexes")).unwrap();
        assert_eq!(bucket, "test-bucket");
        assert_eq!(prefix.to_str().unwrap(), "indexes");
    }
}
