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

use std::sync::Arc;

use async_trait::async_trait;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client as S3Client;
use quickwit_common::uri::Uri;
use quickwit_config::{S3StorageConfig, StorageBackend};
use tokio::sync::OnceCell;

use super::s3_compatible_storage::{create_s3_client, create_s3_client_with_provider};
use crate::{
    DebouncedStorage, S3CompatibleObjectStorage, Storage, StorageFactory, StorageResolverError,
};

/// S3 compatible object storage resolver.
pub struct S3CompatibleObjectStorageFactory {
    storage_config: S3StorageConfig,
    credentials_provider: Option<SharedCredentialsProvider>,
    // we cache the S3Client so we don't rebuild one every time we build a new Storage (for
    // every search query).
    // We don't build it in advance because we don't know if this factory is one that will
    // end up being used, or if something like azure, gcs, or even local files, will be used
    // instead.
    s3_client: OnceCell<S3Client>,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3-compatible storage factory.
    pub fn new(storage_config: S3StorageConfig) -> Self {
        Self {
            storage_config,
            credentials_provider: None,
            s3_client: OnceCell::new(),
        }
    }

    /// Creates a new S3-compatible storage factory with a custom credentials provider.
    pub fn new_with_credentials_provider(
        storage_config: S3StorageConfig,
        credentials_provider: SharedCredentialsProvider,
    ) -> Self {
        Self {
            storage_config,
            credentials_provider: Some(credentials_provider),
            s3_client: OnceCell::new(),
        }
    }
}

#[async_trait]
impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let s3_client = self
            .s3_client
            .get_or_init(|| async {
                if let Some(ref credentials_provider) = self.credentials_provider {
                    create_s3_client_with_provider(&self.storage_config, Some(credentials_provider.clone())).await
                } else {
                    create_s3_client(&self.storage_config).await
                }
            })
            .await
            .clone();
        let storage =
            S3CompatibleObjectStorage::from_uri_and_client(&self.storage_config, uri, s3_client)
                .await?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

#[cfg(test)]
mod tests {
    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_sdk_s3::config::Credentials;
    use quickwit_common::uri::Uri;
    use quickwit_config::{S3StorageConfig, StorageBackend};

    use super::*;

    #[test]
    fn test_s3_factory_backend() {
        let factory = S3CompatibleObjectStorageFactory::new(S3StorageConfig::default());
        assert_eq!(factory.backend(), StorageBackend::S3);
    }

    #[test]
    fn test_s3_factory_new() {
        let config = S3StorageConfig {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            region: Some("us-west-2".to_string()),
            ..Default::default()
        };

        let factory = S3CompatibleObjectStorageFactory::new(config.clone());
        assert_eq!(factory.storage_config.access_key_id, config.access_key_id);
        assert_eq!(factory.storage_config.secret_access_key, config.secret_access_key);
        assert_eq!(factory.storage_config.region, config.region);
        assert!(factory.credentials_provider.is_none());
    }

    #[test]
    fn test_s3_factory_new_with_credentials_provider() {
        let config = S3StorageConfig {
            region: Some("us-west-2".to_string()),
            ..Default::default()
        };

        let credentials = Credentials::new("provider_key", "provider_secret", Some("provider_token".to_string()), None, "test_provider");
        let credentials_provider = SharedCredentialsProvider::new(credentials);

        let factory = S3CompatibleObjectStorageFactory::new_with_credentials_provider(
            config.clone(),
            credentials_provider.clone()
        );

        assert_eq!(factory.storage_config.region, config.region);
        assert!(factory.credentials_provider.is_some());
    }

    #[test]
    fn test_s3_factory_new_with_session_token() {
        let config = S3StorageConfig {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            session_token: Some("test_session_token".to_string()),
            region: Some("us-west-2".to_string()),
            ..Default::default()
        };

        let factory = S3CompatibleObjectStorageFactory::new(config.clone());
        assert_eq!(factory.storage_config.session_token, config.session_token);
    }

    #[tokio::test]
    async fn test_s3_factory_resolve_with_session_token() {
        let config = S3StorageConfig {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            session_token: Some("test_session_token".to_string()),
            region: Some("us-west-2".to_string()),
            ..Default::default()
        };

        let factory = S3CompatibleObjectStorageFactory::new(config);
        let uri = Uri::for_test("s3://test-bucket/test-prefix");

        let storage_result = factory.resolve(&uri).await;
        assert!(storage_result.is_ok());
    }

    #[tokio::test]
    async fn test_s3_factory_resolve_with_credentials_provider() {
        let config = S3StorageConfig {
            region: Some("us-west-2".to_string()),
            ..Default::default()
        };

        let credentials = Credentials::new("provider_key", "provider_secret", Some("provider_token".to_string()), None, "test_provider");
        let credentials_provider = SharedCredentialsProvider::new(credentials);

        let factory = S3CompatibleObjectStorageFactory::new_with_credentials_provider(
            config,
            credentials_provider
        );
        let uri = Uri::for_test("s3://test-bucket/test-prefix");

        let storage_result = factory.resolve(&uri).await;
        assert!(storage_result.is_ok());
    }

    #[tokio::test]
    async fn test_s3_factory_resolve_without_credentials() {
        let config = S3StorageConfig {
            region: Some("us-west-2".to_string()),
            ..Default::default()
        };

        let factory = S3CompatibleObjectStorageFactory::new(config);
        let uri = Uri::for_test("s3://test-bucket/test-prefix");

        // Should work with default credential provider chain
        let storage_result = factory.resolve(&uri).await;
        assert!(storage_result.is_ok());
    }
}
