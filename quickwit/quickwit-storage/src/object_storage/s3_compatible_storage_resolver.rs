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
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use quickwit_common::uri::Uri;
use quickwit_config::{S3StorageConfig, StorageBackend};
use tokio::sync::OnceCell;

use super::s3_compatible_storage::{create_s3_client, parse_s3_uri};
use crate::{
    DebouncedStorage, S3CompatibleObjectStorage, Storage, StorageFactory, StorageResolverError,
};

/// S3 compatible object storage resolver.
pub struct S3CompatibleObjectStorageFactory {
    default_backend: S3Backend,
    // Backends for the buckets listed under `storage.s3.buckets`, keyed by bucket name.
    bucket_backends: HashMap<String, S3Backend>,
}

struct S3Backend {
    storage_config: S3StorageConfig,
    // we cache the S3Client so we don't rebuild one every time we build a new Storage (for
    // every search query).
    // We don't build it in advance because we don't know if this factory is one that will
    // end up being used, or if something like azure, gcs, or even local files, will be used
    // instead.
    s3_client: OnceCell<S3Client>,
}

impl S3Backend {
    fn new(storage_config: S3StorageConfig) -> Self {
        Self {
            storage_config,
            s3_client: OnceCell::new(),
        }
    }
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3-compatible storage factory.
    pub fn new(mut storage_config: S3StorageConfig) -> Self {
        let bucket_backends = storage_config
            .bucket_configs()
            .map(|(bucket, bucket_config)| (bucket.to_string(), S3Backend::new(bucket_config)))
            .collect();
        storage_config.buckets.clear();
        Self {
            default_backend: S3Backend::new(storage_config),
            bucket_backends,
        }
    }

    /// Returns the backend serving `uri`: the `storage.s3.buckets.<bucket>` entry whose key
    /// exactly matches the URI's bucket, or the primary backend otherwise.
    fn backend_for_uri(&self, uri: &Uri) -> &S3Backend {
        if self.bucket_backends.is_empty() {
            return &self.default_backend;
        }
        parse_s3_uri(uri)
            .and_then(|(bucket, _prefix)| self.bucket_backends.get(&bucket))
            .unwrap_or(&self.default_backend)
    }
}

#[async_trait]
impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let backend = self.backend_for_uri(uri);
        let s3_client = backend
            .s3_client
            .get_or_init(|| create_s3_client(&backend.storage_config))
            .await
            .clone();
        let storage =
            S3CompatibleObjectStorage::from_uri_and_client(&backend.storage_config, uri, s3_client)
                .await?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn bucket_config(endpoint: &str) -> S3StorageConfig {
        S3StorageConfig {
            endpoint: Some(endpoint.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn test_s3_factory_routes_by_exact_bucket_name() {
        let storage_config = S3StorageConfig {
            endpoint: Some("https://primary.example.com".to_string()),
            buckets: BTreeMap::from([
                (
                    "logs-bucket-eu".to_string(),
                    bucket_config("https://eu.example.com"),
                ),
                (
                    "my.dotted.bucket".to_string(),
                    bucket_config("https://dotted.example.com"),
                ),
            ]),
            ..Default::default()
        };
        let factory = S3CompatibleObjectStorageFactory::new(storage_config);
        assert!(factory.default_backend.storage_config.buckets.is_empty());

        let endpoint_for = |uri: &'static str| -> (Option<String>, bool) {
            let backend = factory.backend_for_uri(&Uri::for_test(uri));
            (
                backend.storage_config.endpoint.clone(),
                backend.storage_config.is_bucket_config,
            )
        };
        assert_eq!(
            endpoint_for("s3://logs-bucket-eu/indexes/logs"),
            (Some("https://eu.example.com".to_string()), true)
        );
        assert_eq!(
            endpoint_for("s3://logs-bucket-eu"),
            (Some("https://eu.example.com".to_string()), true)
        );
        assert_eq!(
            endpoint_for("s3://my.dotted.bucket/indexes"),
            (Some("https://dotted.example.com".to_string()), true)
        );
        assert_eq!(
            endpoint_for("s3://logs-bucket-eu-2/indexes"),
            (Some("https://primary.example.com".to_string()), false)
        );
        assert_eq!(
            endpoint_for("s3://other-bucket/indexes"),
            (Some("https://primary.example.com".to_string()), false)
        );
    }

    #[test]
    fn test_s3_factory_without_buckets_uses_primary_backend() {
        let factory =
            S3CompatibleObjectStorageFactory::new(bucket_config("https://primary.example.com"));
        let backend = factory.backend_for_uri(&Uri::for_test("s3://any-bucket/indexes"));
        assert!(std::ptr::eq(backend, &factory.default_backend));
    }
}
