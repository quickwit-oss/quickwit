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
    pub fn new(storage_config: S3StorageConfig) -> Self {
        let bucket_backends = storage_config
            .bucket_configs()
            .map(|(bucket, bucket_config)| (bucket.to_string(), S3Backend::new(bucket_config)))
            .collect();
        Self {
            default_backend: S3Backend::new(storage_config),
            bucket_backends,
        }
    }
}

#[async_trait]
impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        // A `storage.s3.buckets.<bucket>` entry, if present, supplies that bucket's own
        // endpoint/credentials/region; any unlisted bucket uses the primary backend.
        let backend = parse_s3_uri(uri)
            .and_then(|(bucket, _prefix)| self.bucket_backends.get(&bucket))
            .unwrap_or(&self.default_backend);
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
