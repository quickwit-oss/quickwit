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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::Bytes;
use quickwit_common::uri::Uri;
use quickwit_config::StorageCacheConfig;
use tracing::{error, info};

use crate::{Storage, StorageResolver};

pub(super) async fn open_run(
    destination: &str,
    resolver: &StorageResolver,
    config: &StorageCacheConfig,
) -> anyhow::Result<(Arc<dyn Storage>, PathBuf)> {
    let storage = resolver.resolve(&destination.parse::<Uri>()?).await?;
    let run = PathBuf::from(ulid::Ulid::generate().to_string());
    let manifest = serde_json::json!({
        "format": "FETR",
        "version": 1,
        "run": run,
        "unix_start_ns": SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos().to_string(),
        "node": std::env::var("QW_NODE_ID").ok(),
        "pod": std::env::var("KUBERNETES_POD_NAME").ok(),
        "host": std::env::var("KUBERNETES_NODE_NAME").ok(),
        "image_tag": std::env::var("IMAGE_TAG").ok(),
        "cache": config,
        "reclaim_sample_size": std::env::var("FEUER_RECLAIM_SAMPLE_SIZE").ok(),
    });
    // Verify write permissions at startup instead of silently running an untraced benchmark.
    storage
        .put(
            &run.join("manifest.json"),
            Box::new(serde_json::to_vec_pretty(&manifest)?),
        )
        .await
        .context("failed to write Feuer access trace manifest")?;
    info!(uri = %storage.uri(), run = %run.display(), "Feuer access trace enabled");
    Ok((storage, run))
}

pub(super) fn attach(cache: feuer::Cache, storage: Arc<dyn Storage>, run: PathBuf) -> feuer::Cache {
    let mut sequence = 0u64;
    cache.with_trace(move |package| {
        let storage = storage.clone();
        let path = run.join(format!("{sequence:012}.feuer"));
        sequence += 1;
        async move {
            upload(storage, path, package).await;
            Ok(())
        }
    })
}

async fn upload(storage: Arc<dyn Storage>, path: PathBuf, package: Bytes) {
    match storage.put(&path, Box::new(package.to_vec())).await {
        Ok(()) => {
            info!(uri = %storage.uri(), path = %path.display(), bytes = package.len(), "Feuer access trace batch uploaded");
        }
        Err(error) => {
            error!(?error, uri = %storage.uri(), path = %path.display(), "Feuer access trace upload failed; dropping batch");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use bytesize::ByteSize;

    use super::*;
    use crate::{MockStorage, StorageErrorKind};

    fn config() -> StorageCacheConfig {
        StorageCacheConfig {
            directory: Some(PathBuf::from("unused")),
            disk_capacity: ByteSize::b(0),
            memory_capacity: ByteSize::mib(8),
        }
    }

    #[tokio::test]
    async fn test_manifest_and_unique_run_directories() {
        let directory = tempfile::tempdir().unwrap();
        let destination = format!("file://{}", directory.path().display());
        let resolver = StorageResolver::for_test();
        let (storage, run) = open_run(&destination, &resolver, &config()).await.unwrap();
        let (other_storage, other_run) =
            open_run(&destination, &resolver, &config()).await.unwrap();
        assert_ne!(run, other_run);
        for (storage, run) in [(storage, run), (other_storage, other_run)] {
            let bytes = storage.get_all(&run.join("manifest.json")).await.unwrap();
            let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(manifest["format"], "FETR");
            assert_eq!(manifest["version"], 1);
            assert_eq!(manifest["run"], run.to_str().unwrap());
            assert_eq!(manifest["cache"], serde_json::to_value(config()).unwrap());
            assert!(
                manifest["unix_start_ns"]
                    .as_str()
                    .unwrap()
                    .parse::<u128>()
                    .unwrap()
                    > 0
            );
        }
    }

    #[tokio::test]
    async fn test_invalid_or_unwritable_destination_fails_startup() {
        let resolver = StorageResolver::for_test();
        for destination in ["", "http://example.com/traces"] {
            assert!(open_run(destination, &resolver, &config()).await.is_err());
        }
        // A regular file cannot be used as an output directory, including under root.
        let file = tempfile::NamedTempFile::new().unwrap();
        let destination = format!("file://{}", file.path().display());
        assert!(open_run(&destination, &resolver, &config()).await.is_err());
    }

    #[tokio::test]
    async fn test_upload_preserves_key_and_package_and_drops_failures() {
        for fail in [false, true] {
            let mut storage = MockStorage::new();
            storage
                .expect_put()
                .times(1)
                .returning(move |path, payload| {
                    assert_eq!(path, Path::new("run/000000000000.feuer"));
                    let bytes = futures::executor::block_on(payload.read_all()).unwrap();
                    assert_eq!(&bytes[..], b"FETR\x01\0\0\0package");
                    if fail {
                        return Err(StorageErrorKind::Unauthorized
                            .with_error(anyhow::anyhow!("access denied")));
                    }
                    Ok(())
                });
            upload(
                Arc::new(storage),
                PathBuf::from("run/000000000000.feuer"),
                Bytes::from_static(b"FETR\x01\0\0\0package"),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_trace_callback_exports_miss_hit_and_error_on_drop() {
        use crate::storage_cache::open_storage_cache;
        use crate::wrap_storage_with_storage_cache;

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut storage = MockStorage::new();
        storage
            .expect_put()
            .times(1)
            .return_once(move |path, payload| {
                assert_eq!(path, Path::new("run/000000000000.feuer"));
                let bytes = futures::executor::block_on(payload.read_all()).unwrap();
                sender.send(bytes).unwrap();
                Ok(())
            });
        // Memory-only avoids requiring io_uring for this callback integration test.
        let cache = open_storage_cache(&config(), &crate::FeuerIoQueues::default()).unwrap();
        let cache = attach(cache, Arc::new(storage), PathBuf::from("run"));
        let mut lower = MockStorage::new();
        lower
            .expect_uri()
            .return_const(Uri::for_test("ram://splits"));
        lower
            .expect_get_slice_into()
            .withf(|path, range, _| path == Path::new("object") && range == &(0..4))
            .times(1)
            .returning(|_, _, buffer| {
                buffer.copy_from_slice(b"data");
                Ok(())
            });
        lower
            .expect_get_slice_into()
            .withf(|path, range, _| path == Path::new("error") && range == &(0..1))
            .times(1)
            .returning(|_, _, _| {
                Err(StorageErrorKind::Io.with_error(anyhow::anyhow!("source failed")))
            });
        let storage = wrap_storage_with_storage_cache(cache, Arc::new(lower));
        for _ in 0..2 {
            assert_eq!(
                storage
                    .get_slice(Path::new("object"), 0..4)
                    .await
                    .unwrap()
                    .as_slice(),
                b"data"
            );
        }
        assert!(storage.get_slice(Path::new("error"), 0..1).await.is_err());
        drop(storage);
        let bytes = tokio::time::timeout(Duration::from_secs(5), receiver)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes[..8], b"FETR\x01\0\0\0");
        let records: Vec<_> = bytes[8..].chunks_exact(41).collect();
        assert_eq!(bytes.len(), 8 + 6 * 41);
        assert_eq!(
            records.iter().map(|record| record[0]).collect::<Vec<_>>(),
            [0, 3, 0, 1, 0, 4]
        );
        for (index, pair) in records.chunks_exact(2).enumerate() {
            let field = |record: &[u8], offset| {
                u64::from_le_bytes(record[offset..offset + 8].try_into().unwrap())
            };
            assert_eq!(field(pair[0], 1), index as u64 + 1);
            assert_eq!(field(pair[0], 1), field(pair[1], 1));
            assert!(field(pair[1], 9) >= field(pair[0], 9));
            assert_eq!(&pair[0][17..33], &pair[1][17..33]);
            assert_eq!(field(pair[0], 33), 0);
            assert_eq!(field(pair[1], 33), if index < 2 { 4 } else { 0 });
        }
        assert_eq!(&records[0][17..33], &records[2][17..33]);
    }
}
