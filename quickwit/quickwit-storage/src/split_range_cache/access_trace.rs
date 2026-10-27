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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::Bytes;
use quickwit_common::uri::Uri;
use quickwit_config::DiskCacheConfig;
use tracing::{error, info};

use crate::{Storage, StorageResolver};

const RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub(super) async fn open_run(
    destination: &str,
    resolver: &StorageResolver,
    config: &DiskCacheConfig,
) -> anyhow::Result<(Arc<dyn Storage>, PathBuf)> {
    let storage = resolver.resolve(&destination.parse::<Uri>()?).await?;
    let run = PathBuf::from(ulid::Ulid::generate().to_string());
    let manifest = serde_json::json!({
        "format": "FETR",
        "version": 2,
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

pub(super) fn attach(
    cache: feuer::TieredMemoryDiskCache,
    storage: Arc<dyn Storage>,
    run: PathBuf,
) -> feuer::TieredMemoryDiskCache {
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
    // Keep the same key and payload across retries. Returning an error to Feuer
    // would stop delivery and silently discard subsequent events.
    let payload = package.to_vec();
    loop {
        match storage.put(&path, Box::new(payload.clone())).await {
            Ok(()) => {
                info!(uri = %storage.uri(), path = %path.display(), bytes = payload.len(), "Feuer access trace batch uploaded");
                return;
            }
            Err(error) => {
                error!(?error, uri = %storage.uri(), path = %path.display(), "Feuer access trace upload failed; retrying with reader backpressure");
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "access_trace_tests.rs"]
mod tests;
