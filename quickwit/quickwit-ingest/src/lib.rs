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

#![deny(clippy::disallowed_methods)]

mod doc_batch;
pub mod error;
mod ingest_api_service;
#[path = "codegen/ingest_service.rs"]
#[allow(clippy::disallowed_methods)]
mod ingest_service;
mod ingest_v2;
mod memory_capacity;
mod metrics;
mod mrecordlog_async;
mod notifications;
mod position;
mod queue;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
pub use doc_batch::*;
pub use error::IngestServiceError;
pub use ingest_api_service::{GetMemoryCapacity, GetPartitionId, IngestApiService};
pub use ingest_service::*;
pub use ingest_v2::*;
pub use memory_capacity::MemoryCapacity;
use once_cell::sync::OnceCell;
pub use position::Position;
pub use queue::Queues;
use quickwit_actors::{Mailbox, Universe};
use quickwit_config::IngestApiConfig;
use tokio::sync::Mutex;

pub const QUEUES_DIR_NAME: &str = "queues";

pub type Result<T> = std::result::Result<T, IngestServiceError>;

type IngestApiServiceMailboxes = HashMap<PathBuf, Mailbox<IngestApiService>>;

pub static INGEST_API_SERVICE_MAILBOXES: OnceCell<Mutex<IngestApiServiceMailboxes>> =
    OnceCell::new();

/// Initializes an [`IngestApiService`] consuming the queue located at `queue_path`.
pub async fn init_ingest_api(
    universe: &Universe,
    queues_dir_path: &Path,
    config: &IngestApiConfig,
) -> anyhow::Result<Mailbox<IngestApiService>> {
    let mut guard = INGEST_API_SERVICE_MAILBOXES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .await;
    if let Some(mailbox) = guard.get(queues_dir_path) {
        return Ok(mailbox.clone());
    }
    let ingest_api_actor = IngestApiService::with_queues_dir(
        queues_dir_path,
        config.max_queue_memory_usage.as_u64() as usize,
        config.max_queue_disk_usage.as_u64() as usize,
    )
    .await
    .with_context(|| {
        format!(
            "failed to open the ingest API record log located at `{}`",
            queues_dir_path.display()
        )
    })?;
    let (ingest_api_service, _ingest_api_handle) = universe.spawn_builder().spawn(ingest_api_actor);
    guard.insert(queues_dir_path.to_path_buf(), ingest_api_service.clone());
    Ok(ingest_api_service)
}

/// Returns the instance of the single IngestApiService via a copy of its Mailbox.
pub async fn get_ingest_api_service(
    queues_dir_path: &Path,
) -> anyhow::Result<Mailbox<IngestApiService>> {
    let guard = INGEST_API_SERVICE_MAILBOXES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .await;
    if let Some(mailbox) = guard.get(queues_dir_path) {
        return Ok(mailbox.clone());
    }
    bail!(
        "ingest API service with queues directory located at `{}` is not initialized",
        queues_dir_path.display()
    )
}

/// Starts an [`IngestApiService`] instance at `<data_dir_path>/queues`.
pub async fn start_ingest_api_service(
    universe: &Universe,
    data_dir_path: &Path,
    config: &IngestApiConfig,
) -> anyhow::Result<Mailbox<IngestApiService>> {
    let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
    init_ingest_api(universe, &queues_dir_path, config).await
}

#[macro_export]
macro_rules! with_lock_metrics {
    ($future:expr, $($label:tt),*) => {
        {
            $crate::ingest_v2::metrics::INGEST_V2_METRICS
                .wal_acquire_lock_requests_in_flight
                .with_label_values([$($label),*])
                .inc();

            let now = std::time::Instant::now();
            let guard = $future;

            let elapsed = now.elapsed();
            if elapsed > std::time::Duration::from_secs(1) {
                quickwit_common::rate_limited_warn!(
                    limit_per_min=6,
                    "lock acquisition took {}ms", elapsed.as_millis()
                );
            }
            $crate::ingest_v2::metrics::INGEST_V2_METRICS
                .wal_acquire_lock_requests_in_flight
                .with_label_values([$($label),*])
                .dec();
            $crate::ingest_v2::metrics::INGEST_V2_METRICS
                .wal_acquire_lock_request_duration_secs
                .with_label_values([$($label),*])
                .observe(elapsed.as_secs_f64());

            guard
        }
    }
}

#[cfg(test)]
mod tests {

    use quickwit_actors::AskError;
    use quickwit_proto::ingest::RateLimitingCause;

    use super::*;
    use crate::{CreateQueueRequest, IngestRequest, SuggestTruncateRequest};

    #[tokio::test]
    async fn test_get_ingest_api_service() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let queues_0_dir_path = temp_dir.path().join("queues-0");
        get_ingest_api_service(&queues_0_dir_path)
            .await
            .unwrap_err();
        init_ingest_api(&universe, &queues_0_dir_path, &IngestApiConfig::default())
            .await
            .unwrap();
        let ingest_api_service_0 = get_ingest_api_service(&queues_0_dir_path).await.unwrap();
        ingest_api_service_0
            .ask_for_res(CreateQueueRequest {
                queue_id: "test-queue".to_string(),
            })
            .await
            .unwrap();

        let queues_1_dir_path = temp_dir.path().join("queues-1");
        init_ingest_api(&universe, &queues_1_dir_path, &IngestApiConfig::default())
            .await
            .unwrap();
        let ingest_api_service_1 = get_ingest_api_service(&queues_1_dir_path).await.unwrap();
        ingest_api_service_1
            .ask_for_res(CreateQueueRequest {
                queue_id: "test-queue".to_string(),
            })
            .await
            .unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_get_ingest_multiple_index_api_service() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let queues_0_dir_path = temp_dir.path().join("queues-0");
        let ingest_api_service =
            init_ingest_api(&universe, &queues_0_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        ingest_api_service
            .ask_for_res(CreateQueueRequest {
                queue_id: "index-1".to_string(),
            })
            .await
            .unwrap();
        let ingest_request = IngestRequest {
            doc_batches: vec![
                DocBatch {
                    index_id: "index-1".to_string(),
                    doc_buffer: vec![10, 11, 12].into(),
                    doc_lengths: vec![2],
                },
                DocBatch {
                    index_id: "index-2".to_string(),
                    doc_buffer: vec![10, 11, 12].into(),
                    doc_lengths: vec![2],
                },
            ],
            commit: CommitType::Auto.into(),
        };
        let ingest_result = ingest_api_service.ask_for_res(ingest_request).await;
        assert!(ingest_result.is_err());
        match ingest_result.unwrap_err() {
            AskError::ErrorReply(ingest_error) => {
                assert!(ingest_error.to_string().contains("index-2"));
            }
            _ => panic!("wrong error type"),
        }
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_queue_limit() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let queues_dir_path = temp_dir.path().join("queues-0");
        get_ingest_api_service(&queues_dir_path).await.unwrap_err();

        let ingest_api_config = serde_json::from_str(
            r#"{
            "max_queue_memory_usage": "1200b",
            "max_queue_disk_usage": "256mb"
        }"#,
        )
        .unwrap();
        init_ingest_api(&universe, &queues_dir_path, &ingest_api_config)
            .await
            .unwrap();
        let ingest_api_service = get_ingest_api_service(&queues_dir_path).await.unwrap();

        ingest_api_service
            .ask_for_res(CreateQueueRequest {
                queue_id: "test-queue".to_string(),
            })
            .await
            .unwrap();

        let ingest_request = IngestRequest {
            doc_batches: vec![DocBatch {
                index_id: "test-queue".to_string(),
                doc_buffer: vec![1; 600].into(),
                doc_lengths: vec![30; 20],
            }],
            commit: CommitType::Auto.into(),
        };

        ingest_api_service
            .ask_for_res(ingest_request.clone())
            .await
            .unwrap();

        ingest_api_service
            .ask_for_res(ingest_request.clone())
            .await
            .unwrap();

        // we have to much in memory
        assert!(matches!(
            ingest_api_service
                .ask_for_res(ingest_request.clone())
                .await
                .unwrap_err(),
            AskError::ErrorReply(IngestServiceError::RateLimited(RateLimitingCause::WalFull))
        ));

        // delete the first batch
        ingest_api_service
            .ask_for_res(SuggestTruncateRequest {
                index_id: "test-queue".to_string(),
                up_to_position_included: 29,
            })
            .await
            .unwrap();

        // now we should be okay
        ingest_api_service
            .ask_for_res(ingest_request)
            .await
            .unwrap();
        universe.assert_quit().await;
    }
}
