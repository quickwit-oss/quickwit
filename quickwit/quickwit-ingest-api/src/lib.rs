// Copyright (C) 2023 Quickwit, Inc.
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

#![deny(clippy::disallowed_methods)]

mod concurrent_log;
mod errors;
mod ingest_api_service;
#[path = "codegen/ingest_service.rs"]
mod ingest_service;
mod metrics;
mod position;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
pub use concurrent_log::ConcurrentMultiRecordLog;
pub use errors::IngestServiceError;
pub use ingest_api_service::IngestApiService;
pub use ingest_service::*;
use metrics::INGEST_METRICS;
use once_cell::sync::OnceCell;
pub use position::Position;
use quickwit_config::IngestApiConfig;
use tokio::sync::Mutex;

pub const QUEUES_DIR_NAME: &str = "queues";

pub type Result<T> = std::result::Result<T, IngestServiceError>;

pub static INGEST_API_SERVICE_MAILBOXES: OnceCell<Mutex<HashMap<PathBuf, IngestApiService>>> =
    OnceCell::new();

/// Initializes an [`IngestApiService`] consuming the queue located at `queue_path`.
pub async fn init_ingest_api(
    queues_dir_path: &Path,
    config: &IngestApiConfig,
) -> anyhow::Result<IngestApiService> {
    let mut guard = INGEST_API_SERVICE_MAILBOXES
        .get_or_init(Default::default)
        .lock()
        .await;
    if let Some(ingest_service) = guard.get(queues_dir_path) {
        return Ok(ingest_service.clone());
    }
    let ingest_api_service = IngestApiService::with_queues_dir(
        queues_dir_path,
        config.max_queue_memory_usage.get_bytes() as usize,
        config.max_queue_disk_usage.get_bytes() as usize,
    )
    .await
    .with_context(|| {
        format!(
            "Failed to open the ingest API record log located at `{}`.",
            queues_dir_path.display()
        )
    })?;
    guard.insert(queues_dir_path.to_path_buf(), ingest_api_service.clone());
    Ok(ingest_api_service)
}

/// Returns the instance of the single IngestApiService via a copy of it's Mailbox.
pub async fn get_ingest_api_service(queues_dir_path: &Path) -> anyhow::Result<IngestApiService> {
    let guard = INGEST_API_SERVICE_MAILBOXES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .await;
    if let Some(ingest_api_service) = guard.get(queues_dir_path) {
        return Ok(ingest_api_service.clone());
    }
    bail!(
        "Ingest API service with queues directory located at `{}` is not initialized.",
        queues_dir_path.display()
    )
}

/// Starts an [`IngestApiService`] instance at `<data_dir_path>/queues`.
pub async fn start_ingest_api_service(
    data_dir_path: &Path,
    config: &IngestApiConfig,
) -> anyhow::Result<IngestApiService> {
    let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
    init_ingest_api(&queues_dir_path, config).await
}

/// Adds a document raw bytes to a [`DocBatch`]
pub fn add_doc(payload: &[u8], fetch_resp: &mut DocBatch) -> usize {
    fetch_resp.concat_docs.extend_from_slice(payload);
    fetch_resp.doc_lens.push(payload.len() as u64);
    INGEST_METRICS
        .ingested_num_bytes
        .inc_by(payload.len() as u64);
    payload.len()
}

/// Returns an iterator over the document payloads within a doc_batch.
pub fn iter_doc_payloads(doc_batch: &DocBatch) -> impl Iterator<Item = &[u8]> {
    doc_batch
        .doc_lens
        .iter()
        .cloned()
        .scan(0, |current_offset, doc_num_bytes| {
            let start = *current_offset;
            let end = start + doc_num_bytes as usize;
            *current_offset = end;
            Some(&doc_batch.concat_docs[start..end])
        })
}

#[cfg(test)]
mod tests {

    use byte_unit::Byte;

    use super::*;
    use crate::{CreateQueueRequest, IngestRequest, SuggestTruncateRequest};

    #[tokio::test]
    async fn test_get_ingest_api_service() {
        let tempdir = tempfile::tempdir().unwrap();
        {
            let queues_dir_path = tempdir.path().join("queues-0");
            get_ingest_api_service(&queues_dir_path).await.unwrap_err();

            let ingest_api_service = init_ingest_api(&queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();

            let partition_id = get_ingest_api_service(&queues_dir_path)
                .await
                .unwrap()
                .partition_id()
                .to_string();
            assert_eq!(ingest_api_service.partition_id(), partition_id);

            ingest_api_service
                .create_queue(CreateQueueRequest {
                    queue_id: "test-queue".to_string(),
                })
                .await
                .unwrap();
        }
        {
            let queues_dir_path = tempdir.path().join("queues-1");
            let ingest_api_service = init_ingest_api(&queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
            ingest_api_service
                .create_queue(CreateQueueRequest {
                    queue_id: "test-queue".to_string(),
                })
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_get_ingest_multiple_index_api_service() {
        let tempdir = tempfile::tempdir().unwrap();

        let mut ingest_api_service = init_ingest_api(tempdir.path(), &IngestApiConfig::default())
            .await
            .unwrap();
        ingest_api_service
            .create_queue(CreateQueueRequest {
                queue_id: "test-index-1".to_string(),
            })
            .await
            .unwrap();
        let ingest_request = IngestRequest {
            doc_batches: vec![
                DocBatch {
                    index_id: "test-index-1".to_string(),
                    concat_docs: vec![10, 11, 12],
                    doc_lens: vec![2],
                },
                DocBatch {
                    index_id: "test-index-2".to_string(),
                    concat_docs: vec![10, 11, 12],
                    doc_lens: vec![2],
                },
            ],
        };
        let error = ingest_api_service.ingest(ingest_request).await.unwrap_err();
        assert_eq!(
            error,
            IngestServiceError::IndexNotFound {
                index_id: "test-index-2".to_string()
            }
        );
        let fetch_request = FetchRequest {
            index_id: "test-index-1".to_string(),
            start_after: None,
            num_bytes_limit: None,
        };
        let fetch_response = ingest_api_service.fetch(fetch_request).await.unwrap();
        assert_eq!(fetch_response.doc_batch.unwrap().concat_docs.len(), 0);
    }

    #[tokio::test]
    async fn test_queue_limit() {
        let tempdir = tempfile::tempdir().unwrap();

        let mut ingest_api_service = init_ingest_api(
            tempdir.path(),
            &IngestApiConfig {
                max_queue_memory_usage: Byte::from_bytes(1024),
                max_queue_disk_usage: Byte::from_bytes(1024 * 1024 * 256),
            },
        )
        .await
        .unwrap();

        ingest_api_service
            .create_queue(CreateQueueRequest {
                queue_id: "test-queue".to_string(),
            })
            .await
            .unwrap();

        let ingest_request = IngestRequest {
            doc_batches: vec![DocBatch {
                index_id: "test-queue".to_string(),
                concat_docs: vec![1; 600],
                doc_lens: vec![30; 20],
            }],
        };

        ingest_api_service
            .ingest(ingest_request.clone())
            .await
            .unwrap();

        ingest_api_service
            .ingest(ingest_request.clone())
            .await
            .unwrap();

        // we have to much in memory
        assert!(matches!(
            ingest_api_service
                .ingest(ingest_request.clone())
                .await
                .unwrap_err(),
            IngestServiceError::RateLimited
        ));

        // delete the first batch
        ingest_api_service
            .suggest_truncate(SuggestTruncateRequest {
                index_id: "test-queue".to_string(),
                up_to_position_included: 29,
            })
            .await
            .unwrap();

        // now we should be okay
        ingest_api_service.ingest(ingest_request).await.unwrap();
    }
}
