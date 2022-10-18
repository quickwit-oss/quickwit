// Copyright (C) 2022 Quickwit, Inc.
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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::IngestApiSourceParams;
use quickwit_ingest_api::{get_ingest_api_service, iter_doc_payloads, IngestApiService};
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use quickwit_proto::ingest_api::{
    CreateQueueIfNotExistsRequest, FetchRequest, FetchResponse, SuggestTruncateRequest,
};
use serde::Serialize;

use super::file_source::BATCH_NUM_BYTES_LIMIT;
use super::{Source, SourceActor, SourceContext, TypedSourceFactory};
use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::SourceExecutionContext;

/// Wait time for SourceActor before pooling for new documents.
/// TODO: Think of better way, maybe increment this (i.e wait longer) as time
/// goes on without receiving docs.
const INGEST_API_POLLING_COOL_DOWN: Duration = Duration::from_secs(1);

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IngestApiSourceCounters {
    /// Maintains the value of where we stopped in queue from
    /// a previous call on `emit_batch` and allows
    /// setting the lower-bound of the checkpoint delta.
    /// It has the same value as `current_offset` at the end of emit_batch.
    pub previous_offset: Option<u64>,
    /// Maintains the value of where we are in queue and allows
    /// setting the upper-bound of the checkpoint delta.
    pub current_offset: Option<u64>,
    pub num_docs_processed: u64,
}

pub struct IngestApiSource {
    ctx: Arc<SourceExecutionContext>,
    source_id: String,
    partition_id: PartitionId,
    params: IngestApiSourceParams,
    ingest_api_service: Mailbox<IngestApiService>,
    counters: IngestApiSourceCounters,
}

impl fmt::Debug for IngestApiSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IngestApiSource {{ source_id: {} }}", self.source_id)
    }
}

impl IngestApiSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: IngestApiSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let source_id = ctx.source_config.source_id.clone();
        let queues_dir_path = ctx.queues_dir_path.as_path();
        let ingest_api_service = get_ingest_api_service(queues_dir_path).await?;
        // Ensure a queue for this index exists.
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: ctx.index_id.clone(),
        };
        ingest_api_service.ask_for_res(create_queue_req).await?;
        let partition_id = PartitionId::from(ctx.index_id.clone());
        let previous_offset = if let Some(Position::Offset(offset_str)) =
            checkpoint.position_for_partition(&partition_id)
        {
            Some(offset_str.parse::<u64>()?)
        } else {
            None
        };
        let current_offset = previous_offset;
        let ingest_api_source = IngestApiSource {
            ctx,
            source_id,
            partition_id,
            params,
            ingest_api_service,
            counters: IngestApiSourceCounters {
                previous_offset,
                current_offset,
                num_docs_processed: 0,
            },
        };
        Ok(ingest_api_source)
    }

    fn update_counters(&mut self, current_offset: u64, num_docs: u64) {
        self.counters.num_docs_processed += num_docs;
        self.counters.current_offset = Some(current_offset);
        self.counters.previous_offset = Some(current_offset);
    }
}

#[async_trait]
impl Source for IngestApiSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let fetch_req = FetchRequest {
            index_id: self.ctx.index_id.clone(),
            start_after: self.counters.current_offset,
            num_bytes_limit: self
                .params
                .batch_num_bytes_limit
                .or(Some(BATCH_NUM_BYTES_LIMIT)),
        };
        let FetchResponse {
            first_position: first_position_opt,
            doc_batch: doc_batch_opt,
        } = self
            .ingest_api_service
            .ask_for_res(fetch_req)
            .await
            .map_err(anyhow::Error::from)?;

        // The `first_position_opt` being none means the doc_batch is empty and there is
        // no more document available, at least for the time being.
        // That is, we have consumed all pending docs in the queue and need to
        // make the client wait a bit before pooling again.
        let (first_position, doc_batch) = if let Some(first_position) = first_position_opt {
            (first_position, doc_batch_opt.unwrap())
        } else {
            return Ok(INGEST_API_POLLING_COOL_DOWN);
        };

        // TODO use a timestamp (in the raw doc batch) given by at ingest time to be more accurate.
        let mut raw_doc_batch = RawDocBatch::default();
        raw_doc_batch.docs.extend(
            iter_doc_payloads(&doc_batch).map(|buff| String::from_utf8_lossy(buff).to_string()),
        );
        let current_offset = first_position + raw_doc_batch.docs.len() as u64 - 1;
        let partition_id = self.partition_id.clone();
        raw_doc_batch
            .checkpoint_delta
            .record_partition_delta(
                partition_id,
                Position::from(self.counters.previous_offset.unwrap_or(0)),
                Position::from(current_offset),
            )
            .unwrap();

        self.update_counters(current_offset, raw_doc_batch.docs.len() as u64);
        ctx.send_message(batch_sink, raw_doc_batch).await?;
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &self,
        checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        if let Some(Position::Offset(offset_str)) =
            checkpoint.position_for_partition(&self.partition_id)
        {
            let up_to_position_included = offset_str.parse::<u64>()?;
            let suggest_truncate_req = SuggestTruncateRequest {
                index_id: self.ctx.index_id.clone(),
                up_to_position_included,
            };
            self.ingest_api_service
                .ask_for_res(suggest_truncate_req)
                .await
                .map_err(anyhow::Error::from)?;
        }
        Ok(())
    }

    fn name(&self) -> String {
        "IngestApiSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::to_value(&self.counters).unwrap()
    }
}

pub struct IngestApiSourceFactory;

#[async_trait]
impl TypedSourceFactory for IngestApiSourceFactory {
    type Source = IngestApiSource;
    type Params = IngestApiSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: IngestApiSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        IngestApiSource::try_new(ctx, params, checkpoint).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{SourceConfig, SourceParams, INGEST_API_SOURCE_ID};
    use quickwit_ingest_api::{add_doc, init_ingest_api};
    use quickwit_metastore::checkpoint::{SourceCheckpoint, SourceCheckpointDelta};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::ingest_api::{DocBatch, IngestRequest};

    use super::*;
    use crate::source::SourceActor;

    fn make_ingest_request(index_id: String, num_batch: u64, batch_size: usize) -> IngestRequest {
        let mut doc_batches = vec![];
        let mut doc_id = 0usize;
        for _ in 0..num_batch {
            let mut doc_batch = DocBatch {
                index_id: index_id.clone(),
                ..Default::default()
            };
            while doc_batch.doc_lens.len() < batch_size {
                add_doc(format!("{:0>4}", doc_id).as_bytes(), &mut doc_batch);
                doc_id += 1;
            }
            doc_batches.push(doc_batch);
        }
        IngestRequest { doc_batches }
    }

    fn make_source_config(ingest_api_source_params: IngestApiSourceParams) -> SourceConfig {
        SourceConfig {
            source_id: INGEST_API_SOURCE_ID.to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::IngestApi(ingest_api_source_params),
        }
    }

    #[tokio::test]
    async fn test_ingest_api_source() -> anyhow::Result<()> {
        let universe = Universe::new();
        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-ingest-api-source");
        let temp_dir = tempfile::tempdir()?;
        let queues_dir_path = temp_dir.path();

        let ingest_api_service = init_ingest_api(&universe, queues_dir_path).await?;
        let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
        let params = IngestApiSourceParams {
            batch_num_bytes_limit: Some(4 * 500),
        };
        let source_config = make_source_config(params.clone());
        let ctx = SourceExecutionContext::for_test(
            metastore,
            &index_id,
            queues_dir_path.to_path_buf(),
            source_config,
        );
        let ingest_api_source =
            IngestApiSource::try_new(ctx, params, SourceCheckpoint::default()).await?;
        let ingest_api_source_actor = SourceActor {
            source: Box::new(ingest_api_source),
            doc_processor_mailbox,
        };
        let (_ingest_api_source_mailbox, ingest_api_source_handle) =
            universe.spawn_builder().spawn(ingest_api_source_actor);

        let ingest_req = make_ingest_request(index_id.clone(), 2, 1000);
        ingest_api_service
                .ask_for_res(ingest_req)
                .await
                .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let counters = ingest_api_source_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(
            counters,
            serde_json::json!({
                "previous_offset": 1999u64,
                "current_offset": 1999u64,
                "num_docs_processed": 2000u64
            })
        );
        let doc_batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert_eq!(doc_batches.len(), 4);
        assert!(doc_batches[1].docs[0].starts_with("0501"));
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_api_source_resume_from_checkpoint() -> anyhow::Result<()> {
        let universe = Universe::new();
        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-ingest-api-source");
        let temp_dir = tempfile::tempdir()?;
        let queues_dir_path = temp_dir.path();
        let ingest_api_service = init_ingest_api(&universe, queues_dir_path).await?;

        let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
        let mut checkpoint = SourceCheckpoint::default();
        let partition_id = PartitionId::from(index_id.clone());
        let checkpoint_delta = SourceCheckpointDelta::from_partition_delta(
            partition_id,
            Position::from(0u64),
            Position::from(1200u64),
        );
        checkpoint.try_apply_delta(checkpoint_delta)?;

        let params = IngestApiSourceParams {
            batch_num_bytes_limit: None,
        };
        let source_config = make_source_config(params.clone());
        let ctx = SourceExecutionContext::for_test(
            metastore,
            &index_id,
            queues_dir_path.to_path_buf(),
            source_config,
        );
        let ingest_api_source = IngestApiSource::try_new(ctx, params, checkpoint).await?;
        let ingest_api_source_actor = SourceActor {
            source: Box::new(ingest_api_source),
            doc_processor_mailbox,
        };
        let (_ingest_api_source_mailbox, ingest_api_source_handle) =
            universe.spawn_builder().spawn(ingest_api_source_actor);

        let ingest_req = make_ingest_request(index_id.clone(), 4, 1000);
        ingest_api_service
            .ask_for_res(ingest_req)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let counters = ingest_api_source_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(
            counters,
            serde_json::json!({
                "previous_offset": 3999u64,
                "current_offset": 3999u64,
                "num_docs_processed": 2799u64
            })
        );
        let doc_batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert_eq!(doc_batches.len(), 1);
        assert!(doc_batches[0].docs[0].starts_with("1201"));
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_api_source_with_one_doc() -> anyhow::Result<()> {
        let universe = Universe::new();
        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-ingest-api-source");
        let temp_dir = tempfile::tempdir()?;
        let queues_dir_path = temp_dir.path();
        let ingest_api_service = init_ingest_api(&universe, queues_dir_path).await?;

        let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
        let params = IngestApiSourceParams {
            batch_num_bytes_limit: None,
        };
        let source_config = make_source_config(params.clone());
        let ctx = SourceExecutionContext::for_test(
            metastore,
            &index_id,
            queues_dir_path.to_path_buf(),
            source_config,
        );
        let ingest_api_source =
            IngestApiSource::try_new(ctx, params, SourceCheckpoint::default()).await?;
        let ingest_api_source_actor = SourceActor {
            source: Box::new(ingest_api_source),
            doc_processor_mailbox,
        };
        let (_ingest_api_source_mailbox, ingest_api_source_handle) =
            universe.spawn_builder().spawn(ingest_api_source_actor);

        let ingest_req = make_ingest_request(index_id.clone(), 1, 1);
        ingest_api_service
            .ask_for_res(ingest_req)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let counters = ingest_api_source_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(
            counters,
            serde_json::json!({
                "previous_offset": 0u64,
                "current_offset": 0u64,
                "num_docs_processed": 1u64
            })
        );
        let doc_batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert_eq!(doc_batches.len(), 1);
        assert!(doc_batches[0].docs[0].starts_with("0000"));
        Ok(())
    }
}
