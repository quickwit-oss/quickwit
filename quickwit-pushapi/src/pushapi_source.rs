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

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_indexing::actors::Indexer;
use quickwit_indexing::models::RawDocBatch;
use quickwit_indexing::source::{Source, SourceContext};
use quickwit_metastore::checkpoint::{CheckpointDelta, PartitionId, Position, SourceCheckpoint};
use quickwit_proto::push_api::{FetchRequest, FetchResponse};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{iter_doc_payloads, PushApiService};

/// Cut a new batch as soon as we have read BATCH_NUM_BYTES_THRESHOLD.
const BATCH_NUM_BYTES_THRESHOLD: u64 = 500_000u64; // 0.5MB

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PushApiSourceTracker {
    pub current_offset: u64,
    pub num_docs_processed: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PushApiSourceParams {
    pub index_id: String,
}

pub struct PushApiSource {
    params: PushApiSourceParams,
    tracker: PushApiSourceTracker,
    push_api_mailbox: Mailbox<PushApiService>,
}

impl PushApiSource {
    pub fn make(
        push_api_mailbox: Mailbox<PushApiService>,
        index_id: String,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let partition_id = PartitionId::from(index_id.clone());
        let offset = if let Some(Position::Offset(offset_str)) =
            checkpoint.position_for_partition(&partition_id).cloned()
        {
            offset_str.parse::<u64>()?
        } else {
            0
        };

        let push_api_source = PushApiSource {
            tracker: PushApiSourceTracker {
                current_offset: offset,
                num_docs_processed: 0,
            },
            params: PushApiSourceParams { index_id },
            push_api_mailbox,
        };
        Ok(push_api_source)
    }
}

#[async_trait]
impl Source for PushApiSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<Indexer>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let start_after = if self.tracker.current_offset == 0 {
            None
        } else {
            Some(self.tracker.current_offset)
        };

        let fetch_req = FetchRequest {
            index_id: self.params.index_id.clone(),
            start_after,
            num_bytes_limit: Some(BATCH_NUM_BYTES_THRESHOLD),
        };
        let fetch_resp = self
            .push_api_mailbox
            .ask_for_res(fetch_req)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;

        let FetchResponse {
            first_position,
            doc_batch: doc_batch_opt,
        } = fetch_resp;
        if doc_batch_opt.is_none() {
            info!("End of queue");
            ctx.send_exit_with_success(batch_sink).await?;
            return Err(ActorExitStatus::Success);
        }

        let doc_batch = doc_batch_opt.unwrap();
        let docs = iter_doc_payloads(&doc_batch)
            .map(|buff| String::from_utf8_lossy(buff).to_string())
            .collect::<Vec<_>>();

        if !docs.is_empty() {
            self.tracker.num_docs_processed += docs.len() as u64;
            let from_position = first_position.unwrap_or(0);
            self.tracker.current_offset = from_position + docs.len() as u64;

            let mut checkpoint_delta = CheckpointDelta::default();
            let partition_id = PartitionId::from(self.params.index_id.as_str());
            checkpoint_delta
                .record_partition_delta(
                    partition_id,
                    Position::from(from_position),
                    Position::from(self.tracker.current_offset),
                )
                .unwrap();

            let raw_doc_batch = RawDocBatch {
                docs,
                checkpoint_delta,
            };
            ctx.send_message(batch_sink, raw_doc_batch).await?;
        }

        Ok(())
    }

    fn name(&self) -> String {
        "PushApiSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::to_value(&self.tracker).unwrap()
    }
}

// pub struct PushApiSourceFactory;

// #[async_trait]
// impl TypedSourceFactory for PushApiSourceFactory {
//     type Source = PushApiSource;
//     type Params = PushApiSourceParams;

//     // TODO handle checkpoint for files.
//     async fn typed_create_source(
//         params: PushApiSourceParams,
//         checkpoint: quickwit_metastore::checkpoint::SourceCheckpoint,
//     ) -> anyhow::Result<Self::Source> {
//         let partition_id = PartitionId::from(params.index_id);
//         let offset = if let Some(Position::Offset(offset_str)) =
//             checkpoint.position_for_partition(&partition_id).cloned()
//         {
//             offset_str.parse::<u64>()?
//         } else {
//             0
//         };

//         let push_api_source = PushApiSource {
//             tracker: PushApiSourceTracker {
//                 current_offset: offset,
//                 num_docs_processed: 0,
//             },
//             params,
//             push_api_mailbox: Mailbox<PushApiService>,
//         };
//         Ok(push_api_source)
//     }
// }

#[cfg(test)]
mod tests {
    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_indexing::source::SourceActor;
    use quickwit_metastore::checkpoint::SourceCheckpoint;
    use quickwit_proto::push_api::{DocBatch, IngestRequest};

    use super::*;
    use crate::{add_doc, spawn_push_api_actor};

    fn make_ingest_request(index_id: String) -> IngestRequest {
        let mut doc_batch = DocBatch {
            index_id,
            ..Default::default()
        };
        for _ in 0..2000 {
            add_doc(
                r#"{"body": "It takes two to lie; one to lie ..."}"#.as_bytes(),
                &mut doc_batch,
            );
        }
        IngestRequest {
            doc_batches: vec![doc_batch],
        }
    }

    #[tokio::test]
    async fn test_pushapi_source() -> anyhow::Result<()> {
        // quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let index_id = "my-index".to_string();

        let queue_path = tempfile::tempdir()?;

        let push_api_service = spawn_push_api_actor(&universe, queue_path.path())?;

        // add
        let ingest_req = make_ingest_request(index_id.clone());
        push_api_service
            .ask_for_res(ingest_req)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;

        let (mailbox, inbox) = create_test_mailbox();
        let push_api_source =
            PushApiSource::make(push_api_service, index_id, SourceCheckpoint::default())?;
        let push_api_source_actor = SourceActor {
            source: Box::new(push_api_source),
            batch_sink: mailbox,
        };
        let (_push_api_source_mailbox, push_api_source_handle) =
            universe.spawn_actor(push_api_source_actor).spawn();

        let (actor_termination, tracker) = push_api_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            tracker,
            serde_json::json!({
                "current_offset": 70u64,
                "num_lines_processed": 4u32
            })
        );
        let batch = inbox.drain_for_test();
        assert_eq!(batch.len(), 2);
        // assert!(matches!(
        //     batch[1].downcast_ref::<Command>().unwrap(),
        //     Command::ExitWithSuccess
        // ));
        Ok(())
    }

}
