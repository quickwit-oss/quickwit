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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::VecSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint, SourceCheckpointDelta};
use quickwit_proto::types::Position;
use serde_json::Value as JsonValue;
use tracing::info;

use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, SourceRuntimeArgs, TypedSourceFactory};

pub struct VecSource {
    source_id: String,
    next_item_idx: usize,
    params: VecSourceParams,
    partition: PartitionId,
}

impl fmt::Debug for VecSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("VecSource")
            .field("source_id", &self.source_id)
            .finish()
    }
}

pub struct VecSourceFactory;

#[async_trait]
impl TypedSourceFactory for VecSourceFactory {
    type Source = VecSource;
    type Params = VecSourceParams;
    async fn typed_create_source(
        ctx: Arc<SourceRuntimeArgs>,
        params: VecSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        let partition = PartitionId::from(params.partition.as_str());
        let next_item_idx = checkpoint
            .position_for_partition(&partition)
            .map(|position| {
                position
                    .as_usize()
                    .expect("offset should be stored as usize")
                    + 1
            })
            .unwrap_or(0);
        Ok(VecSource {
            source_id: ctx.source_id().to_string(),
            next_item_idx,
            params,
            partition,
        })
    }
}

fn position_from_offset(offset: usize) -> Position {
    if offset == 0 {
        return Position::Beginning;
    }
    Position::from(offset - 1)
}

#[async_trait]
impl Source for VecSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let mut doc_batch = RawDocBatch::default();

        doc_batch.docs.extend(
            self.params.docs[self.next_item_idx..]
                .iter()
                .take(self.params.batch_num_docs)
                .cloned(),
        );
        if doc_batch.docs.is_empty() {
            info!("reached end of source");
            ctx.send_exit_with_success(batch_sink).await?;
            return Err(ActorExitStatus::Success);
        }
        let from_item_idx = self.next_item_idx;
        self.next_item_idx += doc_batch.docs.len();
        let to_item_idx = self.next_item_idx;
        doc_batch.checkpoint_delta = SourceCheckpointDelta::from_partition_delta(
            self.partition.clone(),
            position_from_offset(from_item_idx),
            position_from_offset(to_item_idx),
        )
        .unwrap();
        ctx.send_message(batch_sink, doc_batch).await?;
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("VecSource {{ source_id={} }}", self.source_id)
    }

    fn observable_state(&self) -> JsonValue {
        serde_json::json!({
            "next_item_idx": self.next_item_idx,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::PathBuf;

    use bytes::Bytes;
    use quickwit_actors::{Actor, Command, Universe};
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::types::IndexUid;
    use serde_json::json;

    use super::*;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_vec_source() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let docs = std::iter::repeat_with(|| Bytes::from_static(b"{}"))
            .take(100)
            .collect();
        let params = VecSourceParams {
            docs,
            batch_num_docs: 3,
            partition: "partition".to_string(),
        };
        let source_config = SourceConfig {
            source_id: "test-vec-source".to_string(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::Vec(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let metastore = metastore_for_test();
        let vec_source = VecSourceFactory::typed_create_source(
            SourceRuntimeArgs::for_test(
                IndexUid::new("test-index"),
                source_config,
                metastore,
                PathBuf::from("./queues"),
            ),
            params,
            SourceCheckpoint::default(),
        )
        .await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            doc_processor_mailbox,
        };
        assert_eq!(
            vec_source_actor.name(),
            "VecSource { source_id=test-vec-source }"
        );
        let (_vec_source_mailbox, vec_source_handle) =
            universe.spawn_builder().spawn(vec_source_actor);
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, json!({"next_item_idx": 100u64}));
        let batches = doc_processor_inbox.drain_for_test();
        assert_eq!(batches.len(), 35);
        let raw_batch = batches[1].downcast_ref::<RawDocBatch>().unwrap();
        assert_eq!(
            format!("{:?}", raw_batch.checkpoint_delta),
            "∆(partition:(00000000000000000002..00000000000000000005])"
        );
        assert!(matches!(
            &batches[34].downcast_ref::<Command>().unwrap(),
            &Command::ExitWithSuccess
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_vec_source_from_checkpoint() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let docs = (0..10).map(|i| Bytes::from(format!("{i}"))).collect();
        let params = VecSourceParams {
            docs,
            batch_num_docs: 3,
            partition: "".to_string(),
        };
        let mut checkpoint = SourceCheckpoint::default();
        checkpoint.try_apply_delta(SourceCheckpointDelta::from_range(0u64..2u64))?;

        let source_config = SourceConfig {
            source_id: "test-vec-source".to_string(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::Vec(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let metastore = metastore_for_test();
        let vec_source = VecSourceFactory::typed_create_source(
            SourceRuntimeArgs::for_test(
                IndexUid::new("test-index"),
                source_config,
                metastore,
                PathBuf::from("./queues"),
            ),
            params,
            checkpoint,
        )
        .await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            doc_processor_mailbox,
        };
        let (_vec_source_mailbox, vec_source_handle) =
            universe.spawn_builder().spawn(vec_source_actor);
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, json!({"next_item_idx": 10}));
        let messages = doc_processor_inbox.drain_for_test();
        let batch = messages[0].downcast_ref::<RawDocBatch>().unwrap();
        assert_eq!(&batch.docs[0], "2");
        Ok(())
    }
}
