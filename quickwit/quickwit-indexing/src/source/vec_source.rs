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

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::VecSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpointDelta};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{Position, SourceId};
use serde_json::Value as JsonValue;
use tracing::info;

use super::BatchBuilder;
use crate::actors::DocProcessor;
use crate::source::{Source, SourceContext, SourceRuntime, TypedSourceFactory};

pub struct VecSource {
    source_id: SourceId,
    source_params: VecSourceParams,
    next_item_idx: usize,
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
        source_runtime: SourceRuntime,
        source_params: VecSourceParams,
    ) -> anyhow::Result<Self::Source> {
        let checkpoint = source_runtime.fetch_checkpoint().await?;
        let partition = PartitionId::from(source_params.partition.as_str());
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
            source_id: source_runtime.pipeline_id.source_id,
            source_params,
            partition,
            next_item_idx,
        })
    }
}

fn position_from_offset(offset: usize) -> Position {
    if offset == 0 {
        return Position::Beginning;
    }
    Position::offset(offset - 1)
}

#[async_trait]
impl Source for VecSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let mut batch_builder = BatchBuilder::new(SourceType::Vec);

        for doc in self.source_params.docs[self.next_item_idx..]
            .iter()
            .take(self.source_params.batch_num_docs)
            .cloned()
        {
            batch_builder.add_doc(doc);
        }
        if batch_builder.docs.is_empty() {
            info!("reached end of source");
            ctx.send_exit_with_success(batch_sink).await?;
            return Err(ActorExitStatus::Success);
        }
        let from_item_idx = self.next_item_idx;
        self.next_item_idx += batch_builder.docs.len();
        let to_item_idx = self.next_item_idx;

        batch_builder.checkpoint_delta = SourceCheckpointDelta::from_partition_delta(
            self.partition.clone(),
            position_from_offset(from_item_idx),
            position_from_offset(to_item_idx),
        )
        .unwrap();
        ctx.send_message(batch_sink, batch_builder.build()).await?;

        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("{self:?}")
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

    use bytes::Bytes;
    use quickwit_actors::{Actor, Command, Universe};
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_proto::types::IndexUid;
    use serde_json::json;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::SourceActor;
    use crate::source::tests::SourceRuntimeBuilder;

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
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_config = SourceConfig {
            source_id: "test-vec-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::Vec(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let vec_source = VecSourceFactory::typed_create_source(source_runtime, params).await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            doc_processor_mailbox,
        };
        assert_eq!(
            vec_source_actor.name(),
            r#"VecSource { source_id: "test-vec-source" }"#
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
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_config = SourceConfig {
            source_id: "test-vec-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::Vec(params.clone()),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_delta = SourceCheckpointDelta::from_range(0u64..2u64);
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_mock_metastore(Some(source_delta))
            .build();
        let vec_source = VecSourceFactory::typed_create_source(source_runtime, params).await?;
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
