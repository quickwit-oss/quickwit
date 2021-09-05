// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::convert::TryInto;

use crate::models::IndexerMessage;
use crate::models::RawDocBatch;
use crate::source::Source;
use crate::source::SourceContext;
use crate::source::TypedSourceFactory;
use async_trait::async_trait;
use quickwit_actors::ActorExitStatus;
use quickwit_actors::Mailbox;
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::checkpoint::CheckpointDelta;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_metastore::checkpoint::Position;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Deserialize, Serialize)]
pub struct VecSourceParams {
    pub items: Vec<String>,
    pub batch_num_docs: usize,
    #[serde(default)]
    pub partition: String,
}

pub struct VecSource {
    next_item_id: usize,
    params: VecSourceParams,
    partition: PartitionId,
}
pub struct VecSourceFactory;

fn extract_next_item_id(partition: &PartitionId, checkpoint: &Checkpoint) -> anyhow::Result<usize> {
    if let Some(position) = checkpoint.position_for_partition(partition).cloned() {
        if position.as_str().is_empty() {
            Ok(0)
        } else {
            let last_seen: u64 = position.try_into()?;
            Ok(last_seen as usize + 1)
        }
    } else {
        Ok(0)
    }
}

#[async_trait]
impl TypedSourceFactory for VecSourceFactory {
    type Source = VecSource;
    type Params = VecSourceParams;
    async fn typed_create_source(
        params: VecSourceParams,
        checkpoint: Checkpoint,
    ) -> anyhow::Result<Self::Source> {
        let partition = PartitionId::from(params.partition.as_str());
        let next_item_id = extract_next_item_id(&partition, &checkpoint)?;
        Ok(VecSource {
            next_item_id,
            params,
            partition,
        })
    }
}

fn position_from_offset(offset: u64) -> Position {
    if offset == 0 {
        return Position::default();
    }
    Position::from(offset - 1)
}

#[async_trait]
impl Source for VecSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<IndexerMessage>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let line_docs: Vec<String> = self.params.items[self.next_item_id..]
            .iter()
            .take(self.params.batch_num_docs)
            .cloned()
            .collect();
        let start_item_id = self.next_item_id as u64;
        self.next_item_id += line_docs.len();
        let end_item_id = self.next_item_id as u64;
        let mut checkpoint_delta = CheckpointDelta::default();
        checkpoint_delta.add_partition(
            self.partition.clone(),
            position_from_offset(start_item_id),
            position_from_offset(end_item_id),
        );
        if line_docs.is_empty() {
            info!("End of source");
            ctx.send_message(batch_sink, IndexerMessage::EndOfSource)
                .await?;
            return Err(ActorExitStatus::Success);
        }
        let batch = RawDocBatch {
            docs: line_docs,
            checkpoint_delta,
        };
        ctx.send_message(batch_sink, IndexerMessage::from(batch))
            .await?;
        Ok(())
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::json!({
            "next_item_id": self.next_item_id
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SourceActor;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::Universe;
    use serde_json::json;

    #[tokio::test]
    async fn test_vec_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let items = std::iter::repeat_with(|| "{}".to_string())
            .take(100)
            .collect();
        let params = VecSourceParams {
            items,
            batch_num_docs: 3,
            partition: "partition".to_string(),
        };
        let vec_source =
            VecSourceFactory::typed_create_source(params, Checkpoint::default()).await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            batch_sink: mailbox,
        };
        let (_vec_source_mailbox, vec_source_handle) =
            universe.spawn_actor(vec_source_actor).spawn_async();
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, json!({"next_item_id": 100}));
        let batches = inbox.drain_available_message_for_test();
        assert_eq!(batches.len(), 35);
        assert!(
            matches!(&batches[1], &IndexerMessage::Batch(ref raw_batch) if format!("{:?}", raw_batch.checkpoint_delta) == "∆(partition:(00000000000000000002..00000000000000000005])")
        );
        assert!(matches!(&batches[34], &IndexerMessage::EndOfSource));
        Ok(())
    }

    #[tokio::test]
    async fn test_vec_source_from_checkpoint() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let items = (0..10).map(|i| format!("{}", i)).collect();
        let params = VecSourceParams {
            items,
            batch_num_docs: 3,
            partition: "".to_string(),
        };
        let mut checkpoint = Checkpoint::default();
        checkpoint.try_apply_delta(CheckpointDelta::from(0u64..2u64))?;

        let vec_source = VecSourceFactory::typed_create_source(params, checkpoint).await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            batch_sink: mailbox,
        };
        let (_vec_source_mailbox, vec_source_handle) =
            universe.spawn_actor(vec_source_actor).spawn_async();
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, json!({"next_item_id": 10}));
        let messages = inbox.drain_available_message_for_test();
        assert!(
            matches!(&messages[0], &IndexerMessage::Batch(ref raw_batch) if &raw_batch.docs[0] == "2")
        );
        Ok(())
    }
}
