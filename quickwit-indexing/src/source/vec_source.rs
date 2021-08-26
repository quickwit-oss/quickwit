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
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct VecSourceParams {
    items: Vec<String>,
    batch_num_docs: usize,
}

pub struct VecSource {
    next_item_id: usize,
    params: VecSourceParams,
}
pub struct VecSourceFactory;

fn extract_next_item_id(checkpoint: &Checkpoint) -> anyhow::Result<usize> {
    let default_partition = PartitionId::default();
    if let Some(position) = checkpoint
        .position_for_partition(&default_partition)
        .cloned()
    {
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
        let next_item_id = extract_next_item_id(&checkpoint)?;
        Ok(VecSource {
            next_item_id,
            params,
        })
    }
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
        let checkpoint_delta = CheckpointDelta::from(start_item_id..end_item_id);
        if line_docs.is_empty() {
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
        };
        let vec_source =
            VecSourceFactory::typed_create_source(params, Checkpoint::default()).await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            batch_sink: mailbox,
        };
        let (_vec_source_mailbox, vec_source_handle) = universe.spawn_async_actor(vec_source_actor);
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, json!({"next_item_id": 100}));
        let batches = inbox.drain_available_message_for_test();
        assert_eq!(batches.len(), 34);
        let expected_checkpoint_delta = CheckpointDelta::from(3u64..6u64);
        assert!(
            matches!(&batches[1], &IndexerMessage::Batch(ref raw_batch) if raw_batch.checkpoint_delta == expected_checkpoint_delta)
        );
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
        };
        let mut checkpoint = Checkpoint::default();
        checkpoint.try_apply_delta(CheckpointDelta::from(0u64..2u64))?;

        let vec_source = VecSourceFactory::typed_create_source(params, checkpoint).await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            batch_sink: mailbox,
        };
        let (_vec_source_mailbox, vec_source_handle) = universe.spawn_async_actor(vec_source_actor);
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
