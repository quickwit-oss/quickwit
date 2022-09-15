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

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use tokio::runtime::Handle;

use crate::actors::Packager;
use crate::models::{IndexedSplit, IndexedSplitBatch, IndexedSplitBatchBuilder};

/// The index serializer takes a non-serialized split,
/// and serializes it before passing it to the packager.
///
/// This is usually a CPU heavy operation.
///
/// Depending on the data
/// (terms cardinality) and the index settings (sorted or not)
/// it can range from medium IO to IO heavy.
pub struct IndexSerializer {
    packager_mailbox: Mailbox<Packager>,
}

impl IndexSerializer {
    pub fn new(packager_mailbox: Mailbox<Packager>) -> Self {
        Self { packager_mailbox }
    }
}

#[async_trait]
impl Actor for IndexSerializer {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }
}

#[async_trait]
impl Handler<IndexedSplitBatchBuilder> for IndexSerializer {
    type Reply = ();

    async fn handle(
        &mut self,
        batch_builder: IndexedSplitBatchBuilder,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let splits: Vec<IndexedSplit> = {
            let _protect = ctx.protect_zone();
            batch_builder
                .splits
                .into_iter()
                .map(|split_builder| split_builder.finalize())
                .collect::<Result<_, _>>()?
        };
        let indexed_split_batch = IndexedSplitBatch {
            splits,
            checkpoint_delta: batch_builder.checkpoint_delta,
            publish_lock: batch_builder.publish_lock,
            date_of_birth: batch_builder.date_of_birth,
        };
        ctx.send_message(&self.packager_mailbox, indexed_split_batch)
            .await?;
        Ok(())
    }
}
