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
use quickwit_actors::{ActorExitStatus, Mailbox, HEARTBEAT};
use serde::{Deserialize, Serialize};

use crate::models::IndexerMessage;
use crate::source::{Source, SourceContext, TypedSourceFactory};

pub struct EmptySource;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilePosition {
    pub num_bytes: u64,
}

#[async_trait]
impl Source for EmptySource {
    async fn emit_batches(
        &mut self,
        _: &Mailbox<IndexerMessage>,
        _: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        tokio::time::sleep(HEARTBEAT / 2).await;
        Ok(())
    }

    fn name(&self) -> String {
        "EmptySource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::to_value(0).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EmptySourceParams;

pub struct EmptySourceFactory;

#[async_trait]
impl TypedSourceFactory for EmptySourceFactory {
    type Source = EmptySource;

    type Params = EmptySourceParams;

    async fn typed_create_source(
        _: EmptySourceParams,
        _: quickwit_metastore::checkpoint::Checkpoint,
    ) -> anyhow::Result<EmptySource> {
        let empty_source = EmptySource {};
        Ok(empty_source)
    }
}
