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

use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox, HEARTBEAT};
use quickwit_config::VoidSourceParams;

use crate::actors::Indexer;
use crate::source::{Source, SourceContext, TypedSourceFactory};

pub struct VoidSource;

#[async_trait]
impl Source for VoidSource {
    async fn emit_batches(
        &mut self,
        _: &Mailbox<Indexer>,
        _: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        tokio::time::sleep(HEARTBEAT / 2).await;
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        "VoidSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::Value::Object(Default::default())
    }
}

pub struct VoidSourceFactory;

#[async_trait]
impl TypedSourceFactory for VoidSourceFactory {
    type Source = VoidSource;

    type Params = VoidSourceParams;

    async fn typed_create_source(
        _source_id: String,
        _params: VoidSourceParams,
        _checkpoint: quickwit_metastore::checkpoint::SourceCheckpoint,
    ) -> anyhow::Result<VoidSource> {
        Ok(VoidSource)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::{create_test_mailbox, Health, Supervisable, Universe};
    use quickwit_config::SourceParams;
    use quickwit_metastore::checkpoint::SourceCheckpoint;
    use serde_json::json;

    use super::*;
    use crate::source::{quickwit_supported_sources, SourceActor, SourceConfig};

    #[tokio::test]
    async fn test_void_source_loading() -> anyhow::Result<()> {
        let source_config = SourceConfig {
            source_id: "void-test-source".to_string(),
            source_params: SourceParams::void(),
        };
        let source_loader = quickwit_supported_sources();
        let _ = source_loader
            .load_source(source_config.clone(), SourceCheckpoint::default())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_void_source_running() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, _) = create_test_mailbox();
        let void_source = VoidSourceFactory::typed_create_source(
            "my-void-source".to_string(),
            VoidSourceParams {},
            SourceCheckpoint::default(),
        )
        .await?;
        let void_source_actor = SourceActor {
            source: Box::new(void_source),
            batch_sink: mailbox,
        };
        let (_, void_source_handle) = universe.spawn_actor(void_source_actor).spawn();
        matches!(void_source_handle.health(), Health::Healthy);
        let (actor_termination, observed_state) = void_source_handle.quit().await;
        assert_eq!(observed_state, json!({}));
        matches!(actor_termination, ActorExitStatus::Quit);
        Ok(())
    }
}
