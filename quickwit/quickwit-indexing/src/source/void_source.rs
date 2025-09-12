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

use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, HEARTBEAT, Mailbox};
use quickwit_config::VoidSourceParams;
use serde_json::Value as JsonValue;

use crate::actors::DocProcessor;
use crate::source::{Source, SourceContext, SourceRuntime, TypedSourceFactory};

pub struct VoidSource;

#[async_trait]
impl Source for VoidSource {
    async fn emit_batches(
        &mut self,
        _: &Mailbox<DocProcessor>,
        _: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        tokio::time::sleep(*HEARTBEAT / 2).await;
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        "VoidSource".to_string()
    }

    fn observable_state(&self) -> JsonValue {
        JsonValue::Object(Default::default())
    }
}

pub struct VoidSourceFactory;

#[async_trait]
impl TypedSourceFactory for VoidSourceFactory {
    type Source = VoidSource;

    type Params = VoidSourceParams;

    async fn typed_create_source(
        _source_runtime: SourceRuntime,
        _params: VoidSourceParams,
    ) -> anyhow::Result<VoidSource> {
        Ok(VoidSource)
    }
}

#[cfg(test)]
mod tests {

    use std::num::NonZeroUsize;

    use quickwit_actors::{Health, Supervisable, Universe};
    use quickwit_config::{SourceInputFormat, SourceParams};
    use quickwit_proto::types::IndexUid;
    use serde_json::json;

    use super::*;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{SourceActor, SourceConfig, quickwit_supported_sources};

    #[tokio::test]
    async fn test_void_source_loading() {
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_config = SourceConfig {
            source_id: "test-void-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let source = quickwit_supported_sources()
            .load_source(source_runtime)
            .await
            .unwrap();
        assert_eq!(source.name(), "VoidSource");
    }

    #[tokio::test]
    async fn test_void_source_running() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_config = SourceConfig {
            source_id: "test-void-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let void_source =
            VoidSourceFactory::typed_create_source(source_runtime, VoidSourceParams).await?;
        let (doc_processor_mailbox, _) = universe.create_test_mailbox();
        let void_source_actor = SourceActor {
            source: Box::new(void_source),
            doc_processor_mailbox,
        };
        let (_, void_source_handle) = universe.spawn_builder().spawn(void_source_actor);
        matches!(void_source_handle.check_health(true), Health::Healthy);
        let (actor_termination, observed_state) = void_source_handle.quit().await;
        assert_eq!(observed_state, json!({}));
        matches!(actor_termination, ActorExitStatus::Quit);
        Ok(())
    }
}
