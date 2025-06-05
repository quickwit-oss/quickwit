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

use std::collections::HashMap;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::SourceId;
use thiserror::Error;

use super::Source;
use crate::source::SourceRuntime;

#[async_trait]
pub trait SourceFactory: Send + Sync + 'static {
    async fn create_source(&self, source_runtime: SourceRuntime)
    -> anyhow::Result<Box<dyn Source>>;
}

#[async_trait]
pub trait TypedSourceFactory: Send + Sync + 'static {
    type Source: Source;
    type Params: serde::de::DeserializeOwned + Send + Sync + 'static;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        source_params: Self::Params,
    ) -> anyhow::Result<Self::Source>;
}

#[async_trait]
impl<T: TypedSourceFactory> SourceFactory for T {
    async fn create_source(
        &self,
        source_runtime: SourceRuntime,
    ) -> anyhow::Result<Box<dyn Source>> {
        let typed_params: T::Params =
            serde_json::from_value(source_runtime.source_config.params())?;
        let source = Self::typed_create_source(source_runtime, typed_params).await?;
        Ok(Box::new(source))
    }
}

#[derive(Default)]
pub struct SourceLoader {
    type_to_factory: HashMap<SourceType, Box<dyn SourceFactory>>,
}

#[derive(Error, Debug)]
pub enum SourceLoaderError {
    #[error(
        "unknown source type `{requested_source_type}` (available source types are \
         {available_source_types})"
    )]
    UnknownSourceType {
        requested_source_type: SourceType,
        available_source_types: String, //< a comma separated list with the available source_type.
    },
    #[error("failed to create source `{source_id}` of type `{source_type}`. Cause: {error:?}")]
    FailedToCreateSource {
        source_id: SourceId,
        source_type: SourceType,
        #[source]
        error: anyhow::Error,
    },
}

impl SourceLoader {
    pub fn add_source<F: SourceFactory>(&mut self, source_type: SourceType, source_factory: F) {
        self.type_to_factory
            .insert(source_type, Box::new(source_factory));
    }

    pub async fn load_source(
        &self,
        source_runtime: SourceRuntime,
    ) -> Result<Box<dyn Source>, SourceLoaderError> {
        let source_type = source_runtime.source_config.source_type();
        let source_id = source_runtime.source_id().to_string();
        let source_factory = self.type_to_factory.get(&source_type).ok_or_else(|| {
            SourceLoaderError::UnknownSourceType {
                requested_source_type: source_type,
                available_source_types: self.type_to_factory.keys().join(", "),
            }
        })?;
        source_factory
            .create_source(source_runtime)
            .await
            .map_err(|error| SourceLoaderError::FailedToCreateSource {
                source_type,
                source_id,
                error,
            })
    }
}

#[cfg(test)]
mod tests {

    use std::num::NonZeroUsize;

    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_proto::types::IndexUid;

    use crate::source::quickwit_supported_sources;
    use crate::source::tests::SourceRuntimeBuilder;

    #[tokio::test]
    async fn test_source_loader_success() -> anyhow::Result<()> {
        let source_loader = quickwit_supported_sources();
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        source_loader.load_source(source_runtime).await?;
        Ok(())
    }
}
