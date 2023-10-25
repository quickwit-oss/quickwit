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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use thiserror::Error;

use super::Source;
use crate::source::SourceRuntimeArgs;

#[async_trait]
pub trait SourceFactory: 'static + Send + Sync {
    async fn create_source(
        &self,
        ctx: Arc<SourceRuntimeArgs>,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Box<dyn Source>>;
}

#[async_trait]
pub trait TypedSourceFactory: Send + Sync + 'static {
    type Source: Source;
    type Params: serde::de::DeserializeOwned + Send + Sync + 'static;
    async fn typed_create_source(
        ctx: Arc<SourceRuntimeArgs>,
        params: Self::Params,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source>;
}

#[async_trait]
impl<T: TypedSourceFactory> SourceFactory for T {
    async fn create_source(
        &self,
        ctx: Arc<SourceRuntimeArgs>,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Box<dyn Source>> {
        let typed_params: T::Params = serde_json::from_value(ctx.source_config.params())?;
        let file_source = Self::typed_create_source(ctx, typed_params, checkpoint).await?;
        Ok(Box::new(file_source))
    }
}

#[derive(Default)]
pub struct SourceLoader {
    type_to_factory: HashMap<String, Box<dyn SourceFactory>>,
}

#[derive(Error, Debug)]
pub enum SourceLoaderError {
    #[error(
        "unknown source type `{requested_source_type}` (available source types are \
         {available_source_types})"
    )]
    UnknownSourceType {
        requested_source_type: String,
        available_source_types: String, //< a comma separated list with the available source_type.
    },
    #[error("failed to create source `{source_id}` of type `{source_type}`. Cause: {error:?}")]
    FailedToCreateSource {
        source_id: String,
        source_type: String,
        #[source]
        error: anyhow::Error,
    },
}

impl SourceLoader {
    pub fn add_source<S: ToString, F: SourceFactory>(&mut self, source: S, factory: F) {
        self.type_to_factory
            .insert(source.to_string(), Box::new(factory));
    }

    pub async fn load_source(
        &self,
        ctx: Arc<SourceRuntimeArgs>,
        checkpoint: SourceCheckpoint,
    ) -> Result<Box<dyn Source>, SourceLoaderError> {
        let source_type = ctx.source_config.source_type().as_str().to_string();
        let source_id = ctx.source_id().to_string();
        let source_factory = self.type_to_factory.get(&source_type).ok_or_else(|| {
            SourceLoaderError::UnknownSourceType {
                requested_source_type: source_type.clone(),
                available_source_types: self.type_to_factory.keys().join(", "),
            }
        })?;
        source_factory
            .create_source(ctx, checkpoint)
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
    use std::path::PathBuf;

    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::types::IndexUid;

    use super::*;
    use crate::source::quickwit_supported_sources;

    #[tokio::test]
    async fn test_source_loader_success() -> anyhow::Result<()> {
        let metastore = metastore_for_test();
        let source_loader = quickwit_supported_sources();
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        source_loader
            .load_source(
                SourceRuntimeArgs::for_test(
                    IndexUid::new("test-index"),
                    source_config,
                    metastore,
                    PathBuf::from("./queues"),
                ),
                SourceCheckpoint::default(),
            )
            .await?;
        Ok(())
    }
}
