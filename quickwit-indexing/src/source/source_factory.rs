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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use thiserror::Error;

use super::Source;
use crate::source::SourceExecutionContext;

#[async_trait]
pub trait SourceFactory: 'static + Send + Sync {
    async fn create_source(
        &self,
        ctx: Arc<SourceExecutionContext>,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Box<dyn Source>>;
}

#[async_trait]
pub trait TypedSourceFactory: Send + Sync + 'static {
    type Source: Source;
    type Params: serde::de::DeserializeOwned + Send + Sync + 'static;
    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: Self::Params,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source>;
}

#[async_trait]
impl<T: TypedSourceFactory> SourceFactory for T {
    async fn create_source(
        &self,
        ctx: Arc<SourceExecutionContext>,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Box<dyn Source>> {
        let typed_params: T::Params = serde_json::from_value(ctx.config.params())?;
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
        "Unknown source type `{requested_source_type}` (available source types are \
         {available_source_types})."
    )]
    UnknownSourceType {
        requested_source_type: String,
        available_source_types: String, //< a comma separated list with the available source_type.
    },
    #[error("Failed to create source `{source_id}` of type `{source_type}`. Cause: {error:?}")]
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
        ctx: Arc<SourceExecutionContext>,
        checkpoint: SourceCheckpoint,
    ) -> Result<Box<dyn Source>, SourceLoaderError> {
        let source_type = ctx.config.source_type().to_string();
        let source_id = ctx.config.source_id.clone();

        let source_factory = self
            .type_to_factory
            .get(ctx.config.source_type())
            .ok_or_else(|| SourceLoaderError::UnknownSourceType {
                requested_source_type: ctx.config.source_type().to_string(),
                available_source_types: self.type_to_factory.keys().join(", "),
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
pub mod test_helpers {
    use std::sync::Arc;

    use quickwit_metastore::FileBackedMetastore;

    pub async fn metastore_for_test() -> FileBackedMetastore {
        use quickwit_storage::RamStorage;
        FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None)
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::Metastore;

    use super::*;
    use crate::source::quickwit_supported_sources;

    #[tokio::test]
    async fn test_source_loader_success() -> anyhow::Result<()> {
        let metastore: Arc<dyn Metastore> = Arc::new(test_helpers::metastore_for_test().await);
        let source_loader = quickwit_supported_sources();
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            source_params: SourceParams::void(),
        };
        source_loader
            .load_source(
                Arc::new(SourceExecutionContext {
                    metastore: metastore.clone(),
                    index_id: "test-index".to_string(),
                    config: source_config,
                }),
                SourceCheckpoint::default(),
            )
            .await?;
        Ok(())
    }
}
