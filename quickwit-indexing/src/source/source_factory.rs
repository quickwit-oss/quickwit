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

use std::collections::HashMap;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use thiserror::Error;

use super::Source;
use crate::source::SourceConfig;

#[async_trait]
pub trait SourceFactory: 'static + Send + Sync {
    async fn create_source(
        &self,
        params: serde_json::Value,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Box<dyn Source>>;
}

#[async_trait]
pub trait TypedSourceFactory: Send + Sync + 'static {
    type Source: Source;
    type Params: serde::de::DeserializeOwned + Send + Sync + 'static;
    async fn typed_create_source(
        params: Self::Params,
        checkpoint: quickwit_metastore::checkpoint::SourceCheckpoint,
    ) -> anyhow::Result<Self::Source>;
}

#[async_trait]
impl<T: TypedSourceFactory> SourceFactory for T {
    async fn create_source(
        &self,
        params: serde_json::Value,
        checkpoint: quickwit_metastore::checkpoint::SourceCheckpoint,
    ) -> anyhow::Result<Box<dyn Source>> {
        let typed_params: T::Params = serde_json::from_value(params)?;
        let file_source = Self::typed_create_source(typed_params, checkpoint).await?;
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
        source_config: SourceConfig,
        checkpoint: SourceCheckpoint,
    ) -> Result<Box<dyn Source>, SourceLoaderError> {
        let source_factory = self
            .type_to_factory
            .get(&source_config.source_type)
            .ok_or_else(|| SourceLoaderError::UnknownSourceType {
                requested_source_type: source_config.source_type.clone(),
                available_source_types: self.type_to_factory.keys().join(", "),
            })?;
        let SourceConfig {
            source_id,
            source_type,
            params,
        } = source_config;
        source_factory
            .create_source(params, checkpoint)
            .await
            .map_err(|error| SourceLoaderError::FailedToCreateSource {
                source_id,
                source_type,
                error,
            })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::source::quickwit_supported_sources;

    #[tokio::test]
    async fn test_source_loader_success() -> anyhow::Result<()> {
        let source_loader = quickwit_supported_sources();
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            source_type: "vec".to_string(),
            params: json!({"items": [], "batch_num_docs": 3}),
        };
        source_loader
            .load_source(source_config, SourceCheckpoint::default())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_source_loader_missing_type() {
        let source_loader = quickwit_supported_sources();
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            source_type: "vec2".to_string(),
            params: json!({"items": []}),
        };
        let source_result = source_loader
            .load_source(source_config, SourceCheckpoint::default())
            .await;
        assert!(matches!(
            source_result,
            Err(SourceLoaderError::UnknownSourceType { .. })
        ));
    }

    #[tokio::test]
    async fn test_source_loader_invalid_params() -> anyhow::Result<()> {
        let source_loader = quickwit_supported_sources();
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            source_type: "vec".to_string(),
            params: json!({"item": [], "batch_num_docs": 3}), //< item is misspelled
        };
        let source_result = source_loader
            .load_source(source_config, SourceCheckpoint::default())
            .await;
        assert!(matches!(
            source_result,
            Err(SourceLoaderError::FailedToCreateSource { .. })
        ));
        Ok(())
    }
}
