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

use super::Source;
use async_trait::async_trait;
use quickwit_metastore::checkpoint::Checkpoint;
use std::collections::HashMap;

#[async_trait]
pub trait SourceFactory: 'static + Send + Sync {
    async fn create_source(
        &self,
        params: serde_json::Value,
        checkpoint: Checkpoint,
    ) -> anyhow::Result<Box<dyn Source>>;
}

#[async_trait]
pub trait TypedSourceFactory: Send + Sync + 'static {
    type Source: Source;
    type Params: serde::de::DeserializeOwned + Send + Sync + 'static;
    async fn typed_create_source(
        params: Self::Params,
        checkpoint: quickwit_metastore::checkpoint::Checkpoint,
    ) -> anyhow::Result<Self::Source>;
}

#[async_trait]
impl<T: TypedSourceFactory> SourceFactory for T {
    async fn create_source(
        &self,
        params: serde_json::Value,
        checkpoint: quickwit_metastore::checkpoint::Checkpoint,
    ) -> anyhow::Result<Box<dyn Source>> {
        let typed_params: T::Params = serde_json::from_value(params)?;
        let file_source = Self::typed_create_source(typed_params, checkpoint).await?;
        Ok(Box::new(file_source))
    }
}

#[derive(Default)]
pub struct SourceFactoryResolver {
    type_to_factory: HashMap<String, Box<dyn SourceFactory>>,
}

impl SourceFactoryResolver {
    pub fn add_source<S: ToString, F: SourceFactory>(&mut self, source: S, factory: F) {
        self.type_to_factory
            .insert(source.to_string(), Box::new(factory));
    }
}
