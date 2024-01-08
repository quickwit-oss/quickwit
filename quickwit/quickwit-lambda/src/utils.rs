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

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;

use anyhow::Context;
use quickwit_config::{ConfigFormat, NodeConfig};
use quickwit_metastore::MetastoreResolver;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tracing::info;

pub(crate) async fn load_node_config(
    config_template: &str,
) -> anyhow::Result<(NodeConfig, StorageResolver, MetastoreServiceClient)> {
    let config = NodeConfig::load(ConfigFormat::Yaml, config_template.as_bytes())
        .await
        .with_context(|| format!("Failed to parse node config `{config_template}`."))?;
    info!(config=?config, "Loaded node config.");
    let storage_resolver = StorageResolver::configured(&config.storage_configs);
    let metastore_resolver =
        MetastoreResolver::configured(storage_resolver.clone(), &config.metastore_configs);
    let metastore: MetastoreServiceClient =
        metastore_resolver.resolve(&config.metastore_uri).await?;
    Ok((config, storage_resolver, metastore))
}

static CONTAINER_ID: AtomicU32 = AtomicU32::new(0);

pub struct LambdaContainerContext {
    pub container_id: u32,
    pub cold: bool,
}

impl LambdaContainerContext {
    /// Configure and return the Lambda container context.
    ///
    /// The `cold` field returned will be `true` only the first time this
    /// function is called.
    pub fn load() -> Self {
        let mut container_id = CONTAINER_ID.load(SeqCst);
        let mut cold = false;
        if container_id == 0 {
            container_id = rand::random();
            CONTAINER_ID.store(container_id, SeqCst);
            cold = true;
        }
        Self { container_id, cold }
    }
}
