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

use std::sync::Arc;

use quickwit_actors::Universe;
use quickwit_cluster::Cluster;
use quickwit_config::QuickwitConfig;
use quickwit_metastore::Metastore;
use quickwit_search::SearchClientPool;
use quickwit_storage::StorageUriResolver;
use tracing::info;

pub mod actors;
mod garbage_collection;
mod janitor_service;

pub use janitor_service::JanitorService;

pub use self::garbage_collection::{
    delete_splits_with_files, run_garbage_collect, FileEntry, SplitDeletionError,
};
use crate::actors::{DeleteTaskService, GarbageCollector};

pub async fn start_janitor_service(
    universe: &Universe,
    config: &QuickwitConfig, // kept it for retention policy
    metastore: Arc<dyn Metastore>,
    cluster: Arc<Cluster>,
    storage_uri_resolver: StorageUriResolver,
) -> anyhow::Result<JanitorService> {
    info!("Starting janitor service.");
    let garbage_collector = GarbageCollector::new(metastore.clone(), storage_uri_resolver.clone());
    let (_, garbage_collector_handle) = universe.spawn_actor(garbage_collector).spawn();
    let search_client_pool = SearchClientPool::create_and_keep_updated(
        &cluster.members(),
        cluster.member_change_watcher(),
    )
    .await?;
    let delete_task_service = DeleteTaskService::new(
        metastore,
        search_client_pool,
        storage_uri_resolver,
        config.data_dir_path.clone(),
    );
    let (_, delete_task_service_handle) = universe.spawn_actor(delete_task_service).spawn();

    Ok(JanitorService::new(
        garbage_collector_handle,
        delete_task_service_handle,
    ))
}
