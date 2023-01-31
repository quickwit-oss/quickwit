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

#![deny(clippy::disallowed_methods)]

use std::sync::Arc;

use quickwit_actors::{Mailbox, Universe};
use quickwit_common::FileEntry;
use quickwit_config::QuickwitConfig;
use quickwit_metastore::Metastore;
use quickwit_search::SearchJobPlacer;
use quickwit_storage::StorageUriResolver;
use tracing::info;

pub mod actors;
pub mod error;
mod garbage_collection;
mod janitor_service;
mod metrics;
mod retention_policy_execution;

pub use janitor_service::JanitorService;

pub use self::garbage_collection::{
    delete_splits_with_files, run_garbage_collect, SplitDeletionError, SplitRemovalInfo,
};
use crate::actors::{DeleteTaskService, GarbageCollector, RetentionPolicyExecutor};

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(FileEntry)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct JanitorApiSchemas;

pub async fn start_janitor_service(
    universe: &Universe,
    config: &QuickwitConfig,
    metastore: Arc<dyn Metastore>,
    search_job_placer: SearchJobPlacer,
    storage_uri_resolver: StorageUriResolver,
) -> anyhow::Result<Mailbox<JanitorService>> {
    info!("Starting janitor service.");
    let garbage_collector = GarbageCollector::new(metastore.clone(), storage_uri_resolver.clone());
    let (_, garbage_collector_handle) = universe.spawn_builder().spawn(garbage_collector);

    let retention_policy_executor = RetentionPolicyExecutor::new(metastore.clone());
    let (_, retention_policy_executor_handle) =
        universe.spawn_builder().spawn(retention_policy_executor);

    let delete_task_service = DeleteTaskService::new(
        metastore,
        search_job_placer,
        storage_uri_resolver,
        config.data_dir_path.clone(),
        config.indexer_config.max_concurrent_split_uploads,
    );
    let (_, delete_task_service_handle) = universe.spawn_builder().spawn(delete_task_service);

    let janitor_service = JanitorService::new(
        delete_task_service_handle,
        garbage_collector_handle,
        retention_policy_executor_handle,
    );
    let (janitor_service_mailbox, _janitor_service_handle) =
        universe.spawn_builder().spawn(janitor_service);
    Ok(janitor_service_mailbox)
}
