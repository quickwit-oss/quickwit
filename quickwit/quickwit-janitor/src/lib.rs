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

#![deny(clippy::disallowed_methods)]

use quickwit_actors::{Mailbox, Universe};
use quickwit_common::pubsub::EventBroker;
use quickwit_config::NodeConfig;
use quickwit_indexing::actors::MergeSchedulerService;
use quickwit_metastore::SplitInfo;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchJobPlacer;
use quickwit_storage::StorageResolver;
use tracing::info;

pub mod actors;
pub mod error;
mod janitor_service;
mod metrics;
mod retention_policy_execution;

pub use janitor_service::JanitorService;

use crate::actors::{DeleteTaskService, GarbageCollector, RetentionPolicyExecutor};

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(SplitInfo)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct JanitorApiSchemas;

pub async fn start_janitor_service(
    universe: &Universe,
    config: &NodeConfig,
    metastore: MetastoreServiceClient,
    search_job_placer: SearchJobPlacer,
    storage_resolver: StorageResolver,
    event_broker: EventBroker,
    run_delete_task_service: bool,
) -> anyhow::Result<Mailbox<JanitorService>> {
    info!("starting janitor service");
    let garbage_collector = GarbageCollector::new(metastore.clone(), storage_resolver.clone());
    let (_, garbage_collector_handle) = universe.spawn_builder().spawn(garbage_collector);

    let retention_policy_executor = RetentionPolicyExecutor::new(metastore.clone());
    let (_, retention_policy_executor_handle) =
        universe.spawn_builder().spawn(retention_policy_executor);
    let delete_task_service_handle = if run_delete_task_service {
        let delete_task_service = DeleteTaskService::new(
            metastore,
            search_job_placer,
            storage_resolver,
            config.data_dir_path.clone(),
            config.indexer_config.max_concurrent_split_uploads,
            universe.get_or_spawn_one::<MergeSchedulerService>(),
            event_broker,
        )
        .await?;
        let (_, delete_task_service_handle) = universe.spawn_builder().spawn(delete_task_service);
        Some(delete_task_service_handle)
    } else {
        tracing::warn!("delete task service is disabled: delete queries will not be processed");
        None
    };

    let janitor_service = JanitorService::new(
        delete_task_service_handle,
        garbage_collector_handle,
        retention_policy_executor_handle,
    );
    let (janitor_service_mailbox, _janitor_service_handle) =
        universe.spawn_builder().spawn(janitor_service);
    Ok(janitor_service_mailbox)
}
