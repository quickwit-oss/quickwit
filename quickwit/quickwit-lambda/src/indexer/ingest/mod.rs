// Copyright (C) 2024 Quickwit, Inc.
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

mod helpers;

use std::collections::HashSet;

use anyhow::bail;
use helpers::{
    configure_source, create_empty_cluster, init_index_if_necessary, send_telemetry,
    spawn_pipelines, spawn_services,
};
use quickwit_actors::Universe;
use quickwit_cli::start_actor_runtimes;
use quickwit_cli::tool::start_statistics_reporting_loop;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::SourceInputFormat;
use quickwit_index_management::clear_cache_directory;
use quickwit_indexing::models::IndexingStatistics;
use tracing::{debug, info};

use crate::indexer::environment::{CONFIGURATION_TEMPLATE, DISABLE_JANITOR};
use crate::indexer::ingest::helpers::{prune_lambda_source, wait_for_merges};
use crate::utils::load_node_config;

#[derive(Debug, Eq, PartialEq)]
pub struct IngestArgs {
    pub input_path: Uri,
    pub input_format: SourceInputFormat,
    pub overwrite: bool,
    pub vrl_script: Option<String>,
    pub clear_cache: bool,
}

pub async fn ingest(args: IngestArgs) -> anyhow::Result<IndexingStatistics> {
    debug!(args=?args, "lambda-ingest");

    send_telemetry().await;

    let (config, storage_resolver, mut metastore) =
        load_node_config(CONFIGURATION_TEMPLATE).await?;

    let source_config =
        configure_source(args.input_path, args.input_format, args.vrl_script).await?;

    let index_metadata = init_index_if_necessary(
        &mut metastore,
        &storage_resolver,
        &config.default_index_root_uri,
        args.overwrite,
        &source_config,
    )
    .await?;

    let mut services = vec![QuickwitService::Indexer];
    if !*DISABLE_JANITOR {
        services.push(QuickwitService::Janitor);
    }
    let cluster = create_empty_cluster(&config, &services[..]).await?;
    let universe = Universe::new();
    let runtimes_config = RuntimesConfig::default();

    start_actor_runtimes(runtimes_config, &HashSet::from_iter(services))?;

    let (indexing_service_handle, _janitor_service_guard) = spawn_services(
        &universe,
        cluster,
        metastore.clone(),
        storage_resolver.clone(),
        &config,
        runtimes_config,
    )
    .await?;

    let (indexing_pipeline_handle, merge_pipeline_handle) =
        spawn_pipelines(indexing_service_handle.mailbox(), source_config).await?;

    prune_lambda_source(&mut metastore, index_metadata).await?;

    debug!("wait for indexing to complete");
    let statistics = start_statistics_reporting_loop(indexing_pipeline_handle, false).await?;

    debug!("wait for merges to complete");
    wait_for_merges(merge_pipeline_handle).await?;

    debug!("indexing completed, tearing down actors");
    // TODO: is it really necessary to terminate the indexing service?
    // Quitting the universe should be enough.
    universe
        .send_exit_with_success(indexing_service_handle.mailbox())
        .await?;
    indexing_service_handle.join().await;
    debug!("quitting universe");
    universe.quit().await;
    debug!("universe.quit() awaited");

    if args.clear_cache {
        info!("clearing local cache directory");
        clear_cache_directory(&config.data_dir_path).await?;
        info!("local cache directory cleared");
    }

    if statistics.num_invalid_docs > 0 {
        bail!("Failed to ingest {} documents", statistics.num_invalid_docs)
    }
    Ok(statistics)
}
