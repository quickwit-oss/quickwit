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

use std::collections::HashSet;

use anyhow::{Context, ensure};
use colored::Colorize;
use quickwit_actors::Universe;
use quickwit_config::NodeConfig;
use quickwit_index_management::{IndexService, clear_cache_directory};
use quickwit_metastore::{ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};
use quickwit_proto::types::IndexUid;

use super::LocalIngestDocsArgs;
use crate::checklist::GREEN_COLOR;

/// Stops actors; detached upload tasks are not tracked here.
pub(super) async fn quiesce(universe: &Universe) {
    universe.kill();
    let mut joined = HashSet::new();
    loop {
        // Initializers can register children after a quit snapshot.
        let snapshot: HashSet<String> = universe.quit().await.into_keys().collect();
        if snapshot == joined {
            return;
        }
        joined = snapshot;
    }
}

/// Runs cache cleanup and rollback after actor shutdown.
pub(super) async fn finish_load(
    load_result: anyhow::Result<()>,
    index_service: &mut IndexService,
    args: &LocalIngestDocsArgs,
    config: &NodeConfig,
    index_uid: &IndexUid,
) -> anyhow::Result<()> {
    let cache_result = clear_cache_directory_if_requested(args, config).await;
    let load_error = match (load_result, cache_result) {
        (Ok(()), Ok(())) => return Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => error,
        (Err(error), Err(cache_error)) => {
            error.context(format!("cache cleanup also failed: {cache_error:#}"))
        }
    };
    println!("Load failed, clearing index `{}`...", args.index_id);
    if let Err(rollback_error) = clear_index_checked(index_service, index_uid).await {
        return Err(load_error.context(format!(
            "failed to clear the index after the load failed: {rollback_error:#}"
        )));
    }
    Err(load_error)
}

pub(super) async fn clear_index_checked(
    index_service: &mut IndexService,
    index_uid: &IndexUid,
) -> anyhow::Result<()> {
    index_service.clear_index(&index_uid.index_id).await?;
    // clear_index currently logs some deletion failures rather than returning them.
    let query = ListSplitsQuery::for_index(index_uid.clone()).with_limit(1);
    let remaining = index_service
        .metastore()
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query)?)
        .await
        .context("failed to verify index cleanup")?
        .collect_splits_metadata()
        .await
        .context("failed to verify index cleanup")?;
    ensure!(
        remaining.is_empty(),
        "index cleanup left split metadata behind"
    );
    Ok(())
}

async fn clear_cache_directory_if_requested(
    args: &LocalIngestDocsArgs,
    config: &NodeConfig,
) -> anyhow::Result<()> {
    if !args.clear_cache {
        return Ok(());
    }
    println!("Clearing local cache directory...");
    clear_cache_directory(&config.data_dir_path)
        .await
        .context("failed to clear local cache directory")?;
    println!("{} Local cache directory cleared.", "✔".color(GREEN_COLOR));
    Ok(())
}

#[cfg(test)]
mod tests;
