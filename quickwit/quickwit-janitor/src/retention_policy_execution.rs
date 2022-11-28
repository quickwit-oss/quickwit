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

use quickwit_actors::ActorContext;
use quickwit_common::PrettySample;
use quickwit_config::RetentionPolicy;
use quickwit_metastore::{ListSplitsQuery, Metastore, SplitMetadata, SplitState};
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::actors::RetentionPolicyExecutor;

/// Detect all expired splits based a retention policy and
/// only mark them as `MarkedForDeletion`. Actual split deletion
/// is taken care of by the garbage collector.
///
/// * `index_id` - The target index id.
/// * `metastore` - The metastore managing the target index.
/// * `retention_policy` - The retention policy to used to evaluate the splits.
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn run_execute_retention_policy(
    index_id: &str,
    metastore: Arc<dyn Metastore>,
    retention_policy: &RetentionPolicy,
    ctx: &ActorContext<RetentionPolicyExecutor>,
) -> anyhow::Result<Vec<SplitMetadata>> {
    // Select splits that are published and older than the retention period.
    let retention_period = retention_policy.retention_period()?;
    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let max_retention_timestamp = current_timestamp - retention_period.as_secs() as i64;
    let query = ListSplitsQuery::for_index(index_id)
        .with_split_state(SplitState::Published)
        .with_time_range_end_lte(max_retention_timestamp);

    let (expired_splits, ignored_splits): (Vec<SplitMetadata>, Vec<SplitMetadata>) = ctx
        .protect_future(metastore.list_splits(query))
        .await?
        .into_iter()
        .map(|split| split.split_metadata)
        .partition(|split_metadata| split_metadata.time_range.is_some());

    if !ignored_splits.is_empty() {
        let ignored_split_ids: Vec<String> = ignored_splits
            .into_iter()
            .map(|split_metadata| split_metadata.split_id)
            .collect();
        warn!(
            index_id=%index_id,
            split_ids=?PrettySample::new(&ignored_split_ids, 5),
            "Retention policy could not be applied to {} splits because they lack a timestamp range.",
            ignored_split_ids.len()
        );
    }
    if expired_splits.is_empty() {
        return Ok(expired_splits);
    }
    // Mark the expired splits for deletion.
    let expired_split_ids: Vec<&str> = expired_splits
        .iter()
        .map(|split_metadata| split_metadata.split_id())
        .collect();
    info!(
        index_id=%index_id,
        split_ids=?PrettySample::new(&expired_split_ids, 5),
        "Marking {} splits for deletion based on retention policy.",
        expired_split_ids.len()
    );
    ctx.protect_future(metastore.mark_splits_for_deletion(index_id, &expired_split_ids))
        .await?;
    Ok(expired_splits)
}
