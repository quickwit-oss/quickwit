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
use quickwit_config::{RetentionPolicy, RetentionPolicyCutoffReference};
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
    let retention_timestamp = current_timestamp - retention_period.as_secs() as i64;
    let base_query = ListSplitsQuery::for_index(index_id).with_split_state(SplitState::Published);
    let query = match retention_policy.cutoff_reference {
        RetentionPolicyCutoffReference::IndexingTimestamp => {
            base_query.with_indexing_end_timestamp_lte(retention_timestamp)
        }
        RetentionPolicyCutoffReference::SplitTimestampField => {
            base_query.with_time_range_end_lte(retention_timestamp)
        }
    };

    let candidate_splits: Vec<SplitMetadata> = ctx
        .protect_future(metastore.list_splits(query))
        .await?
        .into_iter()
        .map(|meta| meta.split_metadata)
        .collect();
    let expired_splits = ignore_not_applicable_splits(
        index_id,
        candidate_splits,
        retention_policy.cutoff_reference,
    );

    if expired_splits.is_empty() {
        return Ok(expired_splits);
    }

    info!(index_id=%index_id, num_splits=%expired_splits.len(), "retention-policy-mark-splits-for-deletion");
    // Change all expired splits state to MarkedForDeletion.
    let split_ids: Vec<&str> = expired_splits.iter().map(|meta| meta.split_id()).collect();
    ctx.protect_future(metastore.mark_splits_for_deletion(index_id, &split_ids))
        .await?;

    Ok(expired_splits)
}

/// Get rid of splits without time range.
///
/// For retention policy with cutoff_reference set to `split_timestamp_field`, we cannot
/// apply the retention policy to splits without time range.
/// We instead list them in a warning message.
fn ignore_not_applicable_splits(
    index_id: &str,
    splits: Vec<SplitMetadata>,
    cutoff_reference: RetentionPolicyCutoffReference,
) -> Vec<SplitMetadata> {
    if cutoff_reference != RetentionPolicyCutoffReference::SplitTimestampField {
        return splits;
    }

    let mut expired_splits = Vec::with_capacity(splits.len());
    let mut not_applicable_splits = vec![];
    for split_metadata in splits {
        if split_metadata.time_range.is_some() {
            expired_splits.push(split_metadata);
        } else {
            not_applicable_splits.push(split_metadata);
        }
    }

    if !not_applicable_splits.is_empty() {
        warn!(
            index_id=%index_id,
            not_applicable_splits=?not_applicable_splits,
            "Retention policy with cutoff_reference set to `split_timestamp_field` cannot be applied to splits without time range."
        );
    }
    expired_splits
}
