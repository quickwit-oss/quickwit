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

use anyhow::Context;
use quickwit_actors::ActorContext;
use quickwit_config::{RetentionPolicy, RetentionPolicyCutoffReference};
use quickwit_metastore::{Metastore, Split, SplitMetadata, SplitState};
use time::OffsetDateTime;

use crate::actors::RetentionPolicyEvaluator;

/// Detect all discardable splits based a retention policy and mark then as MarkedForDeletion.
///
/// * `index_id` - The target index id.
/// * `metastore` - The metastore managing the target index.
/// * `retention_policy` - The retention policy to used to evaluate the splits.
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn run_evaluate_retention_policy(
    index_id: &str,
    metastore: Arc<dyn Metastore>,
    retention_policy: &RetentionPolicy,
    ctx_opt: Option<&ActorContext<RetentionPolicyEvaluator>>,
) -> anyhow::Result<Vec<SplitMetadata>> {
    // Select published splits and filter for eligibility.
    let current_date_time = OffsetDateTime::now_utc();
    let discardable_splits: Vec<SplitMetadata> = metastore
        .list_splits(index_id, SplitState::Published, None, None)
        .await?
        .into_iter()
        // TODO: possibly move this in DB query
        .filter(|split| is_discardable(current_date_time, split, retention_policy).unwrap_or(false))
        .map(|meta| meta.split_metadata)
        .collect();
    if let Some(ctx) = ctx_opt {
        ctx.record_progress();
    }

    // Schedule all discardable splits for delete
    let split_ids: Vec<&str> = discardable_splits
        .iter()
        .map(|meta| meta.split_id())
        .collect();
    metastore
        .mark_splits_for_deletion(index_id, &split_ids)
        .await?;

    Ok(discardable_splits)
}

/// Checks to see if a split can be discarded based on retention policy.
fn is_discardable(
    current_date_time: OffsetDateTime,
    split: &Split,
    retention_policy: &RetentionPolicy,
) -> anyhow::Result<bool> {
    let retention_period = retention_policy.retention_period()?;
    let publish_timestamp = split
        .publish_timestamp
        .context("Expected a split with a valid published timestamp.")?;
    let publish_date_time = OffsetDateTime::from_unix_timestamp(publish_timestamp)?;
    let is_discardable = match retention_policy.cutoff_reference {
        RetentionPolicyCutoffReference::PublishTimestamp => {
            (current_date_time - publish_date_time) >= retention_period
        }
        RetentionPolicyCutoffReference::SplitTimestampField => {
            match &split.split_metadata.time_range {
                Some(time_range) => {
                    let time_range_end = OffsetDateTime::from_unix_timestamp(*time_range.end())?;
                    (current_date_time - time_range_end) >= retention_period
                }
                None => (current_date_time - publish_date_time) >= retention_period,
            }
        }
    };
    Ok(is_discardable)
}
