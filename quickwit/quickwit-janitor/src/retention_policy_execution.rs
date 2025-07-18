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

use quickwit_actors::ActorContext;
use quickwit_common::pretty::PrettySample;
use quickwit_config::RetentionPolicy;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::metastore::{
    ListSplitsRequest, MarkSplitsForDeletionRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{IndexUid, SplitId};
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
    index_uid: IndexUid,
    metastore: MetastoreServiceClient,
    retention_policy: &RetentionPolicy,
    ctx: &ActorContext<RetentionPolicyExecutor>,
) -> anyhow::Result<Vec<SplitMetadata>> {
    // Select splits that are published and older than the retention period.
    let retention_period = retention_policy.retention_period()?;
    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let max_retention_timestamp = current_timestamp - retention_period.as_secs() as i64;
    let query = ListSplitsQuery::for_index(index_uid.clone())
        .with_split_state(SplitState::Published)
        .with_max_time_range_end(max_retention_timestamp);

    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let (expired_splits, ignored_splits): (Vec<SplitMetadata>, Vec<SplitMetadata>) = ctx
        .protect_future(metastore.list_splits(list_splits_request))
        .await?
        .collect_splits_metadata()
        .await?
        .into_iter()
        .partition(|split_metadata| split_metadata.time_range.is_some());

    if !ignored_splits.is_empty() {
        let ignored_split_ids: Vec<String> = ignored_splits
            .into_iter()
            .map(|split_metadata| split_metadata.split_id)
            .collect();
        warn!(
            index_id=%index_uid.index_id,
            split_ids=?PrettySample::new(&ignored_split_ids, 5),
            "Retention policy could not be applied to {} splits because they lack a timestamp range.",
            ignored_split_ids.len()
        );
    }
    if expired_splits.is_empty() {
        return Ok(expired_splits);
    }
    // Mark the expired splits for deletion.
    let expired_split_ids: Vec<SplitId> = expired_splits
        .iter()
        .map(|split_metadata| split_metadata.split_id.to_string())
        .collect();
    info!(
        index_id=%index_uid.index_id,
        split_ids=?PrettySample::new(&expired_split_ids, 5),
        "Marking {} splits for deletion based on retention policy.",
        expired_split_ids.len()
    );
    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid, expired_split_ids);
    ctx.protect_future(metastore.mark_splits_for_deletion(mark_splits_for_deletion_request))
        .await?;
    Ok(expired_splits)
}
