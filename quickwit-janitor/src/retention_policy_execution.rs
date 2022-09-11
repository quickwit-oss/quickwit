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
use quickwit_metastore::{Metastore, Split, SplitMetadata, SplitState};
use time::OffsetDateTime;
use tracing::warn;

use crate::actors::RetentionPolicyExecutor;

/// Detect all expired splits based a retention policy and
/// mark them as MarkedForDeletion.
///
/// * `index_id` - The target index id.
/// * `metastore` - The metastore managing the target index.
/// * `retention_policy` - The retention policy to used to evaluate the splits.
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn run_execute_retention_policy(
    index_id: &str,
    metastore: Arc<dyn Metastore>,
    retention_policy: &RetentionPolicy,
    ctx_opt: Option<&ActorContext<RetentionPolicyExecutor>>,
) -> anyhow::Result<Vec<SplitMetadata>> {
    // Select published splits and filter for expiration.
    let current_date_time = OffsetDateTime::now_utc();
    let expired_splits: Vec<SplitMetadata> = metastore
        .list_splits(index_id, SplitState::Published, None, None)
        .await?
        .into_iter()
        // TODO: possibly move this in DB query
        .filter(|split| {
            is_split_expired(current_date_time, split, retention_policy).unwrap_or(false)
        })
        .map(|meta| meta.split_metadata)
        .collect();
    if let Some(ctx) = ctx_opt {
        ctx.record_progress();
    }

    if expired_splits.is_empty() {
        return Ok(expired_splits);
    }

    // Change all expired splits state to MarkedForDeletion.
    // The actual deletion will be taken care of by the garbage collection.
    let split_ids: Vec<&str> = expired_splits.iter().map(|meta| meta.split_id()).collect();
    metastore
        .mark_splits_for_deletion(index_id, &split_ids)
        .await?;

    Ok(expired_splits)
}

/// Checks to see if a split is expired based on retention policy.
fn is_split_expired(
    current_date_time: OffsetDateTime,
    split: &Split,
    retention_policy: &RetentionPolicy,
) -> anyhow::Result<bool> {
    let retention_period = retention_policy.retention_period()?;
    // The split `publish_timestamp` field does not exist in previous version of Quickwit < v0.4.
    // In order to stay backward compatible, we will use the `updated_timestamp` field
    // for splits generated from older version of Quickwit.
    let publish_timestamp = split.publish_timestamp.unwrap_or(split.update_timestamp);
    let publish_date_time = OffsetDateTime::from_unix_timestamp(publish_timestamp)?;
    let is_expired = match retention_policy.cutoff_reference {
        RetentionPolicyCutoffReference::PublishTimestamp => {
            (current_date_time - publish_date_time) >= retention_period
        }
        RetentionPolicyCutoffReference::SplitTimestampField => {
            if let Some(time_range) = &split.split_metadata.time_range {
                let time_range_end = OffsetDateTime::from_unix_timestamp(*time_range.end())?;
                (current_date_time - time_range_end) >= retention_period
            } else {
                warn!(index_id=%split.split_metadata.index_id, split_id=%split.split_id(),
                "Retention policy evaluation expected a `time_range` on the split, but none exist.");
                false
            }
        }
    };
    Ok(is_expired)
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use chrono::{Duration, Utc};
    use quickwit_config::{RetentionPolicy, RetentionPolicyCutoffReference};
    use quickwit_metastore::{Split, SplitMetadata, SplitState};
    use time::OffsetDateTime;

    use super::is_split_expired;

    fn make_split(
        update_timestamp: i64,
        publish_ts_opt: Option<i64>,
        time_range: Option<RangeInclusive<i64>>,
    ) -> Split {
        Split {
            split_metadata: SplitMetadata {
                split_id: "foo".to_string(),
                footer_offsets: 5..20,
                time_range,
                ..Default::default()
            },
            split_state: SplitState::Published,
            update_timestamp,
            publish_timestamp: publish_ts_opt,
        }
    }

    #[test]
    fn test_is_split_expired_with_publish_timestamp() {
        let one_hour_retention_policy = RetentionPolicy::new(
            "1 hour".to_string(),
            RetentionPolicyCutoffReference::PublishTimestamp,
            "hourly".to_string(),
        );

        // newly created split
        let now_timestamp = Utc::now().timestamp();
        let split = make_split(now_timestamp, Some(now_timestamp), None);
        assert!(!is_split_expired(
            OffsetDateTime::now_utc(),
            &split,
            &one_hour_retention_policy
        )
        .unwrap());

        let two_hours_ago_timestamp = (Utc::now() - Duration::hours(2)).timestamp();

        // A two hours old split
        let split = make_split(two_hours_ago_timestamp, Some(two_hours_ago_timestamp), None);
        assert!(is_split_expired(
            OffsetDateTime::now_utc(),
            &split,
            &one_hour_retention_policy
        )
        .unwrap());

        // A two hours old split without publish_timestamp
        let split = make_split(two_hours_ago_timestamp, None, None);
        assert!(is_split_expired(
            OffsetDateTime::now_utc(),
            &split,
            &one_hour_retention_policy
        )
        .unwrap());
    }

    #[test]
    fn test_is_split_expired_with_time_range() {
        let one_hour_retention_policy = RetentionPolicy::new(
            "1 hour".to_string(),
            RetentionPolicyCutoffReference::SplitTimestampField,
            "hourly".to_string(),
        );

        // newly created split
        let now_timestamp = Utc::now().timestamp();
        let split = make_split(now_timestamp, None, Some(10..=now_timestamp));
        assert!(!is_split_expired(
            OffsetDateTime::now_utc(),
            &split,
            &one_hour_retention_policy
        )
        .unwrap());

        let two_hours_ago_timestamp = (Utc::now() - Duration::hours(2)).timestamp();

        // A two hours old split
        let split = make_split(
            two_hours_ago_timestamp,
            None,
            Some(1000..=two_hours_ago_timestamp),
        );
        assert!(is_split_expired(
            OffsetDateTime::now_utc(),
            &split,
            &one_hour_retention_policy
        )
        .unwrap());

        // A two hours old split without time range.
        let split = make_split(two_hours_ago_timestamp, None, None);
        assert!(!is_split_expired(
            OffsetDateTime::now_utc(),
            &split,
            &one_hour_retention_policy
        )
        .unwrap());
    }
}
