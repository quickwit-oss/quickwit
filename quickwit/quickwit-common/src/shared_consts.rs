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

use std::sync::OnceLock;
use std::time::Duration;

use bytesize::ByteSize;
use tracing::warn;

/// Field name reserved for storing the dynamically indexed fields.
pub const FIELD_PRESENCE_FIELD_NAME: &str = "_field_presence";

pub const MINIMUM_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(5 * 60); // 5mn
const MAXIMUM_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(2 * 24 * 3600); // 2 days

/// We cannot safely delete splits right away as a:
/// - in-flight queries could actually have selected this split,
/// - scroll queries may also have a point in time on these splits.
///
/// We deal this probably by introducing a grace period. A split is first marked as delete,
/// and hence won't be selected for search. After a few minutes, once it reasonably safe to assume
/// that all queries involving this split have terminated, we effectively delete the split.
/// This duration is controlled by `DELETION_GRACE_PERIOD`.
pub fn split_deletion_grace_period() -> Duration {
    const DEFAULT_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(60 * 32); // 32 min

    static SPLIT_DELETION_GRACE_PERIOD_SECS_LOCK: OnceLock<Duration> = std::sync::OnceLock::new();
    *SPLIT_DELETION_GRACE_PERIOD_SECS_LOCK.get_or_init(|| {
        let deletion_grace_period_secs: u64 = crate::get_from_env(
            "QW_SPLIT_DELETION_GRACE_PERIOD_SECS",
            DEFAULT_DELETION_GRACE_PERIOD.as_secs(),
            false,
        );
        let deletion_grace_period_secs_clamped: u64 = deletion_grace_period_secs.clamp(
            MINIMUM_DELETION_GRACE_PERIOD.as_secs(),
            MAXIMUM_DELETION_GRACE_PERIOD.as_secs(),
        );
        if deletion_grace_period_secs_clamped != deletion_grace_period_secs {
            warn!(
                "The deletion grace period is clamped to {} seconds. The provided value was {} \
                 seconds.",
                deletion_grace_period_secs_clamped, deletion_grace_period_secs
            );
        }
        Duration::from_secs(deletion_grace_period_secs_clamped)
    })
}

/// In order to amortized search with scroll, we fetch more documents than are
/// being requested.
pub const SCROLL_BATCH_LEN: usize = 1_000;

/// Prefix used in chitchat to broadcast the list of primary shards hosted by a leader.
pub const INGESTER_PRIMARY_SHARDS_PREFIX: &str = "ingester.primary_shards:";

/// File name for the encoded list of fields in the split
pub const SPLIT_FIELDS_FILE_NAME: &str = "split_fields";

/// More or less the indexing throughput of a core
/// i.e. PIPELINE_THROUGHPUT / PIPELINE_FULL_CAPACITY
pub const DEFAULT_SHARD_THROUGHPUT_LIMIT: ByteSize = ByteSize::mib(5);
/// Large enough to absorb small bursts but should remain defensive against unbalanced shards.
pub const DEFAULT_SHARD_BURST_LIMIT: ByteSize = ByteSize::mib(50);

/// A compromise between "exponential" scale up and moderate shard count increase.
pub const DEFAULT_SHARD_SCALE_UP_FACTOR: f32 = 1.5;

// (Just a reexport).
pub use bytesize::MIB;
