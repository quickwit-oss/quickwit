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

use std::collections::BTreeSet;
use std::fmt;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use std::time::Duration;

use quickwit_metastore::{SplitMaturity, SplitMetadata};
use quickwit_proto::types::{DocMappingUid, IndexUid, NodeId, SourceId, SplitId};
use tantivy::DateTime;
use time::OffsetDateTime;

use crate::merge_policy::MergePolicy;

pub struct SplitAttrs {
    /// ID of the node that produced the split.
    pub node_id: NodeId,
    // Index UID to which the split belongs.
    pub index_uid: IndexUid,
    /// Source ID to which the split belongs.
    pub source_id: SourceId,

    /// Doc mapping UID used to produce this split.
    pub doc_mapping_uid: DocMappingUid,

    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage resolver: for instance, the Amazon S3 region.
    pub split_id: SplitId,

    /// Partition to which the split belongs.
    ///
    /// Partitions are usually meant to isolate documents based on some field like
    /// `tenant_id`. For this reason, ideally splits with a different `partition_id`
    /// should not be merged together. Merging two splits with different `partition_id`
    /// does not hurt correctness however.
    pub partition_id: u64,

    /// Number of valid documents in the split.
    pub num_docs: u64,

    // Sum of the size of the document that were sent to the indexed.
    // This includes both documents that are valid or documents that are
    // invalid.
    pub uncompressed_docs_size_in_bytes: u64,

    pub time_range: Option<RangeInclusive<DateTime>>,

    pub replaced_split_ids: Vec<String>,

    /// Delete opstamp.
    pub delete_opstamp: u64,

    // Number of merge operation the split has been through so far.
    pub num_merge_ops: usize,
}

impl fmt::Debug for SplitAttrs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SplitAttrs")
            .field("split_id", &self.split_id)
            .field("partition_id", &self.partition_id)
            .field("replaced_split_ids", &self.replaced_split_ids)
            .field("time_range", &self.time_range)
            .field(
                "uncompressed_docs_size_in_bytes",
                &self.uncompressed_docs_size_in_bytes,
            )
            .field("num_docs", &self.num_docs)
            .field("num_merge_ops", &self.num_merge_ops)
            .finish()
    }
}

pub fn create_split_metadata(
    merge_policy: &Arc<dyn MergePolicy>,
    retention_policy: Option<&quickwit_config::RetentionPolicy>,
    split_attrs: &SplitAttrs,
    tags: BTreeSet<String>,
    footer_offsets: Range<u64>,
) -> SplitMetadata {
    let create_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let time_range = split_attrs
        .time_range
        .as_ref()
        .map(|range| range.start().into_timestamp_secs()..=range.end().into_timestamp_secs());

    let mut maturity =
        merge_policy.split_maturity(split_attrs.num_docs as usize, split_attrs.num_merge_ops);
    if let Some(max_maturity) = max_maturity_before_end_of_retention(
        retention_policy,
        create_timestamp,
        time_range.as_ref().map(|time_range| *time_range.end()),
    ) {
        maturity = maturity.min(max_maturity);
    }
    SplitMetadata {
        node_id: split_attrs.node_id.to_string(),
        index_uid: split_attrs.index_uid.clone(),
        source_id: split_attrs.source_id.clone(),
        doc_mapping_uid: split_attrs.doc_mapping_uid,
        split_id: split_attrs.split_id.clone(),
        partition_id: split_attrs.partition_id,
        num_docs: split_attrs.num_docs as usize,
        time_range,
        uncompressed_docs_size_in_bytes: split_attrs.uncompressed_docs_size_in_bytes,
        create_timestamp,
        maturity,
        tags,
        footer_offsets,
        delete_opstamp: split_attrs.delete_opstamp,
        num_merge_ops: split_attrs.num_merge_ops,
    }
}

/// reduce the maturity period of a split based on retention policy, so that it doesn't get merged
/// after it expires.
fn max_maturity_before_end_of_retention(
    retention_policy: Option<&quickwit_config::RetentionPolicy>,
    create_timestamp: i64,
    time_range_end: Option<i64>,
) -> Option<SplitMaturity> {
    let time_range_end = time_range_end? as u64;
    let retention_period_s = retention_policy?.retention_period().ok()?.as_secs();

    let maturity = if let Some(maturation_period_s) =
        (time_range_end + retention_period_s).checked_sub(create_timestamp as u64)
    {
        SplitMaturity::Immature {
            maturation_period: Duration::from_secs(maturation_period_s),
        }
    } else {
        // this split could be deleted as soon as it is created. Ideally we would
        // handle that sooner.
        SplitMaturity::Mature
    };
    Some(maturity)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_metastore::SplitMaturity;

    use super::max_maturity_before_end_of_retention;

    #[test]
    fn test_max_maturity_before_end_of_retention() {
        let retention_policy = quickwit_config::RetentionPolicy {
            evaluation_schedule: "daily".to_string(),
            retention_period: "300 sec".to_string(),
        };
        let create_timestamp = 1000;

        // this should be deleted asap, not subject to merge
        assert_eq!(
            max_maturity_before_end_of_retention(
                Some(&retention_policy),
                create_timestamp,
                Some(200),
            ),
            Some(SplitMaturity::Mature)
        );

        // retention ends at 750 + 300 = 1050, which is 50s from now
        assert_eq!(
            max_maturity_before_end_of_retention(
                Some(&retention_policy),
                create_timestamp,
                Some(750),
            ),
            Some(SplitMaturity::Immature {
                maturation_period: Duration::from_secs(50)
            })
        );

        // no retention policy
        assert_eq!(
            max_maturity_before_end_of_retention(None, create_timestamp, Some(850),),
            None,
        );

        // no timestamp_range.end but a retention policy, that's odd, don't change anything about
        // the maturity period
        assert_eq!(
            max_maturity_before_end_of_retention(Some(&retention_policy), create_timestamp, None,),
            None,
        );
    }
}
