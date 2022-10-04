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

use std::collections::BTreeSet;
use std::fmt;
use std::ops::{Range, RangeInclusive};

use quickwit_metastore::SplitMetadata;
use time::OffsetDateTime;

use crate::models::IndexingPipelineId;

pub struct SplitAttrs {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage URI resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// Partition to which the split belongs to.
    ///
    /// Partitions are usually meant to isolate documents based on some field like
    /// `tenant_id`. For this reason, ideally splits with a different `partition_id`
    /// should not be merged together. Merging two splits with different `partition_id`
    /// does not hurt correctness however.
    pub partition_id: u64,

    pub pipeline_id: IndexingPipelineId,

    /// Number of valid documents in the split.
    pub num_docs: u64,

    // Sum of the size of the document that were sent to the indexed.
    // This includes both documents that are valid or documents that are
    // invalid.
    pub uncompressed_docs_size_in_bytes: u64,

    pub time_range: Option<RangeInclusive<i64>>,

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
    split_attrs: &SplitAttrs,
    tags: BTreeSet<String>,
    footer_offsets: Range<u64>,
) -> SplitMetadata {
    SplitMetadata {
        split_id: split_attrs.split_id.clone(),
        index_id: split_attrs.pipeline_id.index_id.clone(),
        partition_id: split_attrs.partition_id,
        source_id: split_attrs.pipeline_id.source_id.clone(),
        node_id: split_attrs.pipeline_id.node_id.clone(),
        pipeline_ord: split_attrs.pipeline_id.pipeline_ord,
        num_docs: split_attrs.num_docs as usize,
        time_range: split_attrs.time_range.clone(),
        uncompressed_docs_size_in_bytes: split_attrs.uncompressed_docs_size_in_bytes,
        create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
        tags,
        footer_offsets,
        delete_opstamp: split_attrs.delete_opstamp,
        num_merge_ops: split_attrs.num_merge_ops,
    }
}
