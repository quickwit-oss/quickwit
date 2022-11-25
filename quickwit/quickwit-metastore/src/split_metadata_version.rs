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
use std::ops::{Range, RangeInclusive};

use serde::{Deserialize, Serialize};

use crate::split_metadata::utc_now_timestamp;
use crate::SplitMetadata;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SplitMetadataV0_4 {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage URI resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// Id of the index this split belongs to.
    #[serde(default)]
    pub index_id: String,

    #[serde(default)]
    pub partition_id: u64,

    #[serde(default)]
    pub source_id: Option<String>,

    #[serde(default)]
    pub node_id: Option<String>,

    /// Number of records (or documents) in the split.
    pub num_docs: usize,

    /// Sum of the size (in bytes) of the raw documents in this split.
    ///
    /// Note this is not the split file size. It is the size of the original
    /// JSON payloads.
    #[serde(alias = "size_in_bytes")]
    pub uncompressed_docs_size_in_bytes: u64,

    /// If a timestamp field is available, the min / max timestamp in
    /// the split.
    pub time_range: Option<RangeInclusive<i64>>,

    /// Timestamp for tracking when the split was created.
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,

    /// A set of tags for categorizing and searching group of splits.
    #[serde(default)]
    pub tags: BTreeSet<String>,

    /// Contains the range of bytes of the footer that needs to be downloaded
    /// in order to open a split.
    ///
    /// The footer offsets
    /// make it possible to download the footer in a single call to `.get_slice(...)`.
    pub footer_offsets: Range<u64>,

    /// Split delete opstamp.
    #[serde(default)]
    pub delete_opstamp: u64,

    #[serde(default)]
    num_merge_ops: usize,
}

impl From<SplitMetadataV0_4> for SplitMetadata {
    fn from(v3: SplitMetadataV0_4) -> Self {
        let source_id = v3.source_id.unwrap_or_else(|| "unknown".to_string());

        let node_id = if let Some(node_id) = v3.node_id {
            // The previous version encoded `v1.node_id` as `{node_id}/{pipeline_ord}`.
            // Since pipeline_ord is no longer needed, we only extract the `node_id` portion
            // to keep backward compatibility.  This has the advantage of avoiding a
            // brand new version.
            if let Some((node_id, _)) = node_id.rsplit_once('/') {
                node_id.to_string()
            } else {
                node_id
            }
        } else {
            "unknown".to_string()
        };

        SplitMetadata {
            split_id: v3.split_id,
            index_id: v3.index_id,
            partition_id: v3.partition_id,
            source_id,
            node_id,
            delete_opstamp: v3.delete_opstamp,
            num_docs: v3.num_docs,
            uncompressed_docs_size_in_bytes: v3.uncompressed_docs_size_in_bytes,
            time_range: v3.time_range,
            create_timestamp: v3.create_timestamp,
            tags: v3.tags,
            footer_offsets: v3.footer_offsets,
            num_merge_ops: v3.num_merge_ops,
        }
    }
}

impl From<SplitMetadata> for SplitMetadataV0_4 {
    fn from(split: SplitMetadata) -> Self {
        SplitMetadataV0_4 {
            split_id: split.split_id,
            index_id: split.index_id,
            partition_id: split.partition_id,
            source_id: Some(split.source_id),
            node_id: Some(split.node_id),
            delete_opstamp: split.delete_opstamp,
            num_docs: split.num_docs,
            uncompressed_docs_size_in_bytes: split.uncompressed_docs_size_in_bytes,
            time_range: split.time_range,
            create_timestamp: split.create_timestamp,
            tags: split.tags,
            footer_offsets: split.footer_offsets,
            num_merge_ops: split.num_merge_ops,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedSplitMetadata {
    #[serde(rename = "0.4")]
    V0_4(SplitMetadataV0_4),
}

impl From<VersionedSplitMetadata> for SplitMetadata {
    fn from(versioned_helper: VersionedSplitMetadata) -> Self {
        match versioned_helper {
            VersionedSplitMetadata::V0_4(v0_4) => v0_4.into(),
        }
    }
}

impl From<SplitMetadata> for VersionedSplitMetadata {
    fn from(split_metadata: SplitMetadata) -> Self {
        VersionedSplitMetadata::V0_4(split_metadata.into())
    }
}
