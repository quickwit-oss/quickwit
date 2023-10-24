// Copyright (C) 2023 Quickwit, Inc.
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

use quickwit_proto::types::IndexUid;
use serde::{Deserialize, Serialize};

use crate::split_metadata::{utc_now_timestamp, SplitMaturity};
use crate::SplitMetadata;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub(crate) struct SplitMetadataV0_6 {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// Uid of the index this split belongs to.
    #[schema(value_type = String)]
    #[serde(alias = "index_id")]
    pub index_uid: IndexUid,

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

    #[schema(value_type = Option<Object>)]
    /// If a timestamp field is available, the min / max timestamp in
    /// the split.
    pub time_range: Option<RangeInclusive<i64>>,

    /// Timestamp for tracking when the split was created.
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,

    /// Split maturity either `Mature` or `Immature` with a given maturation period.
    #[serde(default)]
    #[schema(value_type = Value)]
    pub maturity: SplitMaturity,

    #[serde(default)]
    #[schema(value_type = Vec<String>)]
    /// A set of tags for categorizing and searching group of splits.
    pub tags: BTreeSet<String>,

    #[schema(value_type = Object)]
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

impl From<SplitMetadataV0_6> for SplitMetadata {
    fn from(v6: SplitMetadataV0_6) -> Self {
        let source_id = v6.source_id.unwrap_or_else(|| "unknown".to_string());

        let node_id = if let Some(node_id) = v6.node_id {
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
            split_id: v6.split_id,
            index_uid: v6.index_uid,
            partition_id: v6.partition_id,
            source_id,
            node_id,
            delete_opstamp: v6.delete_opstamp,
            num_docs: v6.num_docs,
            uncompressed_docs_size_in_bytes: v6.uncompressed_docs_size_in_bytes,
            time_range: v6.time_range,
            create_timestamp: v6.create_timestamp,
            maturity: v6.maturity,
            tags: v6.tags,
            footer_offsets: v6.footer_offsets,
            num_merge_ops: v6.num_merge_ops,
        }
    }
}

impl From<SplitMetadata> for SplitMetadataV0_6 {
    fn from(split: SplitMetadata) -> Self {
        SplitMetadataV0_6 {
            split_id: split.split_id,
            index_uid: split.index_uid,
            partition_id: split.partition_id,
            source_id: Some(split.source_id),
            node_id: Some(split.node_id),
            delete_opstamp: split.delete_opstamp,
            num_docs: split.num_docs,
            uncompressed_docs_size_in_bytes: split.uncompressed_docs_size_in_bytes,
            time_range: split.time_range,
            create_timestamp: split.create_timestamp,
            maturity: split.maturity,
            tags: split.tags,
            footer_offsets: split.footer_offsets,
            num_merge_ops: split.num_merge_ops,
        }
    }
}

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "version")]
pub(crate) enum VersionedSplitMetadata {
    #[serde(rename = "0.6")]
    // Retro compatibility.
    #[serde(alias = "0.5")]
    #[serde(alias = "0.4")]
    V0_6(SplitMetadataV0_6),
}

impl From<VersionedSplitMetadata> for SplitMetadata {
    fn from(versioned_helper: VersionedSplitMetadata) -> Self {
        match versioned_helper {
            VersionedSplitMetadata::V0_6(v0_6) => v0_6.into(),
        }
    }
}

impl From<SplitMetadata> for VersionedSplitMetadata {
    fn from(split_metadata: SplitMetadata) -> Self {
        VersionedSplitMetadata::V0_6(split_metadata.into())
    }
}
