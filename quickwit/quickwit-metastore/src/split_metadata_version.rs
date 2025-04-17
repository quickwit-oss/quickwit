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

use std::collections::BTreeSet;
use std::ops::{Range, RangeInclusive};

use quickwit_proto::types::{DocMappingUid, IndexUid, SplitId};
use serde::{Deserialize, Serialize};

use crate::SplitMetadata;
use crate::split_metadata::{SplitMaturity, utc_now_timestamp};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub(crate) struct SplitMetadataV0_8 {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage resolver: for instance, the Amazon S3 region.
    #[schema(value_type = String)]
    pub split_id: SplitId,

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

    // we default fill with zero: we don't know the right uid, and it's correct to assume all
    // splits before when updates first appeared are compatible with each other.
    #[serde(default)]
    doc_mapping_uid: DocMappingUid,
}

impl From<SplitMetadataV0_8> for SplitMetadata {
    fn from(v8: SplitMetadataV0_8) -> Self {
        let source_id = v8.source_id.unwrap_or_else(|| "unknown".to_string());

        let node_id = if let Some(node_id) = v8.node_id {
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
            split_id: v8.split_id,
            index_uid: v8.index_uid,
            partition_id: v8.partition_id,
            source_id,
            node_id,
            delete_opstamp: v8.delete_opstamp,
            num_docs: v8.num_docs,
            uncompressed_docs_size_in_bytes: v8.uncompressed_docs_size_in_bytes,
            time_range: v8.time_range,
            create_timestamp: v8.create_timestamp,
            maturity: v8.maturity,
            tags: v8.tags,
            footer_offsets: v8.footer_offsets,
            num_merge_ops: v8.num_merge_ops,
            doc_mapping_uid: v8.doc_mapping_uid,
        }
    }
}

impl From<SplitMetadata> for SplitMetadataV0_8 {
    fn from(split: SplitMetadata) -> Self {
        SplitMetadataV0_8 {
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
            doc_mapping_uid: split.doc_mapping_uid,
        }
    }
}

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "version")]
pub(crate) enum VersionedSplitMetadata {
    #[serde(rename = "0.9")]
    // Retro compatibility.
    #[serde(alias = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(SplitMetadataV0_8),
}

impl From<VersionedSplitMetadata> for SplitMetadata {
    fn from(versioned_helper: VersionedSplitMetadata) -> Self {
        match versioned_helper {
            VersionedSplitMetadata::V0_8(v0_8) => v0_8.into(),
        }
    }
}

impl From<SplitMetadata> for VersionedSplitMetadata {
    fn from(split_metadata: SplitMetadata) -> Self {
        VersionedSplitMetadata::V0_8(split_metadata.into())
    }
}
