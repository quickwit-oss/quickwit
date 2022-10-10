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
use crate::{SplitMetadata, SplitState};

#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
struct SplitMetadataV0 {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage URI resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// Number of records (or documents) in the split.
    /// TODO make u64
    #[serde(alias = "num_records")]
    pub num_docs: usize,

    /// Sum of the size (in bytes) of the documents in this split.
    ///
    /// Note this is not the split file size. It is the size of the original
    /// JSON payloads.
    pub size_in_bytes: u64,

    /// If a timestamp field is available, the min / max timestamp in
    /// the split.
    pub time_range: Option<RangeInclusive<i64>>,

    /// The state of the split. This is the only mutable attribute of the split.
    pub split_state: SplitState,

    /// Timestamp for tracking when the split was created.
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,

    /// Timestamp for tracking when the split was last updated.
    #[serde(default = "utc_now_timestamp")]
    pub update_timestamp: i64,

    /// A set of tags for categorizing and searching group of splits.
    #[serde(default)]
    pub tags: BTreeSet<String>,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
pub(crate) struct SplitMetadataAndFooterV0 {
    split_metadata: SplitMetadataV0,
    footer_offsets: Range<u64>,
}

impl From<SplitMetadataAndFooterV0> for SplitMetadata {
    fn from(v0: SplitMetadataAndFooterV0) -> Self {
        SplitMetadata {
            footer_offsets: v0.footer_offsets,
            split_id: v0.split_metadata.split_id,
            partition_id: 0,
            source_id: "unknown".to_string(),
            node_id: "unknown".to_string(),
            pipeline_ord: 0,
            delete_opstamp: 0,
            num_docs: v0.split_metadata.num_docs,
            uncompressed_docs_size_in_bytes: v0.split_metadata.size_in_bytes,
            time_range: v0.split_metadata.time_range,
            create_timestamp: v0.split_metadata.create_timestamp,
            tags: v0.split_metadata.tags,
            index_id: "".to_string(),
            num_merge_ops: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SplitMetadataV1 {
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

impl From<SplitMetadataV1> for SplitMetadata {
    fn from(v1: SplitMetadataV1) -> Self {
        let source_id = v1.source_id.unwrap_or_else(|| "unknown".to_string());

        let (node_id, pipeline_ord) = if let Some(node_id) = v1.node_id {
            if let Some((node_id, pipeline_ord)) = node_id.rsplit_once('/') {
                (
                    node_id.to_string(),
                    pipeline_ord.parse::<usize>().unwrap_or(0),
                )
            } else {
                (node_id.to_string(), 0)
            }
        } else {
            ("unknown".to_string(), 0)
        };

        SplitMetadata {
            split_id: v1.split_id,
            index_id: v1.index_id,
            partition_id: v1.partition_id,
            source_id,
            node_id,
            pipeline_ord,
            delete_opstamp: v1.delete_opstamp,
            num_docs: v1.num_docs,
            uncompressed_docs_size_in_bytes: v1.uncompressed_docs_size_in_bytes,
            time_range: v1.time_range,
            create_timestamp: v1.create_timestamp,
            tags: v1.tags,
            footer_offsets: v1.footer_offsets,
            num_merge_ops: v1.num_merge_ops,
        }
    }
}

impl From<SplitMetadata> for SplitMetadataV1 {
    fn from(split: SplitMetadata) -> Self {
        SplitMetadataV1 {
            split_id: split.split_id,
            index_id: split.index_id,
            partition_id: split.partition_id,
            source_id: Some(split.source_id),
            node_id: Some(format!("{}/{}", split.node_id, split.pipeline_ord)),
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
    #[serde(rename = "0")]
    V0(#[serde(skip_serializing)] SplitMetadataAndFooterV0),
    #[serde(rename = "1")]
    V1(SplitMetadataV1),
}

impl From<VersionedSplitMetadata> for SplitMetadata {
    fn from(versioned_helper: VersionedSplitMetadata) -> Self {
        match versioned_helper {
            VersionedSplitMetadata::V0(v0) => v0.into(),
            VersionedSplitMetadata::V1(v1) => v1.into(),
        }
    }
}

impl From<SplitMetadata> for VersionedSplitMetadata {
    fn from(split_metadata: SplitMetadata) -> Self {
        VersionedSplitMetadata::V1(split_metadata.into())
    }
}

impl<'de> Deserialize<'de> for SplitMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let split_metadata_value = serde_json::Value::deserialize(deserializer)?;
        // Unfortunately, it is not possible to tell serde that in the absence
        // of a tag, a given tag should be considered as the default.
        // Old serialized metadata do not contain the version tag and therefore we are required to
        // handle this corner case manually.
        let version_is_present = split_metadata_value
            .as_object()
            .map(|obj| obj.contains_key("version"))
            .unwrap_or(false);
        if !version_is_present {
            return Ok(
                serde_json::from_value::<SplitMetadataAndFooterV0>(split_metadata_value)
                    .map_err(serde::de::Error::custom)?
                    .into(),
            );
        }
        Ok(
            serde_json::from_value::<VersionedSplitMetadata>(split_metadata_value)
                .map_err(serde::de::Error::custom)?
                .into(),
        )
    }
}
