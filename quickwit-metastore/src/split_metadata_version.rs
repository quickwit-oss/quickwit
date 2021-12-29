// Copyright (C) 2021 Quickwit, Inc.
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

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
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

    /// Number of demux operations this split has undergone.
    #[serde(default)]
    pub demux_num_ops: usize,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct SplitMetadataAndFooterV0 {
    split_metadata: SplitMetadataV0,
    footer_offsets: Range<u64>,
}

impl From<SplitMetadataAndFooterV0> for SplitMetadata {
    fn from(v0: SplitMetadataAndFooterV0) -> Self {
        SplitMetadata {
            footer_offsets: v0.footer_offsets,
            split_id: v0.split_metadata.split_id,
            num_docs: v0.split_metadata.num_docs,
            original_size_in_bytes: v0.split_metadata.size_in_bytes,
            time_range: v0.split_metadata.time_range,
            create_timestamp: v0.split_metadata.create_timestamp,
            tags: v0.split_metadata.tags,
            demux_num_ops: v0.split_metadata.demux_num_ops,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Default, Debug, Serialize, Deserialize)]
pub(crate) struct SplitMetadataV1 {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage URI resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// Number of records (or documents) in the split.
    pub num_docs: usize,

    /// Sum of the size (in bytes) of the documents in this split.
    ///
    /// Note this is not the split file size. It is the size of the original
    /// JSON payloads.
    pub size_in_bytes: u64,

    /// If a timestamp field is available, the min / max timestamp in
    /// the split.
    pub time_range: Option<RangeInclusive<i64>>,

    /// Timestamp for tracking when the split was created.
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,

    /// A set of tags for categorizing and searching group of splits.
    #[serde(default)]
    pub tags: BTreeSet<String>,

    /// Number of demux operations this split has undergone.
    #[serde(default)]
    pub demux_num_ops: usize,

    /// Contains the range of bytes of the footer that needs to be downloaded
    /// in order to open a split.
    ///
    /// The footer offsets
    /// make it possible to download the footer in a single call to `.get_slice(...)`.
    pub footer_offsets: Range<u64>,
}

impl From<SplitMetadataV1> for SplitMetadata {
    fn from(v1: SplitMetadataV1) -> Self {
        SplitMetadata {
            footer_offsets: v1.footer_offsets,
            split_id: v1.split_id,
            num_docs: v1.num_docs,
            original_size_in_bytes: v1.size_in_bytes,
            time_range: v1.time_range,
            create_timestamp: v1.create_timestamp,
            tags: v1.tags,
            demux_num_ops: v1.demux_num_ops,
        }
    }
}

impl From<SplitMetadata> for SplitMetadataV1 {
    fn from(v1: SplitMetadata) -> Self {
        SplitMetadataV1 {
            footer_offsets: v1.footer_offsets,
            split_id: v1.split_id,
            num_docs: v1.num_docs,
            size_in_bytes: v1.original_size_in_bytes,
            time_range: v1.time_range,
            create_timestamp: v1.create_timestamp,
            tags: v1.tags,
            demux_num_ops: v1.demux_num_ops,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedSplitMetadataDeserializeHelper {
    #[serde(rename = "0")]
    V0(SplitMetadataAndFooterV0),
    #[serde(rename = "1")]
    V1(SplitMetadataV1),
}

impl From<VersionedSplitMetadataDeserializeHelper> for SplitMetadata {
    fn from(versioned_helper: VersionedSplitMetadataDeserializeHelper) -> Self {
        match versioned_helper {
            VersionedSplitMetadataDeserializeHelper::V0(split_metadata) => split_metadata.into(),
            VersionedSplitMetadataDeserializeHelper::V1(split_metadata) => split_metadata.into(),
        }
    }
}

impl From<SplitMetadata> for VersionedSplitMetadataDeserializeHelper {
    fn from(split_metadata: SplitMetadata) -> Self {
        VersionedSplitMetadataDeserializeHelper::V1(split_metadata.into())
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
            serde_json::from_value::<VersionedSplitMetadataDeserializeHelper>(split_metadata_value)
                .map_err(serde::de::Error::custom)?
                .into(),
        )
    }
}
