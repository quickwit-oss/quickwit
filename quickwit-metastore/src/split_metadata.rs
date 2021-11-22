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
use std::fmt;
use std::ops::{Range, RangeInclusive};
use std::str::FromStr;

use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Carries split metadata and footer offsets. The footer offsets
/// make it possible to download the footer in a single call to `.get_slice(...)`.
///
/// This struct can deserialize older format automatically
/// but can only serialize to the last version.
#[derive(Clone, Eq, PartialEq, Default, Debug, Serialize)]
#[serde(into = "VersionedSplitMetadataDeserializeHelper")]
pub struct SplitMetadataAndFooterOffsets {
    /// A split metadata carries all meta data about a split.
    pub split_metadata: SplitMetadata,
    /// Contains the range of bytes of the footer that needs to be downloaded
    /// in order to open a split.
    pub footer_offsets: Range<u64>,
}

#[derive(Clone, Eq, PartialEq, Default, Debug, Serialize, Deserialize)]
pub(crate) struct SplitMetadataAndFooterOffsetsV0 {
    split_metadata: SplitMetadata,
    footer_offsets: Range<u64>,
}

impl From<SplitMetadataAndFooterOffsetsV0> for SplitMetadataAndFooterOffsets {
    fn from(v0: SplitMetadataAndFooterOffsetsV0) -> Self {
        SplitMetadataAndFooterOffsets {
            split_metadata: v0.split_metadata,
            footer_offsets: v0.footer_offsets,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedSplitMetadataDeserializeHelper {
    #[serde(rename = "0")]
    V0(SplitMetadataAndFooterOffsetsV0),
}

impl From<VersionedSplitMetadataDeserializeHelper> for SplitMetadataAndFooterOffsets {
    fn from(versioned_helper: VersionedSplitMetadataDeserializeHelper) -> Self {
        match versioned_helper {
            VersionedSplitMetadataDeserializeHelper::V0(split_metadata) => split_metadata.into(),
        }
    }
}

impl From<SplitMetadataAndFooterOffsets> for VersionedSplitMetadataDeserializeHelper {
    fn from(split_metadata_and_footer_offsets: SplitMetadataAndFooterOffsets) -> Self {
        VersionedSplitMetadataDeserializeHelper::V0(SplitMetadataAndFooterOffsetsV0 {
            split_metadata: split_metadata_and_footer_offsets.split_metadata,
            footer_offsets: split_metadata_and_footer_offsets.footer_offsets,
        })
    }
}

impl<'de> Deserialize<'de> for SplitMetadataAndFooterOffsets {
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
            return Ok(serde_json::from_value::<SplitMetadataAndFooterOffsetsV0>(
                split_metadata_value,
            )
            .map_err(serde::de::Error::custom)?
            .into());
        }
        Ok(
            serde_json::from_value::<VersionedSplitMetadataDeserializeHelper>(split_metadata_value)
                .map_err(serde::de::Error::custom)?
                .into(),
        )
    }
}

/// A split metadata carries all meta data about a split.
#[derive(Clone, Eq, PartialEq, Default, Debug, Serialize, Deserialize)]
pub struct SplitMetadata {
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

impl SplitMetadata {
    /// Creates a new instance of split metadata.
    pub fn new(split_id: String) -> Self {
        Self {
            split_id,
            split_state: SplitState::New,
            num_docs: 0,
            size_in_bytes: 0,
            time_range: None,
            create_timestamp: Utc::now().timestamp(),
            update_timestamp: Utc::now().timestamp(),
            tags: Default::default(),
            demux_num_ops: 0,
        }
    }
}

/// A split state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SplitState {
    /// The split is newly created.
    New,

    /// The split is almost ready. Some of its files may have been uploaded in the storage.
    Staged,

    /// The split is ready and published.
    Published,

    /// The split is scheduled for deletion.
    ScheduledForDeletion,
}

impl Default for SplitState {
    fn default() -> Self {
        Self::New
    }
}

impl FromStr for SplitState {
    type Err = &'static str;

    fn from_str(input: &str) -> Result<SplitState, Self::Err> {
        match input {
            "New" => Ok(SplitState::New),
            "Staged" => Ok(SplitState::Staged),
            "Published" => Ok(SplitState::Published),
            "ScheduledForDeletion" => Ok(SplitState::ScheduledForDeletion),
            _ => Err("Unknown split state"),
        }
    }
}

impl fmt::Display for SplitState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Helper function to provide a default UTC timestamp.
fn utc_now_timestamp() -> i64 {
    Utc::now().timestamp()
}
