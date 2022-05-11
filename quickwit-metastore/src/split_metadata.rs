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

use core::fmt;
use std::collections::BTreeSet;
use std::ops::{Range, RangeInclusive};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::VersionedSplitMetadataDeserializeHelper;

/// Carries split metadata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Split {
    /// The state of the split.
    pub split_state: SplitState,

    /// Timestamp for tracking when the split was last updated.
    pub update_timestamp: i64,

    /// Immutable part of the split.
    #[serde(flatten)]
    pub split_metadata: SplitMetadata,
}

impl Split {
    /// Returns the split_id.
    pub fn split_id(&self) -> &str {
        &self.split_metadata.split_id
    }
}

/// Carries immutable split metadata.
/// This struct can deserialize older format automatically
/// but can only serialize to the last version.
#[derive(Clone, Eq, PartialEq, Default, Debug, Serialize)]
#[serde(into = "VersionedSplitMetadataDeserializeHelper")]
pub struct SplitMetadata {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage URI resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// Number of records (or documents) in the split.
    /// TODO make u64
    pub num_docs: usize,

    /// Sum of the size (in bytes) of the documents in this split.
    ///
    /// Note this is not the split file size. It is the size of the original
    /// JSON payloads.
    pub original_size_in_bytes: u64,

    /// If a timestamp field is available, the min / max timestamp in
    /// the split.
    pub time_range: Option<RangeInclusive<i64>>,

    /// Timestamp for tracking when the split was created.
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,

    /// Set of unique tags values of form `{field_name}:{field_value}`.
    /// The set is filled at indexing with values from each field registered
    /// in the [`DocMapping`](quickwit_config::DocMapping) `tag_fields` attribute and only when
    /// cardinality of a given field is less or equal to [`MAX_VALUES_PER_TAG_FIELD`].
    /// An additional special tag of the form `{field_name}!` is added to the set
    /// to indicate that this field `field_name` was indeed registered in `tag_fields`.
    /// When cardinality is strictly higher than [`MAX_VALUES_PER_TAG_FIELD`],
    /// no field value is added to the set.
    ///
    /// [`MAX_VALUES_PER_TAG_FIELD`]: https://github.com/quickwit-oss/quickwit/blob/main/quickwit-indexing/src/actors/packager.rs#L36
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

impl SplitMetadata {
    /// Creates a new instance of split metadata.
    pub fn new(split_id: String) -> Self {
        Self {
            split_id,
            num_docs: 0,
            original_size_in_bytes: 0,
            time_range: None,
            create_timestamp: utc_now_timestamp(),
            tags: Default::default(),
            demux_num_ops: 0,
            footer_offsets: Default::default(),
        }
    }

    /// Returns the split_id.
    pub fn split_id(&self) -> &str {
        &self.split_id
    }
}

/// A split state.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SplitState {
    /// The split is almost ready. Some of its files may have been uploaded in the storage.
    Staged,

    /// The split is ready and published.
    Published,

    /// The split is marked for deletion.
    MarkedForDeletion,
}

impl FromStr for SplitState {
    type Err = String;

    fn from_str(input: &str) -> Result<SplitState, Self::Err> {
        let split_state = match input {
            "Staged" => SplitState::Staged,
            "Published" => SplitState::Published,
            "MarkedForDeletion" => SplitState::MarkedForDeletion,
            "ScheduledForDeletion" => SplitState::MarkedForDeletion, // Deprecated
            "New" => SplitState::Staged,                             // Deprecated
            _ => return Err(format!("Unknown split state `{}`.", input)),
        };
        Ok(split_state)
    }
}

impl fmt::Display for SplitState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Helper function to provide a UTC now timestamp to use
/// as a default in deserialization.
///
/// During unit test, the value is constant.
pub fn utc_now_timestamp() -> i64 {
    if cfg!(any(test, feature = "testsuite")) {
        1640577000
    } else {
        OffsetDateTime::now_utc().unix_timestamp()
    }
}
