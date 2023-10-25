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
use std::fmt;
use std::ops::{Range, RangeInclusive};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use byte_unit::Byte;
use quickwit_proto::types::IndexUid;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds};
use time::OffsetDateTime;

use crate::split_metadata_version::VersionedSplitMetadata;

/// Carries split metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Split {
    /// The state of the split.
    pub split_state: SplitState,

    /// Timestamp for tracking when the split was last updated.
    pub update_timestamp: i64,

    /// Timestamp for tracking when the split was published.
    pub publish_timestamp: Option<i64>,

    #[serde(flatten)]
    #[schema(value_type = VersionedSplitMetadata)]
    /// Immutable part of the split.
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
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(into = "VersionedSplitMetadata")]
#[serde(try_from = "VersionedSplitMetadata")]
pub struct SplitMetadata {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// Id of the index this split belongs to.
    pub index_uid: IndexUid,

    /// Partition to which the split belongs to.
    ///
    /// Partitions are usually meant to isolate documents based on some field like
    /// `tenant_id`. For this reason, ideally splits with a different `partition_id`
    /// should not be merged together. Merging two splits with different `partition_id`
    /// does not hurt correctness however.
    pub partition_id: u64,

    /// Source ID.
    pub source_id: String,

    /// Node ID.
    pub node_id: String,

    /// Number of records (or documents) in the split.
    /// TODO make u64
    pub num_docs: usize,

    /// Sum of the size (in bytes) of the raw documents in this split.
    ///
    /// Note this is not the split file size. It is the size of the original
    /// JSON payloads.
    pub uncompressed_docs_size_in_bytes: u64,

    /// If a timestamp field is available, the min / max timestamp in
    /// the split, expressed in seconds.
    pub time_range: Option<RangeInclusive<i64>>,

    /// Timestamp for tracking when the split was created.
    pub create_timestamp: i64,

    /// Split maturity either `Mature` or `Immature` with a given maturation period.
    pub maturity: SplitMaturity,

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
    pub tags: BTreeSet<String>,

    /// Contains the range of bytes of the footer that needs to be downloaded
    /// in order to open a split.
    ///
    /// The footer offsets
    /// make it possible to download the footer in a single call to `.get_slice(...)`.
    pub footer_offsets: Range<u64>,

    /// Delete opstamp.
    pub delete_opstamp: u64,

    /// Number of merge operations that was involved to create
    /// this split.
    pub num_merge_ops: usize,
}

impl SplitMetadata {
    /// Creates a new instance of split metadata.
    pub fn new(
        split_id: String,
        index_uid: IndexUid,
        partition_id: u64,
        source_id: String,
        node_id: String,
    ) -> Self {
        Self {
            split_id,
            index_uid,
            partition_id,
            source_id,
            node_id,
            create_timestamp: utc_now_timestamp(),
            ..Default::default()
        }
    }

    /// Returns the split_id.
    pub fn split_id(&self) -> &str {
        &self.split_id
    }

    /// Returns true if the split is mature at the unix `timestamp`.
    pub fn is_mature(&self, datetime: OffsetDateTime) -> bool {
        match self.maturity {
            SplitMaturity::Mature => true,
            SplitMaturity::Immature {
                maturation_period: time_to_maturity,
            } => {
                self.create_timestamp + time_to_maturity.as_secs() as i64
                    <= datetime.unix_timestamp()
            }
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    /// Returns an instance of `SplitMetadata` for testing.
    pub fn for_test(split_id: String) -> Self {
        Self {
            split_id,
            ..Default::default()
        }
    }

    /// Converts the split metadata into a [`SplitInfo`].
    pub fn as_split_info(&self) -> SplitInfo {
        let file_name = quickwit_common::split_file(self.split_id());

        SplitInfo {
            uncompressed_docs_size_bytes: Byte::from_bytes(self.uncompressed_docs_size_in_bytes),
            file_name: PathBuf::from(file_name),
            file_size_bytes: Byte::from_bytes(self.footer_offsets.end),
            split_id: self.split_id.clone(),
            num_docs: self.num_docs,
        }
    }
}

/// A summarized version of the split metadata for display purposes.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SplitInfo {
    /// The split ID.
    pub split_id: String,
    /// The number of documents in the split.
    pub num_docs: usize,
    /// The sum of the sizes of the original JSON payloads in bytes.
    #[schema(value_type = u64)]
    pub uncompressed_docs_size_bytes: Byte,
    /// The name of the split file on disk.
    #[schema(value_type = String)]
    pub file_name: PathBuf,
    /// The size of the split file on disk in bytes.
    #[schema(value_type = u64)]
    pub file_size_bytes: Byte,
}

#[cfg(any(test, feature = "testsuite"))]
impl quickwit_config::TestableForRegression for SplitMetadata {
    fn sample_for_regression() -> Self {
        use ulid::Ulid;

        SplitMetadata {
            split_id: "split".to_string(),
            index_uid: IndexUid::from_parts("my-index", Ulid::nil()),
            source_id: "source".to_string(),
            node_id: "node".to_string(),
            delete_opstamp: 10,
            partition_id: 7u64,
            num_docs: 12303,
            uncompressed_docs_size_in_bytes: 234234,
            time_range: Some(121000..=130198),
            create_timestamp: 3,
            maturity: SplitMaturity::Immature {
                maturation_period: Duration::from_secs(4),
            },
            tags: ["234".to_string(), "aaa".to_string()].into_iter().collect(),
            footer_offsets: 1000..2000,
            num_merge_ops: 3,
        }
    }

    fn test_equality(&self, other: &Self) {
        assert_eq!(self, other);
    }
}

/// A split state.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, utoipa::ToSchema)]
pub enum SplitState {
    /// The split is almost ready. Some of its files may have been uploaded in the storage.
    Staged,

    /// The split is ready and published.
    Published,

    /// The split is marked for deletion.
    MarkedForDeletion,
}

impl fmt::Display for SplitState {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl SplitState {
    /// Returns a string representation of the given enum.
    pub fn as_str(&self) -> &'static str {
        match self {
            SplitState::Staged => "Staged",
            SplitState::Published => "Published",
            SplitState::MarkedForDeletion => "MarkedForDeletion",
        }
    }
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
            _ => return Err(format!("unknown split state `{input}`")),
        };
        Ok(split_state)
    }
}

/// `SplitMaturity` defines the maturity of a split, it is either `Mature`
/// or `Immature` with a given maturation period.
/// The maturity is determined by the `MergePolicy`.
#[serde_as]
#[derive(Clone, Copy, Debug, Default, Eq, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum SplitMaturity {
    /// The split is mature and no longer a candidates for merges.
    #[default]
    Mature,
    /// The split is immature and can undergo merges until `maturation_period` passes,
    /// measured relatively from the split's creation timestamp.
    Immature {
        /// Maturation period.
        #[serde_as(as = "DurationMilliSeconds<u64>")]
        #[serde(rename = "maturation_period_millis")]
        maturation_period: Duration,
    },
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_split_maturity_serialization() {
        {
            let split_maturity = super::SplitMaturity::Immature {
                maturation_period: std::time::Duration::from_millis(10),
            };
            let serialized = serde_json::to_string(&split_maturity).unwrap();
            assert_eq!(
                serialized,
                r#"{"type":"immature","maturation_period_millis":10}"#
            );
            let deserialized: super::SplitMaturity = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, split_maturity);
        }
        {
            let split_maturity = super::SplitMaturity::Mature;
            let serialized = serde_json::to_string(&split_maturity).unwrap();
            assert_eq!(serialized, r#"{"type":"mature"}"#);
            let deserialized: super::SplitMaturity = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, split_maturity);
        }
    }
}
