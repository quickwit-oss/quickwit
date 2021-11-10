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

#[cfg(feature = "postgres")]
pub mod postgresql_metastore;
pub mod single_file_metastore;

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::ops::{Range, RangeInclusive};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quickwit_index_config::IndexConfig;
use serde::{Deserialize, Serialize};

use crate::checkpoint::{Checkpoint, CheckpointDelta};
use crate::MetastoreResult;

/// An index metadata carries all meta data about an index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Index ID. The index ID identifies the index when querying the metastore.
    pub index_id: String,
    /// Index URI. The index URI defines the location of the storage that contains the
    /// split files.
    pub index_uri: String,
    /// The config used for this index.
    pub index_config: Arc<dyn IndexConfig>,
    /// Checkpoint relative to a source. It express up to where documents have been indexed.
    pub checkpoint: Checkpoint,
}

/// Carries split and bundle offsets for single read metadata.
#[derive(Clone, Eq, PartialEq, Default, Debug, Serialize, Deserialize)]
pub struct SplitMetadataAndFooterOffsets {
    /// A split metadata carries all meta data about a split.
    pub split_metadata: SplitMetadata,
    /// Contains the range of bytes of the footer that needs to be downloaded
    /// in order to open a split.
    pub footer_offsets: Range<u64>,
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
    pub num_records: usize,

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
    pub created_at: i64,

    /// Timestamp for tracking when the split was updated.
    pub updated_at: i64,

    /// A set of tags for categorizing and searching group of splits.
    #[serde(default)]
    pub tags: HashSet<String>,

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
            num_records: 0,
            size_in_bytes: 0,
            time_range: None,
            created_at: Utc::now().timestamp(),
            updated_at: Utc::now().timestamp(),
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

/// A MetadataSet carries an index metadata and its split metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataSet {
    /// Metadata specific to the index.
    pub index: IndexMetadata,
    /// List of splits belonging to the index.
    pub splits: HashMap<String, SplitMetadataAndFooterOffsets>,
}

/// Metastore meant to manage Quickwit's indexes and their splits.
///
/// Quickwit needs a way to ensure that we can cleanup unused files,
/// and this process needs to be resilient to any fail-stop failures.
/// We rely on atomically transitioning the status of splits.
///
/// The split state goes through the following life cycle:
/// 1. `New`
///   - Create new split and start indexing.
/// 2. `Staged`
///   - Start uploading the split files.
/// 3. `Published`
///   - Uploading the split files is complete and the split is searchable.
/// 4. `ScheduledForDeletion`
///   - Mark the split for deletion.
///
/// If a split has a file in the storage, it MUST be registered in the metastore,
/// and its state can be as follows:
/// - `Staged`: The split is almost ready. Some of its files may have been uploaded in the storage.
/// - `Published`: The split is ready and published.
/// - `ScheduledForDeletion`: The split is scheduled for deletion.
///
/// Before creating any file, we need to stage the split. If there is a failure, upon recovery, we
/// schedule for deletion all the staged splits. A client may not necessarily remove files from
/// storage right after marking it for deletion. A CLI client may delete files right away, but a
/// more serious deployment should probably only delete those files after a grace period so that the
/// running search queries can complete.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Checks if the metastore is available.
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    /// Checks if the given index is in this metastore.
    async fn check_index_available(&self, index_id: &str) -> anyhow::Result<()> {
        self.index_metadata(index_id).await?;
        Ok(())
    }

    /// Creates an index.
    /// This API creates index metadata set in the metastore.
    /// An error will occur if an index that already exists in the storage is specified.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()>;

    /// Returns the index_metadata for a given index.
    ///
    /// TODO consider merging with list_splits to remove one round-trip
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata>;

    /// Deletes an index.
    /// This API removes the specified index metadata set from the metastore,
    /// but does not remove the index from the storage.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()>;

    /// Stages a split.
    /// A split needs to be staged before uploading any of its files to the storage.
    /// An error will occur if an index that does not exist in the storage is specified.
    /// Also, an error will occur if you specify a split that already exists.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadataAndFooterOffsets,
    ) -> MetastoreResult<()>;

    /// Publishes a list splits.
    /// This API only updates the state of the split from `Staged` to `Published`.
    /// At this point, the split files are assumed to have already been uploaded.
    /// If the split is already published, this API call returns a success.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()>;

    /// Replaces a list of splits with another list.
    /// This API is useful during merge and demux operations.
    /// The new splits should be staged, and the replaced splits should exist.
    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Lists the splits.
    /// Returns a list of splits that intersects the given `time_range`, `split_state` and `tag`.
    /// Regardless of the time range filter, if a split has no timestamp it is always returned.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn list_splits(
        &self,
        index_id: &str,
        split_state: SplitState,
        time_range: Option<Range<i64>>,
        tags: &[String],
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>>;

    /// Lists the splits without filtering.
    /// Returns a list of all splits currently known to the metastore regardless of their state.
    async fn list_all_splits(
        &self,
        index_id: &str,
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>>;

    /// Marks a list of splits for deletion.
    /// This API will change the state to `ScheduledForDeletion` so that it is not referenced by the
    /// client. It actually does not remove the split from storage.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Deletes a list of splits.
    /// This API only takes a split that is in `Staged` or `ScheduledForDeletion` state.
    /// This removes the split metadata from the metastore, but does not remove the split from
    /// storage. An error will occur if you specify an index or split that does not exist in the
    /// storage.
    async fn delete_splits<'a>(&self, index_id: &str, split_ids: &[&'a str])
        -> MetastoreResult<()>;

    /// Returns the Metastore uri.
    fn uri(&self) -> String;
}

// Returns true if filter_tags is empty (unspecified),
// or if filter_tags is specified and split_tags contains at least one of the tags in filter_tags.
pub fn match_tags_filter(split_tags: &[String], filter_tags: &[String]) -> bool {
    if filter_tags.is_empty() {
        return true;
    }
    for filter_tag in filter_tags {
        if split_tags.contains(filter_tag) {
            return true;
        }
    }
    false
}
