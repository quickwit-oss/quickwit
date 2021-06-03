/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

pub mod single_file_metastore;

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;

use async_trait::async_trait;
use quickwit_doc_mapping::DocMapperType;
use serde::{Deserialize, Serialize};

use crate::MetastoreResult;

/// An index metadata carries all meta data about an index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Index Id. The index id serves to identify the index when querying the metastore.
    pub index_id: String,
    /// Index Uri. The index uri defines the location of the storage that contains the
    /// split files.
    pub index_uri: String,
    /// The doc mapper type used for this index
    pub doc_mapper_type: DocMapperType,
}

/// A split metadata carries all meta data about a split.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitMetadata {
    /// Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    /// should be enough to uniquely identify a split.
    /// In reality, some information may be implicitly configured
    /// in the storage URI resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    /// The state of the split
    pub split_state: SplitState,

    /// Number of records (or documents) in the split.
    pub num_records: usize,

    /// Weight of the split in bytes.
    pub size_in_bytes: usize,

    /// If a timestamp field is available, the min / max timestamp in the split.
    pub time_range: Option<Range<u64>>,

    /// Number of merge this segment has been subjected to during its lifetime.
    pub generation: usize,
}

impl SplitMetadata {
    /// Creates a new instance of split metadata
    pub fn new(split_id: String) -> Self {
        Self {
            split_id,
            split_state: SplitState::New,
            num_records: 0,
            size_in_bytes: 0,
            time_range: None,
            generation: 0,
        }
    }
}

/// A split state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SplitState {
    /// The split is newly created
    New,

    /// The split is almost ready. Some of its files may have been uploaded in the storage.
    Staged,

    /// The split is ready and published.
    Published,

    /// The split is scheduled for deletion.
    ScheduledForDeletion,
}

/// A MetadataSet carries an index metadata and its split metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataSet {
    /// Metadata specific to the index
    pub index: IndexMetadata,
    /// List of split belonging to the index.
    pub splits: HashMap<String, SplitMetadata>,
}

/// Metastore meant to manage quickwit's indices and its splits.
///
/// Quickwit needs a way to ensure that we can cleanup unused files,
/// and this process needs to be resilient to any fail-stop failures.
/// We rely atomically shifting the status of splits.
///
/// The split state goes into the following life cycle:
/// 1. New
///   - Create new split and start indexing.
/// 2. Staged
///   - Start uploading the split files.
/// 3. Published
///   - Uploading the split files are complete and searchable.
/// 4. ScheduledForDeletion
///   - Mark the split for deletion.
///
/// If a split has a files in the storage, it MUST be registered in the metastore,
/// and its state can be follows.
/// - Staged : The split is almost ready. Some of its files may have been uploaded in the storage.
/// - Published : The split is ready and published.
/// - ScheduledForDeletion : The split is scheduled for deletion.
///
/// Before creating any file, we need to be Staged the split. If there is a failure, upon recovery we schedule for delete all of the staged splits.
/// A client may not necessarily remove file from storage right after marking it as deleted.
/// A CLI client may delete files right away, but a more serious deployment should probably
/// only delete these files after a period of time to give some time to running search queries to finish.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Creates an index.
    /// This API creates index metadata set in the metastore.
    /// An error will occur if an index that exists in the storage is specified.
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
    /// A split needs to be Staged before uploading any of its files to the storage.
    /// An error will occur if an index that does not exist in the storage is specified.
    /// Also, an error will occur if you specify a split that already exists.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()>;

    /// Publishes a split.
    /// This API only updates the state of the split from Staged to Published.
    /// At this point, the split files are assumed to have already been uploaded.
    /// If the split is already published, this API call returns a success.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: Vec<&'a str>,
    ) -> MetastoreResult<()>;

    /// Lists the splits.
    /// Returns a list of splits that intersect the given time_range and split_state.
    /// Regardless of the timerange filter, if a split has no timestamp it is always returned.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn list_splits(
        &self,
        index_id: &str,
        split_state: SplitState,
        time_range: Option<Range<u64>>,
    ) -> MetastoreResult<Vec<SplitMetadata>>;

    /// Marks split as deleted.
    /// This API will change the state to ScheduledForDeletion so that it is not referenced by the client.
    /// It does not actually remove the split from storage.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    async fn mark_split_as_deleted(&self, index_id: &str, split_id: &str) -> MetastoreResult<()>;

    /// Deletes a split.
    /// This API only takes a split that is in Staged or ScheduledForDeletion state.
    /// This removes the split metadata from the metastore, but does not remove the split from storage.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    async fn delete_split(&self, index_id: &str, split_id: &str) -> MetastoreResult<()>;
}
