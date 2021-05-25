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
use serde::{Deserialize, Serialize};

use quickwit_doc_mapping::DocMapping;

use crate::MetastoreResult;

/// An index URI, such as `file:///var/lib/quickwit/indexes/nginx` or `s3://my-bucket/indexes/nginx`.
pub type IndexUri = String;

/// A split ID.
pub type SplitId = String;

pub static FILE_FORMAT_VERSION: &str = "0";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitMetadata {
    // Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    // should be enough to uniquely identify a split.
    // In reality, some information may be implicitly configured
    // in the storage URI resolver: for instance, the Amazon S3 region.
    pub split_id: String,

    // The state of the split
    pub split_state: SplitState,

    // Number of records (or documents) in the split.
    pub num_records: usize,

    // Weight of the split in bytes.
    pub size_in_bytes: usize,

    // If a timestamp field is available, the min / max timestamp in the split.
    pub time_range: Option<Range<u64>>,

    // Number of merge this segment has been subjected to during its lifetime.
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SplitState {
    // The split is newly created
    New,
    // The split is almost ready. Some of its files may have been uploaded in the storage.
    Staged,
    // The split is ready and published.
    Published,
    // The split is scheduled for deletion.
    ScheduledForDeletion,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataSet {
    index: IndexMetadata,
    splits: HashMap<SplitId, SplitMetadata>,
}

#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Index exists.
    async fn index_exists(&self, index_uri: IndexUri) -> MetastoreResult<bool>;

    /// Creates an index.
    async fn create_index(
        &self,
        index_uri: IndexUri,
        doc_mapping: DocMapping,
    ) -> MetastoreResult<()>;

    /// Opens an index.
    async fn open_index(&self, index_uri: IndexUri) -> MetastoreResult<()>;

    /// Deletes an index.
    async fn delete_index(&self, index_uri: IndexUri) -> MetastoreResult<()>;

    /// Stages a split.
    async fn stage_split(
        &self,
        index_uri: IndexUri,
        split_id: SplitId,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<SplitId>;

    /// Publishes a split.
    async fn publish_split(&self, index_uri: IndexUri, split_id: SplitId) -> MetastoreResult<()>;

    /// Lists the splits.
    async fn list_splits(
        &self,
        index_uri: IndexUri,
        split_state: SplitState,
        time_range: Option<Range<u64>>,
    ) -> MetastoreResult<Vec<SplitMetadata>>;

    async fn mark_split_as_deleted(
        &self,
        index_uri: IndexUri,
        split_id: SplitId,
    ) -> MetastoreResult<()>;

    /// Deletes a split.
    async fn delete_split(&self, index_uri: IndexUri, split_id: SplitId) -> MetastoreResult<()>;
}
