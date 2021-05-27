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

/// A file format version.
const FILE_FORMAT_VERSION: &str = "0";

/// An index metadata carries all meta data about an index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    version: String,
}

/// A split metadata carries all meta data about a split.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitMetadata {
    // Split ID. Joined with the index URI (<index URI>/<split ID>), this ID
    // should be enough to uniquely identify a split.
    // In reality, some information may be implicitly configured
    // in the storage URI resolver: for instance, the Amazon S3 region.
    split_id: String,

    // The state of the split
    split_state: SplitState,

    // Number of records (or documents) in the split.
    num_records: u64,

    // Weight of the split in bytes.
    size_in_bytes: u64,

    // If a timestamp field is available, the min / max timestamp in the split.
    time_range: Option<Range<u64>>,

    // Number of merge this segment has been subjected to during its lifetime.
    generation: usize,
}

/// A split state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SplitState {
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
    index: IndexMetadata,
    splits: HashMap<String, SplitMetadata>,
}

/// Metastore meant to manage quickwit's indices and its splits.
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Creates an index.
    async fn create_index(&self, index_id: &str, doc_mapping: DocMapping) -> MetastoreResult<()>;

    /// Deletes an index.
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()>;

    /// Stages a split.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()>;

    /// Publishes a split.
    async fn publish_split(&self, index_id: &str, split_id: &str) -> MetastoreResult<()>;

    /// Lists the splits.
    async fn list_splits(
        &self,
        index_id: &str,
        split_state: SplitState,
        time_range: Option<Range<u64>>,
    ) -> MetastoreResult<Vec<SplitMetadata>>;

    /// Marks split as deleted.
    async fn mark_split_as_deleted(&self, index_id: &str, split_id: &str) -> MetastoreResult<()>;

    /// Deletes a split.
    async fn delete_split(&self, index_id: &str, split_id: &str) -> MetastoreResult<()>;
}
