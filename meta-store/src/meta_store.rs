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

mod single_file;

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::io;
use std::ops::Range;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A split ID.
pub type SplitId = String;

/// An index meta data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetaData {
    /// The meta store version.
    version: String,
    /// The index name.
    name: String,
    /// The index path.
    path: String,
}

/// A Split meta data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitMetaData {
    // Split uri. In spirit, this uri should be self sufficient
    // to identify a split.
    // In reality, some information may be implicitly configure
    // in the store uri resolver, such as the Amazon S3 region.
    split_uri: String,

    // Number of records (or document) in the split
    num_records: u64,

    // Weight in bytes of the split
    size_in_bytes: u64,

    // if a time field is available, the min / max timestamp in the segment.
    time_range: Option<Range<u64>>,

    // Number of merge this segment has been subjected to during its lifetime.
    generation: usize,
}

/// A split manifest entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// The file name.
    file_name: String,
    /// File siize in bytes.
    file_size_in_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SplitState {
    // The splits is almost ready. Some of its files may have been uploaded in the storage.
    Staged,
    // The splits is ready and published.
    Published,
    // The split is scheduled to be deleted.
    ScheduledForDeleted,
}

/// A split manifest carries all meta data about a split
/// and its files.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitManifest {
    // The set of information required to open the split,
    // and possibly allocate the split.
    metadata: SplitMetaData,

    // The list of files associated to the split.
    files: Vec<ManifestEntry>,

    // The state of split
    state: SplitState,
}

/// A container for index and split metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct MetaDataSet {
    // The index meta data.
    index: IndexMetaData,

    // The list of split manifests.
    splits: HashMap<SplitId, SplitManifest>,
}

/// MetaStore error kind.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MetaStoreErrorKind {
    /// The split manifest is invalid.
    InvalidManifest,
    /// The split ID already exists.
    ExistingSplitId,
    /// Any generic internal error.
    InternalError,
    /// The split does not exist.
    DoesNotExist,
    /// The split is not staged.
    SplitIsNotStaged,
    /// The index does not exist.
    IndexDoesNotExist,
    Forbidden,
    /// Io error.
    Io,
}

impl MetaStoreErrorKind {
    /// Creates a MetaStoreError.
    pub fn with_error<E>(self, source: E) -> MetaStoreError
    where
        anyhow::Error: From<E>,
    {
        MetaStoreError {
            kind: self,
            source: From::from(source),
        }
    }
}

impl From<MetaStoreError> for io::Error {
    fn from(metastore_err: MetaStoreError) -> Self {
        let io_error_kind = match metastore_err.kind() {
            MetaStoreErrorKind::DoesNotExist => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(io_error_kind, metastore_err.source)
    }
}

/// Generic MetaStore error.
#[derive(Error, Debug)]
#[error("MetaStoreError(kind={kind:?}, source={source})")]
pub struct MetaStoreError {
    kind: MetaStoreErrorKind,
    #[source]
    source: anyhow::Error,
}

impl MetaStoreError {
    /// Add some context to the wrapper error.
    pub fn add_context<C>(self, ctx: C) -> Self
    where
        C: Display + Send + Sync + 'static,
    {
        MetaStoreError {
            kind: self.kind,
            source: self.source.context(ctx),
        }
    }

    /// Returns the corresponding `MetaStoreErrorKind` for this error.
    pub fn kind(&self) -> MetaStoreErrorKind {
        self.kind
    }
}

impl From<io::Error> for MetaStoreError {
    fn from(err: io::Error) -> MetaStoreError {
        MetaStoreError {
            kind: MetaStoreErrorKind::Io,
            source: anyhow::Error::from(err),
        }
    }
}

pub type MetaStoreResult<T> = Result<T, MetaStoreError>;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Stages a splits.
    /// A split needs to be staged BEFORE uploading any of its files to the storage.
    /// The SplitId is returned for convenienced but it is not generated
    /// by the metastore. In fact it was supplied by the client and is present in the split manifest.
    async fn stage_split(
        &self,
        split_id: SplitId,
        split_manifest: SplitManifest,
    ) -> MetaStoreResult<SplitId>;
    /// Records a split as published.
    ///
    /// This API is typically used by an indexer who needs to publish a new split.
    /// At this point, the split files are assumed to have already uploaded.
    /// The metastore only updates the state of the split from staging to published.
    ///
    /// It has two side effetcs:
    /// - it makes the split visible to other clients via the ListSplit API.
    /// - it guards eventual recovery procedure from removing the split files.
    ///
    /// Unless specified otherwise in the implementation, the metastore DOES NOT
    /// do any check on the presence, integrity or validity of the metadata of this split.
    ///
    /// A split successfully published MUST eventually be visible to all clients.
    /// Stronger consistency semantics should be documented in the implementation.
    ///
    /// If the split is already published, this API call returns a success.
    async fn publish_split(&self, split_id: SplitId) -> MetaStoreResult<()>;
    /// Returns the list of published splits intersecting with the given time_range.
    /// Regardless of the timerange filter, if a split has no timestamp it is always returned.
    /// Splits are returned in any order
    async fn list_splits(
        &self,
        state: SplitState,
        time_range: Option<Range<u64>>,
    ) -> MetaStoreResult<HashMap<SplitId, SplitManifest>>;
    /// Marks a split as deleted.
    /// This function is successful if a split was already marked as deleted.
    async fn mark_as_deleted(&self, split_id: SplitId) -> MetaStoreResult<()>;
    /// This function only takes split that are in staging or in mark as deleted state.
    async fn delete_split(&self, split_id: SplitId) -> MetaStoreResult<()>;
}
