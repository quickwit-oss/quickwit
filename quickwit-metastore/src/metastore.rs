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

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::io;
use std::ops::Range;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// pub type IndexId = String;

pub type SplitId = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetaData {
    version: String,
    name: String,
    path: String,
}

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    file_name: String,
    file_size_in_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SplitState {
    // the splits is almost ready. Some of its files may have been uploaded in the storage.
    Staged,
    // the splits is ready and published.
    Published,
    // the split is scheduled to be deleted.
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

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaDataSet {
    index: IndexMetaData,
    // splits: Arc<RwLock<HashMap<SplitId, SplitManifest>>>,
    splits: HashMap<SplitId, SplitManifest>,
}

/// Metastore error kind.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MetastoreErrorKind {
    InvalidManifest,
    ExistingSplitId,
    InternalError,
    DoesNotExist,
    SplitIsNotStaged,
    IndexDoesNotExist,
    Forbidden,
    Io,
}

impl MetastoreErrorKind {
    /// Creates a MetastoreError.
    pub fn with_error<E>(self, source: E) -> MetastoreError
    where
        anyhow::Error: From<E>,
    {
        MetastoreError {
            kind: self,
            source: From::from(source),
        }
    }
}

impl From<MetastoreError> for io::Error {
    fn from(metastore_err: MetastoreError) -> Self {
        let io_error_kind = match metastore_err.kind() {
            MetastoreErrorKind::DoesNotExist => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(io_error_kind, metastore_err.source)
    }
}

/// Generic Metastore error.
#[derive(Error, Debug)]
#[error("MetastoreError(kind={kind:?}, source={source})")]
pub struct MetastoreError {
    kind: MetastoreErrorKind,
    #[source]
    source: anyhow::Error,
}

impl MetastoreError {
    /// Add some context to the wrapper error.
    pub fn add_context<C>(self, ctx: C) -> Self
    where
        C: Display + Send + Sync + 'static,
    {
        MetastoreError {
            kind: self.kind,
            source: self.source.context(ctx),
        }
    }

    /// Returns the corresponding `MetastoreErrorKind` for this error.
    pub fn kind(&self) -> MetastoreErrorKind {
        self.kind
    }
}

impl From<io::Error> for MetastoreError {
    fn from(err: io::Error) -> MetastoreError {
        MetastoreError {
            kind: MetastoreErrorKind::Io,
            source: anyhow::Error::from(err),
        }
    }
}

#[allow(dead_code)]
pub type MetastoreResult<T> = Result<T, MetastoreError>;

#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    async fn stage_split(
        &self,
        split_id: SplitId,
        split_manifest: SplitManifest,
    ) -> MetastoreResult<SplitId>;
    async fn publish_split(&self, split_id: SplitId) -> MetastoreResult<()>;
    async fn list_splits(
        &self,
        // index_id: IndexId,
        state: SplitState,
        time_range: Option<Range<u64>>,
    ) -> MetastoreResult<()>;
    async fn mark_as_deleted(&self, split_id: SplitId) -> MetastoreResult<()>;
    async fn delete_split(&self, split_id: SplitId) -> MetastoreResult<()>;
}
