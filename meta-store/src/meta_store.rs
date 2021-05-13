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

use std::fmt::{Debug, Display};
use std::io;
use std::ops::Range;

use async_trait::async_trait;
use thiserror::Error;

pub type IndexId = String;
pub type SplitId = String;

pub enum State {
    // the splits is almost ready. Some of its files may have been uploaded in the storage.
    Staged,
    // the splits is ready and published.
    Published,
    // the split is scheduled to be deleted.
    ScheduledForDeleted,
}

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

/// A split manifest carries all meta data about a split
/// and its files.pub
pub struct SplitManifest {
    // the index it belongs to or is destined to belong to.
    id: IndexId,

    // The set of information required to open the split,
    // and possibly allocate the split.
    metadata: SplitMetaData,
    // The list of files associated to the split.
    // files: Vec<ManifestEntry>,
}

/// MetaStore error kind.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MetaStoreErrorKind {
    InvalidManifest,
    ExistingSplitId,
    InternalError,
    DoesNotExist,
    SplitIsNotStaged,
    IndexDoesNotExist,
    Forbidden,
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
pub trait MetaStore: Send + Sync + Debug + 'static {
    async fn stage_split(&self, split_manifest: SplitManifest) -> MetaStoreResult<SplitId>;
    async fn publish_split(&self, split_id: SplitId) -> MetaStoreResult<()>;
    async fn list_splits(
        &self,
        index_id: IndexId,
        state: State,
        time_range: Option<Range<u64>>,
    ) -> MetaStoreResult<()>;
    async fn mark_as_deleted(&self, split_id: SplitId) -> MetaStoreResult<()>;
    async fn delete_split(&self, split_id: SplitId) -> MetaStoreResult<()>;
}
