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

use std::ops::Range;

use async_trait::async_trait;
use rusoto_core::Region;

use quickwit_storage::S3CompatibleObjectStorage;

use crate::meta_store::{IndexId, MetaStore, MetaStoreResult, SplitId, SplitManifest, State};

/// S3 Compatible object storage implementation.
#[derive(Debug)]
pub struct S3CompatibleMetaStore {
    storage: S3CompatibleObjectStorage,
}

impl S3CompatibleMetaStore {
    /// Creates an object storage given a region and a bucket name.
    pub fn new(region: Region, bucket: &str) -> anyhow::Result<S3CompatibleMetaStore> {
        let storage = S3CompatibleObjectStorage::new(region, bucket)?;

        Ok(S3CompatibleMetaStore { storage })
    }

    /// Creates an object storage given a region and an uri.
    pub fn from_uri(region: Region, uri: &str) -> anyhow::Result<S3CompatibleMetaStore> {
        let storage = S3CompatibleObjectStorage::from_uri(region, uri)?;

        Ok(S3CompatibleMetaStore { storage })
    }
}

#[async_trait]
impl MetaStore for S3CompatibleMetaStore {
    async fn stage_split(&self, _split_manifest: SplitManifest) -> MetaStoreResult<SplitId> {
        unimplemented!();
    }

    async fn publish_split(&self, _split_id: SplitId) -> MetaStoreResult<()> {
        unimplemented!();
    }

    async fn list_splits(
        &self,
        _index_id: IndexId,
        _state: State,
        _time_range: Option<Range<u64>>,
    ) -> MetaStoreResult<()> {
        unimplemented!();
    }

    async fn mark_as_deleted(&self, _split_id: SplitId) -> MetaStoreResult<()> {
        unimplemented!();
    }

    async fn delete_split(&self, _split_id: SplitId) -> MetaStoreResult<()> {
        unimplemented!();
    }
}
