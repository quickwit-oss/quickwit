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

use std::sync::Arc;

pub use rusoto_core::Region;

use crate::{S3CompatibleObjectStorage, StorageFactory};

/// S3 Object storage Uri Resolver
///
/// The default implementation uses s3 as a protocol, and detects the region using the
/// `AWS_DEFAULT_REGION` or `AWS_REGION` environment variable. If it is malformed,
/// it will fall back to `Region::UsEast1`.
pub struct S3CompatibleObjectStorageFactory {
    region: Region,
    protocol: &'static str,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3CompatibleObjetStorageFactory with the given AWS region.
    pub fn new(region: Region, protocol: &'static str) -> Self {
        S3CompatibleObjectStorageFactory { region, protocol }
    }
}

impl Default for S3CompatibleObjectStorageFactory {
    fn default() -> Self {
        S3CompatibleObjectStorageFactory::new(Region::default(), "s3")
    }
}

impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn protocol(&self) -> String {
        self.protocol.to_string()
    }

    fn resolve(&self, uri: &str) -> crate::StorageResult<std::sync::Arc<dyn crate::Storage>> {
        let storage = S3CompatibleObjectStorage::from_uri(self.region.clone(), uri)?;
        Ok(Arc::new(storage))
    }
}
