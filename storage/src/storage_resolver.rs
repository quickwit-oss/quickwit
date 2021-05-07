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
use std::sync::{Arc, RwLock};

use rusoto_core::Region;

use crate::{RamStorage, S3CompatibleObjectStorage, Storage, StorageFromURIError};

pub trait StorageURIResolver: Send + Sync + 'static {
    fn resolve(&self, uri: &str) -> Result<Arc<dyn Storage>, StorageFromURIError>;
}

pub struct DefaultStorageURIResolver {
    region: Region,
}

impl DefaultStorageURIResolver {
    pub fn new(region: Region) -> Self {
        DefaultStorageURIResolver { region }
    }
}

impl StorageURIResolver for DefaultStorageURIResolver {
    fn resolve(&self, uri: &str) -> Result<Arc<dyn Storage>, StorageFromURIError> {
        if uri.starts_with("s3://") {
            let s3_storage = S3CompatibleObjectStorage::from_uri(self.region.clone(), uri)?;
            Ok(Arc::new(s3_storage))
        } else {
            Err(StorageFromURIError::InvalidURI(uri.to_string()))
        }
        // NOTE we could implement a file:// resolver here.
    }
}

pub struct RamStorageURIResolver {
    map: RwLock<HashMap<String, RamStorage>>,
}

impl StorageURIResolver for RamStorageURIResolver {
    fn resolve(&self, uri: &str) -> Result<Arc<dyn Storage>, StorageFromURIError> {
        let mut wlock = self.map.write().map_err(|_| {
            StorageFromURIError::Other(anyhow::anyhow!("Lock in InRAMResolver is poisoned"))
        })?;
        let storage = wlock
            .entry(uri.to_string())
            .or_insert_with(RamStorage::default)
            .clone();
        Ok(Arc::new(storage))
    }
}
