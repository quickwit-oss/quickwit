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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_storage::{quickwit_storage_uri_resolver, StorageResolverError, StorageUriResolver};

use crate::{
    Metastore, MetastoreError, MetastoreFactory, MetastoreResolverError, SingleFileMetastore,
};

/// A single file metastore factory
#[derive(Clone)]
pub struct SingleFileMetastoreFactory {
    storage_uri_resolver: StorageUriResolver,
}

impl Default for SingleFileMetastoreFactory {
    fn default() -> Self {
        SingleFileMetastoreFactory {
            storage_uri_resolver: quickwit_storage_uri_resolver().clone(),
        }
    }
}

#[async_trait]
impl MetastoreFactory for SingleFileMetastoreFactory {
    async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let storage = self
            .storage_uri_resolver
            .resolve(uri)
            .map_err(|err| match err {
                StorageResolverError::InvalidUri { message } => {
                    MetastoreResolverError::InvalidUri(message)
                }
                StorageResolverError::ProtocolUnsupported { protocol } => {
                    MetastoreResolverError::ProtocolUnsupported(protocol)
                }
                StorageResolverError::FailedToOpenStorage { kind, message } => {
                    MetastoreResolverError::FailedToOpenMetastore(MetastoreError::InternalError {
                        message: format!("Failed to open metastore file `{}`.", uri),
                        cause: anyhow::anyhow!("StorageError {:?}: {}.", kind, message),
                    })
                }
            })?;

        Ok(Arc::new(SingleFileMetastore::new(storage)))
    }
}
