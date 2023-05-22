// Copyright (C) 2023 Quickwit, Inc.
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
use quickwit_common::uri::Uri;
use quickwit_config::{MetastoreBackend, MetastoreConfig};

use crate::{Metastore, MetastoreResolverError};

/// A metastore factory builds a [`Metastore`] object for a target [`MetastoreBackend`] from a
/// [`MetastoreConfig`] and a [`Uri`].
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait MetastoreFactory: Send + Sync + 'static {
    /// Returns the metastore backend targeted by the factory.
    fn backend(&self) -> MetastoreBackend;

    /// Returns the appropriate [`Metastore`] object for the `uri`.
    async fn resolve(
        &self,
        metastore_config: &MetastoreConfig,
        uri: &Uri,
    ) -> Result<Arc<dyn Metastore>, MetastoreResolverError>;
}

/// A metastore factory for handling unsupported or unavailable metastore backends.
#[derive(Clone)]
pub struct UnsupportedMetastore {
    backend: MetastoreBackend,
    message: &'static str,
}

impl UnsupportedMetastore {
    /// Creates a new [`UnsupportedMetastore`].
    pub fn new(backend: MetastoreBackend, message: &'static str) -> Self {
        Self { backend, message }
    }
}

#[async_trait]
impl MetastoreFactory for UnsupportedMetastore {
    fn backend(&self) -> MetastoreBackend {
        self.backend
    }

    async fn resolve(
        &self,
        _metastore_config: &MetastoreConfig,
        _uri: &Uri,
    ) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        Err(MetastoreResolverError::UnsupportedBackend(
            self.message.to_string(),
        ))
    }
}
