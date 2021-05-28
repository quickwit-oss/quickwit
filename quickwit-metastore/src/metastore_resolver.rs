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

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use crate::metastore::single_file_metastore::SingleFileMetastoreFactory;
use crate::{Metastore, MetastoreResolverError};

/// A metastore factory builds a [`Metastore`] object from an URI.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait MetastoreFactory: Send + Sync + 'static {
    /// Returns the protocol this URI resolver is serving.
    fn protocol(&self) -> String;
    /// Given an URI, returns a [`Metastore`] object.
    async fn resolve(&self, uri: String) -> Result<Arc<dyn Metastore>, MetastoreResolverError>;
}

/// Resolves an URI by dispatching it to the right [`MetastoreFactory`]
/// based on its protocol.
pub struct MetastoreUriResolver {
    per_protocol_resolver: HashMap<String, Arc<dyn MetastoreFactory>>,
}

impl Default for MetastoreUriResolver {
    fn default() -> Self {
        let mut resolver = MetastoreUriResolver {
            per_protocol_resolver: Default::default(),
        };
        resolver.register(SingleFileMetastoreFactory::default());
        resolver
    }
}

impl MetastoreUriResolver {
    /// Registers a resolver.
    ///
    /// If a previous resolver was registered for this protocol, it is discarded
    /// and replaced with the new one.
    pub fn register<S: MetastoreFactory>(&mut self, resolver: S) {
        self.per_protocol_resolver
            .insert(resolver.protocol(), Arc::new(resolver));
    }

    /// Resolves the given URI.
    pub async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let protocol = uri.split("://").next().ok_or_else(|| {
            MetastoreResolverError::InvalidUri(format!(
                "Protocol not found in metastore uri: {}",
                uri
            ))
        })?;
        let resolver = self
            .per_protocol_resolver
            .get(protocol)
            .ok_or_else(|| MetastoreResolverError::ProtocolUnsupported(protocol.to_string()))?;
        let metastore = resolver.resolve(uri.to_string()).await?;
        Ok(metastore)
    }
}
