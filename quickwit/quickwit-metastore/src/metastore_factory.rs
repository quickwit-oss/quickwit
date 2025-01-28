// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{MetastoreBackend, MetastoreConfig};
use quickwit_proto::metastore::MetastoreServiceClient;

use crate::MetastoreResolverError;

/// A metastore factory builds a [`MetastoreServiceClient`] object for a target [`MetastoreBackend`]
/// from a [`MetastoreConfig`] and a [`Uri`].
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait MetastoreFactory: Send + Sync + 'static {
    /// Returns the metastore backend targeted by the factory.
    fn backend(&self) -> MetastoreBackend;

    /// Returns the appropriate [`MetastoreServiceClient`] object for the `uri`.
    async fn resolve(
        &self,
        metastore_config: &MetastoreConfig,
        uri: &Uri,
    ) -> Result<MetastoreServiceClient, MetastoreResolverError>;
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
    ) -> Result<MetastoreServiceClient, MetastoreResolverError> {
        Err(MetastoreResolverError::UnsupportedBackend(
            self.message.to_string(),
        ))
    }
}
