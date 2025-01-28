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

use quickwit_proto::metastore::MetastoreError;

/// Generic Storage Resolver error.
#[derive(Debug, thiserror::Error)]
pub enum MetastoreResolverError {
    /// The metastore config is invalid.
    #[error("invalid metastore config: `{0}`")]
    InvalidConfig(String),

    /// The URI does not contain sufficient information to connect to the metastore.
    #[error("invalid metastore URI: `{0}`")]
    InvalidUri(String),

    /// The requested backend is unsupported or unavailable.
    #[error("unsupported metastore backend: `{0}`")]
    UnsupportedBackend(String),

    /// The config and URI are valid, and are meant to be handled by this resolver, but the
    /// resolver failed to actually connect to the backend. e.g. connection error, credentials
    /// error, incompatible version, internal error in a third party, etc.
    #[error("failed to connect to metastore: `{0}`")]
    Initialization(#[from] MetastoreError),
}
