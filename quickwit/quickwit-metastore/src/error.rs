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
