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

//! [`IngestMetastore`] exposes a small subset of the metastore APIs in a trait defined in this
//! crate instead of depending on the `metastore` crate directly to avoid circular dependencies. It
//! should be removed once we have migrated to the code-generated metastore client. In the meantime,
//! the concrete implementation of this trait lives in the `serve` crate.

use async_trait::async_trait;
use quickwit_proto::ingest::IngestV2Result;
use quickwit_proto::metastore::{
    CloseShardsRequest, CloseShardsResponse, DeleteShardsRequest, DeleteShardsResponse,
};

#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait IngestMetastore: Send + Sync + 'static {
    async fn close_shards(
        &self,
        request: CloseShardsRequest,
    ) -> IngestV2Result<CloseShardsResponse>;

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> IngestV2Result<DeleteShardsResponse>;
}
