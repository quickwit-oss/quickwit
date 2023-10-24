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

mod fetch;
mod ingester;
mod models;
mod mrecord;
mod replication;
mod router;
mod shard_table;
#[cfg(test)]
mod test_utils;

use quickwit_common::tower::Pool;
use quickwit_proto::ingest::ingester::IngesterServiceClient;
use quickwit_proto::types::NodeId;

pub use self::fetch::MultiFetchStream;
pub use self::ingester::Ingester;
pub use self::mrecord::{decoded_mrecords, MRecord};
pub use self::router::IngestRouter;

pub type IngesterPool = Pool<NodeId, IngesterServiceClient>;

/// Identifies an ingester client, typically a source, for logging and debugging purposes.
pub type ClientId = String;

pub type LeaderId = NodeId;

pub type FollowerId = NodeId;
