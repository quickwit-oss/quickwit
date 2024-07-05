// Copyright (C) 2024 Quickwit, Inc.
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

use bytesize::ByteSize;
use quickwit_common::uri::Uri;

/// An embryo of a cluster config.
// TODO: Move to `quickwit-config` and version object.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub auto_create_indexes: bool,
    pub default_index_root_uri: Uri,
    pub replication_factor: usize,
    pub shard_throughput_limit: ByteSize,
}

impl ClusterConfig {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        ClusterConfig {
            cluster_id: "test-cluster".to_string(),
            auto_create_indexes: false,
            default_index_root_uri: Uri::for_test("ram:///indexes"),
            replication_factor: 1,
            shard_throughput_limit: quickwit_common::shared_consts::DEFAULT_SHARD_THROUGHPUT_LIMIT,
        }
    }
}
