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
    pub shard_scale_up_factor: f32,
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
            shard_scale_up_factor: 1.01,
        }
    }
}
