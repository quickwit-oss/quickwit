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

use quickwit_metastore::CreateIndexRequestExt;
use quickwit_proto::metastore::{CreateIndexRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::IndexUid;

/// Create a metrics index rooted at `data_dir` and return its `IndexUid`.
pub async fn create_metrics_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    data_dir: &std::path::Path,
) -> IndexUid {
    let index_uri = format!("file://{}", data_dir.display());
    let index_config: quickwit_config::IndexConfig = serde_json::from_value(serde_json::json!({
        "version": "0.8",
        "index_id": index_id,
        "index_uri": index_uri,
        "doc_mapping": { "field_mappings": [] },
    }))
    .expect("valid metrics index config");

    let response = metastore
        .clone()
        .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
        .await
        .expect("create_index");
    response.index_uid().clone()
}
