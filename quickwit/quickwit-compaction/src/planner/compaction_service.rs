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
use quickwit_indexing::merge_policy::MergeOperation;
use quickwit_proto::compaction::{
    CompactionPlannerService, CompactionResult, MergeTaskAssignment, PingRequest, PingResponse,
    ReportStatusRequest, ReportStatusResponse,
};
use quickwit_proto::types::{IndexUid, SourceId};

use super::index_config_store::IndexEntry;

#[derive(Debug, Clone)]
pub struct StubCompactionPlannerService;

#[async_trait]
impl CompactionPlannerService for StubCompactionPlannerService {
    async fn ping(&self, _request: PingRequest) -> CompactionResult<PingResponse> {
        Ok(PingResponse {})
    }

    async fn report_status(
        &self,
        _request: ReportStatusRequest,
    ) -> CompactionResult<ReportStatusResponse> {
        Ok(ReportStatusResponse {
            new_tasks: Vec::new(),
        })
    }
}

pub fn build_task_assignment(
    task_id: &str,
    index_entry: &IndexEntry,
    operation: &MergeOperation,
    index_uid: &IndexUid,
    source_id: &SourceId,
) -> MergeTaskAssignment {
    MergeTaskAssignment {
        task_id: task_id.to_string(),
        splits_metadata_json: operation
            .splits_as_slice()
            .iter()
            .map(|s| {
                serde_json::to_string(s).expect("split metadata serialization should not fail")
            })
            .collect(),
        doc_mapping_json: index_entry.doc_mapping_json(),
        search_settings_json: index_entry.search_settings_json(),
        indexing_settings_json: index_entry.indexing_settings_json(),
        retention_policy_json: index_entry.retention_policy_json(),
        index_uid: Some(index_uid.clone()),
        source_id: source_id.to_string(),
        index_storage_uri: index_entry.index_storage_uri(),
    }
}
