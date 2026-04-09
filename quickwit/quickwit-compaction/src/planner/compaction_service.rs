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
use quickwit_proto::compaction::{CompactionResult, CompactionService, PingRequest, PingResponse};

#[derive(Debug, Clone)]
pub struct StubCompactionService;

#[async_trait]
impl CompactionService for StubCompactionService {
    async fn ping(&self, _request: PingRequest) -> CompactionResult<PingResponse> {
        Ok(PingResponse {})
    }
}
