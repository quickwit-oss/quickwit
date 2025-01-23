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

include!("../codegen/quickwit/quickwit.ingest.router.rs");

impl IngestRequestV2 {
    pub fn num_bytes(&self) -> usize {
        self.subrequests
            .iter()
            .map(|subrequest| subrequest.num_bytes())
            .sum()
    }
}

impl IngestSubrequest {
    pub fn num_bytes(&self) -> usize {
        self.doc_batch
            .as_ref()
            .map(|doc_batch| doc_batch.doc_buffer.len())
            .unwrap_or(0)
    }
}
