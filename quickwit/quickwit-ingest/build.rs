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

use quickwit_codegen::{Codegen, ProstConfig};

fn main() {
    // Legacy ingest codegen
    let mut prost_config = ProstConfig::default();
    prost_config.bytes(["DocBatch.doc_buffer"]);

    Codegen::builder()
        .with_protos(&["src/ingest_service.proto"])
        .with_output_dir("src/codegen/")
        .with_result_type_path("crate::Result")
        .with_error_type_path("crate::IngestServiceError")
        .with_prost_config(prost_config)
        .generate_rpc_name_impls()
        .run()
        .unwrap();
}
