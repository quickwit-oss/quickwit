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

use std::hint::black_box;
use std::time::{Duration, Instant};

use binggan::{INSTRUMENTED_SYSTEM, PeakMemAlloc, PeakMemAllocTrait};
use quickwit_doc_mapper::DocMapper;

#[global_allocator]
static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let flat =
        r#"{"id":42,"first":"Alice","last":"Smith","email":"alice@example.com"}"#.to_string();
    let flat_mapping = r#"{"mode":"strict","index_field_presence":false,"field_mappings":[{"name":"id","type":"u64"},{"name":"first","type":"text"},{"name":"last","type":"text"},{"name":"email","type":"text"}]}"#;
    let json_mapping = r#"{"mode":"dynamic","store_source":true,"index_field_presence":false,"field_mappings":[{"name":"json","type":"json"}]}"#;
    let nested = (0..64).map(|index| format!(r#""k{index:03}":{{"z":[1,2,"2024-01-02T03:04:05Z"],"a":"line\nquote\"unicode\u00e9"}}"#)).collect::<Vec<_>>().join(",");
    let json = format!(r#"{{"json":{{{nested}}},"z":"{}"}}"#, "payload".repeat(256));
    let escaped = format!(r#"{{"json":{{"esca\u0070ed":[{{{nested}}}]}},"a":[{{{nested}}}]}}"#);
    let children = (0..128)
        .map(|index| format!(r#", "u{index:03}":{{"value":"{}"}}"#, "unknown".repeat(8)))
        .collect::<String>();
    let mixed = format!(r#"{{"z":{{"known":"ok"{children}}},"root":{{{nested}}}}}"#);
    let mixed_mapping = r#"{"mode":"dynamic","index_field_presence":false,"field_mappings":[{"name":"z","type":"object","field_mappings":[{"name":"known","type":"text"}]}]}"#;
    let array =
        serde_json::json!({"items": vec!["long mapped text ".repeat(16); 1024]}).to_string();
    let array_mapping = r#"{"mode":"strict","index_field_presence":false,"field_mappings":[{"name":"items","type":"array<text>"}]}"#;
    for (name, raw, config) in [
        ("flat", flat, flat_mapping),
        ("source_json", json, json_mapping),
        ("escaped_nested", escaped, json_mapping),
        ("mixed_dynamic", mixed, mixed_mapping),
        ("mapped_array", array, array_mapping),
    ] {
        let mapper: DocMapper = serde_json::from_str(config).unwrap();
        let template: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&raw).unwrap();
        for owned in [true, false] {
            // Preparation and replenishment are outside ALL timed regions. Mapping includes
            // dropping each owned input and the resulting Tantivy document. No pipeline claim.
            let batch_size = if name == "flat" { 512 } else { 16 };
            let mut elapsed = Duration::ZERO;
            let mut count = 0;
            let mut peak = 0;
            for batch_index in 0..128 {
                if owned {
                    let inputs: Vec<_> = (0..batch_size).map(|_| template.clone()).collect();
                    if batch_index == 0 {
                        GLOBAL.reset_peak_memory();
                    }
                    let start = Instant::now();
                    for input in inputs {
                        black_box(
                            mapper
                                .doc_from_json_obj(black_box(input), raw.len() as u64)
                                .unwrap(),
                        );
                    }
                    elapsed += start.elapsed();
                } else {
                    if batch_index == 0 {
                        GLOBAL.reset_peak_memory();
                    }
                    let start = Instant::now();
                    for _ in 0..batch_size {
                        black_box(
                            mapper
                                .doc_from_json_bytes(black_box(raw.as_bytes()))
                                .unwrap(),
                        );
                    }
                    elapsed += start.elapsed();
                }
                if batch_index == 0 {
                    peak = GLOBAL.get_peak_memory();
                }
                count += batch_size;
            }
            println!(
                "{name},{},{count},{},{},{peak}",
                if owned { "owned" } else { "raw" },
                raw.len(),
                elapsed.as_nanos()
            );
        }
    }
}
