// Copyright (C) 2022 Quickwit, Inc.
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

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use quickwit_doc_mapper::DocMapper;

const JSON_TEST_DATA: &str = include_str!("data/simple-parse-bench.json");

const DOC_MAPPER_CONF: &str = r#"{
    "type": "default",
    "default_search_fields": [],
    "tag_fields": [],
    "field_mappings": [
        {"name": "id", "type": "u64", "fast": false },
        {"name": "first_name", "type": "text" },
        {"name": "last_name", "type": "text" },
        {"name": "email", "type": "text" }
    ]
}"#;

pub fn simple_json_to_doc_benchmark(c: &mut Criterion) {
    let doc_mapper: Box<dyn DocMapper> = serde_json::from_str(DOC_MAPPER_CONF).unwrap();
    let lines: Vec<&str> = JSON_TEST_DATA.lines().map(|line| line.trim()).collect();

    let mut group = c.benchmark_group("simple-json-to-doc");
    group.throughput(Throughput::Bytes(JSON_TEST_DATA.len() as u64));
    group.bench_function("simple-json-to-doc-donothing", |b| {
        b.iter(|| {
            let _lines: Vec<String> = lines.iter().map(|line| line.to_string()).collect();
        })
    });
    group.bench_function("simple-json-to-doc", |b| {
        b.iter(|| {
            let lines: Vec<String> = lines.iter().map(|line| line.to_string()).collect();
            for line in lines {
                doc_mapper.doc_from_json(line).unwrap();
            }
        })
    });
    group.bench_function("simple-json-to-doc-tantivy", |b| {
        b.iter(|| {
            let lines: Vec<String> = lines.iter().map(|line| line.to_string()).collect();
            let schema = doc_mapper.schema();
            for line in lines {
                let _doc = schema.parse_document(&line).unwrap();
            }
        })
    });
}

criterion_group!(benches, simple_json_to_doc_benchmark);
criterion_main!(benches);
