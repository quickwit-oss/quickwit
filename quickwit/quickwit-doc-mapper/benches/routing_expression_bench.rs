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

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use quickwit_doc_mapper::{DocMapper, RoutingExpr};
use serde_json::Value as JsonValue;

const JSON_TEST_DATA: &str = include_str!("data/simple-routing-expression-bench.json");

const DOC_MAPPER_CONF: &str = r#"{
    "type": "default",
    "default_search_fields": [],
    "tag_fields": [],
    "field_mappings": [
        {"name": "timestamp", "type": "datetime", "input_formats": ["unix_timestamp"], "output_format": "%Y-%m-%d %H:%M:%S", "output_format": "%Y-%m-%d %H:%M:%S", "fast": true },
        {"name": "source", "type": "text" },
        {"name": "vin", "type": "text" },
        {"name": "vid", "type": "text" },
        {"name": "date", "type": "datetime", "input_formats": ["%Y-%m-%d"], "output_format": "%Y-%m-%d"},
        {"name": "domain", "type": "text" },
        {"name": "seller", "type": "object", "field_mappings": [
            {"name": "id", "type": "text" },
            {"name": "name", "type": "text" },
            {"name": "address", "type": "text" },
            {"name": "zip", "type": "text" }
        ]}
    ],
    "partition_key": "seller.id"
}"#;

pub fn simple_routing_expression_benchmark(c: &mut Criterion) {
    let doc_mapper: Box<DocMapper> = serde_json::from_str(DOC_MAPPER_CONF).unwrap();
    let lines: Vec<&str> = JSON_TEST_DATA.lines().map(|line| line.trim()).collect();

    let json_lines: Vec<serde_json::Map<String, JsonValue>> = lines
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    let mut group = c.benchmark_group("simple-routing-expression");
    group.throughput(Throughput::Bytes(JSON_TEST_DATA.len() as u64));
    group.bench_function("simple-json-to-doc", |b| {
        b.iter(|| {
            for line in &lines {
                doc_mapper.doc_from_json_str(line).unwrap();
            }
        })
    });
    group.bench_function("simple-eval-hash", |b| {
        b.iter(|| {
            let routing_expr = RoutingExpr::new("seller.id").unwrap();
            for json in &json_lines {
                routing_expr.eval_hash(json);
            }
        })
    });
}

criterion_group!(benches, simple_routing_expression_benchmark);
criterion_main!(benches);
