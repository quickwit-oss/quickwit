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

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use quickwit_config::DocsClusteringConfig;
use quickwit_indexing::docs_clustering::{DocIdClusterer, Fingerprint, Fingerprinter};
use serde_json::{Value as JsonValue, json};
use tantivy::DocId;

const NUM_DOCS: usize = 100_000;

fn docs_clustering_config(num_levels: usize) -> DocsClusteringConfig {
    let mut policies = vec![json!({
        "fingerprint": [
            {
                "kind": "structure",
                "exclude": ["message", "timestamp"]
            }
        ]
    })];
    if num_levels >= 2 {
        policies.push(json!({
            "fingerprint": [
                {
                    "kind": "raw",
                    "path": "service"
                }
            ]
        }));
    }
    if num_levels >= 3 {
        policies.push(json!({
            "fingerprint": [
                {
                    "kind": "tokenized",
                    "path": "message"
                }
            ]
        }));
    }
    if num_levels >= 4 {
        policies.push(json!({
            "fingerprint": [
                {
                    "kind": "raw",
                    "path": "status"
                }
            ]
        }));
    }

    let config: DocsClusteringConfig = serde_json::from_value(JsonValue::Array(policies)).unwrap();
    config.validate().unwrap();
    config
}

fn generate_fingerprints(
    num_docs: usize,
    num_levels: usize,
    cardinality: usize,
) -> Vec<Fingerprint> {
    let config = docs_clustering_config(num_levels);
    let fingerprinter = Fingerprinter::new(&config);
    (0..num_docs)
        .map(|doc_idx| {
            let service_id = doc_idx % cardinality;
            let template_id = (doc_idx / cardinality) % 128;
            let status = match doc_idx % 5 {
                0 => "debug",
                1 => "info",
                2 => "warn",
                3 => "error",
                _ => "critical",
            };
            let doc = json!({
                "timestamp": doc_idx,
                "service": format!("service-{service_id}"),
                "message": format!("template {template_id} request {} completed in {} ms", doc_idx, doc_idx % 1_000),
                "status": status,
                "host": format!("host-{}", doc_idx % 1_024),
            });
            fingerprinter.fingerprint(&doc)
        })
        .collect()
}

fn is_unsorted_doc(doc_idx: usize, unsorted_doc_frequency_opt: Option<usize>) -> bool {
    match unsorted_doc_frequency_opt {
        Some(frequency) => doc_idx % frequency == 0,
        None => false,
    }
}

fn bench_doc_id_mapping(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    fingerprints: &[Fingerprint],
    unsorted_doc_frequency_opt: Option<usize>,
) {
    group.throughput(criterion::Throughput::Elements(fingerprints.len() as u64));
    group.bench_function(BenchmarkId::new(name, fingerprints.len()), |b| {
        b.iter(|| {
            let mut clusterer = DocIdClusterer::default();
            for (doc_idx, fingerprint) in fingerprints.iter().enumerate() {
                let fingerprint_opt = if is_unsorted_doc(doc_idx, unsorted_doc_frequency_opt) {
                    None
                } else {
                    Some(fingerprint.clone())
                };
                clusterer.push(fingerprint_opt, doc_idx as DocId);
            }
            black_box(
                clusterer
                    .into_doc_id_mapping(fingerprints.len() as u64)
                    .unwrap(),
            )
        })
    });
}

fn bench_doc_id_clusterer(c: &mut Criterion) {
    let low_cardinality_fingerprints = generate_fingerprints(NUM_DOCS, 2, 16);
    let high_cardinality_fingerprints = generate_fingerprints(NUM_DOCS, 2, 16_384);
    let deep_fingerprints = generate_fingerprints(NUM_DOCS, 4, 256);

    let mut group = c.benchmark_group("DocIdClusterer");
    bench_doc_id_mapping(
        &mut group,
        "two-level/low-cardinality",
        &low_cardinality_fingerprints,
        None,
    );
    bench_doc_id_mapping(
        &mut group,
        "two-level/high-cardinality",
        &high_cardinality_fingerprints,
        None,
    );
    bench_doc_id_mapping(
        &mut group,
        "four-level/mixed-cardinality",
        &deep_fingerprints,
        None,
    );
    bench_doc_id_mapping(
        &mut group,
        "four-level/with-unsorted-docs",
        &deep_fingerprints,
        Some(10),
    );
    group.finish();
}

criterion_group!(benches, bench_doc_id_clusterer);
criterion_main!(benches);
