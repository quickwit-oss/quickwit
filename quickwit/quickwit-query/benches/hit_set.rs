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
use quickwit_query::query_ast::HitSet;
use tantivy::{BitSet, DocSet, TERMINATED};

fn bench_hit_set(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("hit_set");
    let num_docs = 10_000_000;
    for count in [128, 10_000, 100_000, 1_000_000, 10_000_000] {
        let stride = num_docs / count;
        let mut bitset = BitSet::with_max_value(num_docs);
        for doc in 0..count {
            bitset.insert(doc * stride);
        }
        let hits = HitSet::from_bitset(bitset);
        eprintln!(
            "{count}/{num_docs} hits, stride {stride}: {} bytes",
            hits.clone().into_buffer().len()
        );
        let case = count.to_string();
        group.bench_with_input(BenchmarkId::new("seek", &case), &hits, |bencher, hits| {
            bencher.iter(|| {
                let mut hits = hits.clone();
                black_box(hits.seek(black_box((count - 2) * stride + 1)))
            });
        });
        group.bench_with_input(BenchmarkId::new("scan", &case), &hits, |bencher, hits| {
            bencher.iter(|| {
                let mut hits = hits.clone();
                while hits.doc() != TERMINATED {
                    black_box(hits.doc());
                    hits.advance();
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_hit_set);
criterion_main!(benches);
