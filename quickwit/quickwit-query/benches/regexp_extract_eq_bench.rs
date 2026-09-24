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

//! Criterion benches for `EQ(REGEXP_EXTRACT)` JIT vs FST-prefilter search paths.
//!
//! Builds one in-memory Tantivy index of log-like lines (raw+fast string field) and
//! compares JIT-only vs FST prefilter ∧ JIT for anchored and unanchored extracts.

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use quickwit_query::query_ast::{BoolQuery, BuildTantivyAstContext, CalcFieldQuery, QueryAst};
use tantivy::collector::Count;
use tantivy::jitexpr::ast::deserialize;
use tantivy::query::Query;
use tantivy::schema::{FAST, STRING, Schema};
use tantivy::{Index, IndexReader, Searcher, TantivyDocument, doc};

const NUM_DOCS: u64 = 50_000;
const TARGET_USER_ID: &str = "USER42";
/// Every Nth document carries the target userid (≈1% hit rate).
const TARGET_EVERY: u64 = 100;

/// Whole-line anchored pattern matching the generated log format (single capture).
const ANCHORED_PATTERN: &str =
    r"^2024-01-01T00:00:00Z INFO host=web-[0-9]+ userid=([A-Z0-9]+) path=/api request=[0-9]+$";
const UNANCHORED_PATTERN: &str = r"userid=([A-Z0-9]+)";

struct BenchIndex {
    reader: IndexReader,
    schema: Schema,
}

impl BenchIndex {
    fn searcher(&self) -> Searcher {
        self.reader.searcher()
    }
}

fn calc_field(expression: &str) -> CalcFieldQuery {
    CalcFieldQuery {
        expression: deserialize(expression).unwrap(),
    }
}

fn prefilter_conjunction(calc: CalcFieldQuery, schema: &Schema) -> QueryAst {
    let regex_query = calc
        .try_prefilter_regex_query(schema)
        .expect("expression should be eligible for prefilter");
    BoolQuery {
        filter: vec![regex_query.into(), QueryAst::CalcField(calc)],
        ..Default::default()
    }
    .into()
}

fn build_query(ast: &QueryAst, schema: &Schema) -> Box<dyn Query> {
    let context = BuildTantivyAstContext::for_test(schema);
    ast.build_tantivy_query(&context).unwrap()
}

fn search_count(searcher: &Searcher, query: &dyn Query) -> usize {
    searcher.search(query, &Count).unwrap()
}

fn log_line(host: u64, user_id: &str, request: u64, extra: &[&str]) -> String {
    let mut line = format!(
        "2024-01-01T00:00:00Z INFO host=web-{host} userid={user_id} path=/api request={request}"
    );
    for part in extra {
        line.push(' ');
        line.push_str(part);
    }
    line
}

/// Shared log-like corpus used by every bench in this file.
fn build_log_line_index() -> BenchIndex {
    let mut schema_builder = Schema::builder();
    let message = schema_builder.add_text_field("message", STRING | FAST);
    let index = Index::create_in_ram(schema_builder.build());
    let mut writer = index
        .writer_with_num_threads::<TantivyDocument>(1, 100_000_000)
        .unwrap();

    for i in 0..NUM_DOCS {
        let host = i % 16;
        let value = if i % (TARGET_EVERY * 10) == 1 {
            // Leftmost unanchored extract is OTHER; TARGET appears later.
            log_line(host, "OTHER", i, &["userid=USER42"])
        } else if i % (TARGET_EVERY * 10) == 2 {
            // Longer id: unanchored extract is USER42EXTRA, not TARGET.
            log_line(host, "USER42EXTRA", i, &[])
        } else if i % TARGET_EVERY == 0 {
            log_line(host, TARGET_USER_ID, i, &[])
        } else {
            log_line(host, &format!("U{i:06}"), i, &[])
        };
        writer.add_document(doc!(message => value)).unwrap();
    }
    writer.commit().unwrap();

    BenchIndex {
        reader: index.reader().unwrap(),
        schema: index.schema(),
    }
}

fn eq_extract_expression(pattern: &str) -> String {
    format!(r#"(EQ (REGEXP_EXTRACT message "{pattern}" 1u64) "{TARGET_USER_ID}")"#)
}

fn bench_pair(c: &mut Criterion, group_name: &str, index: &Arc<BenchIndex>, expression: &str) {
    let calc = calc_field(expression);
    let jit_ast: QueryAst = calc.clone().into();
    let optimized = prefilter_conjunction(calc, &index.schema);
    let jit_query = build_query(&jit_ast, &index.schema);
    let opt_query = build_query(&optimized, &index.schema);

    let searcher = index.searcher();
    let jit_hits = search_count(&searcher, &*jit_query);
    let opt_hits = search_count(&searcher, &*opt_query);
    assert_eq!(jit_hits, opt_hits, "prefilter∧JIT must match JIT hits");
    assert!(jit_hits > 0, "bench query must match at least one doc");

    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(NUM_DOCS));
    group.bench_function("jit_only", |b| {
        let searcher = index.searcher();
        b.iter(|| std::hint::black_box(search_count(&searcher, &*jit_query)));
    });
    group.bench_function("prefilter_and_jit", |b| {
        let searcher = index.searcher();
        b.iter(|| std::hint::black_box(search_count(&searcher, &*opt_query)));
    });
    group.finish();
}

fn bench_regexp_extract_eq(c: &mut Criterion) {
    let index = Arc::new(build_log_line_index());

    bench_pair(
        c,
        "regexp_extract_eq_anchored",
        &index,
        &eq_extract_expression(ANCHORED_PATTERN),
    );
    bench_pair(
        c,
        "regexp_extract_eq_unanchored",
        &index,
        &eq_extract_expression(UNANCHORED_PATTERN),
    );
}

criterion_group!(regexp_extract_eq_benches, bench_regexp_extract_eq);
criterion_main!(regexp_extract_eq_benches);
