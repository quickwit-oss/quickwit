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

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use quickwit_doc_mapper::tokenizers::LogTokenizer;
use tantivy::tokenizer::{SimpleTokenizer, TextAnalyzer};

const LOG_TEST_DATA: &str = include_str!("data/access.log");

pub fn log_tokenizer_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_tokenizer_benchmark");
    group.throughput(Throughput::Bytes(LOG_TEST_DATA.len() as u64));

    group
        .measurement_time(Duration::from_secs(10))
        .bench_function("logs_simple_tokenizer", |b| {
            b.iter(|| {
                let simple = TextAnalyzer::from(SimpleTokenizer);
                let mut simple_stream = simple.token_stream(LOG_TEST_DATA);
                let mut simple_tokenizer_tokens = 0;

                while simple_stream.advance() {
                    simple_tokenizer_tokens += 1;
                }
                assert_ne!(simple_tokenizer_tokens, 0);
            })
        });

    group
        .measurement_time(Duration::from_secs(10))
        .bench_function("logs_log_tokenizer", |b| {
            b.iter(|| {
                let log = TextAnalyzer::from(LogTokenizer);
                let mut log_stream = log.token_stream(LOG_TEST_DATA);
                let mut log_tokenizer_tokens = 0;

                while log_stream.advance() {
                    log_tokenizer_tokens += 1;
                }
                assert_ne!(log_tokenizer_tokens, 0);
            })
        });
}

criterion_group!(benches, log_tokenizer_benchmark);
criterion_main!(benches);
