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

//! Before/after benchmark for the `MRecord` on-disk format: the legacy v0 raw encoding
//! (`MRecord::encode`) versus the extensible v1 protobuf encoding (`MRecord::encode_v1`).
//!
//! Three groups:
//! - `mrecord_encode` / `mrecord_decode`: per-record codec microbenchmarks (CPU only).
//! - `mrecord_append_batch`: a batch of documents encoded and appended to a real WAL, mirroring the
//!   write path (`append_non_empty_doc_batch`) at its production granularity.
//!
//! Run with: `cargo bench -p quickwit-ingest --bench mrecord_bench`

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mrecordlog::MultiRecordLog;
use quickwit_ingest::MRecord;

/// Representative document sizes (bytes): a small log line up to a large structured event.
const DOC_SIZES: [usize; 4] = [128, 1_024, 8_192, 65_536];

fn make_doc(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Encodes a `Doc` and drains the result into a reused buffer, modelling the cost of producing the
/// bytes handed to the WAL plus the single copy the WAL performs on append. This is the fair
/// apples-to-apples comparison: v0 builds a header+doc chain that is copied once into the sink,
/// whereas v1 allocates and copies into its own buffer first and is then copied into the sink.
fn bench_encode(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mrecord_encode");
    let mut sink: Vec<u8> = Vec::with_capacity(128 * 1_024);

    for doc_size in DOC_SIZES {
        let doc = make_doc(doc_size);
        group.throughput(Throughput::Bytes(doc_size as u64));

        group.bench_with_input(BenchmarkId::new("v0", doc_size), &doc, |bencher, doc| {
            bencher.iter(|| {
                let mut encoded = MRecord::Doc(doc.clone()).encode();
                sink.clear();
                sink.put(&mut encoded);
                black_box(&sink);
            });
        });

        group.bench_with_input(BenchmarkId::new("v1", doc_size), &doc, |bencher, doc| {
            bencher.iter(|| {
                let encoded = MRecord::Doc(doc.clone()).encode_v1();
                sink.clear();
                sink.put_slice(&encoded);
                black_box(&sink);
            });
        });
    }
    group.finish();
}

/// Decodes a previously encoded `Doc`. Both formats extract the document zero-copy from a `Bytes`
/// buffer, so this guards against a decode regression in the new format.
fn bench_decode(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mrecord_decode");

    for doc_size in DOC_SIZES {
        let doc = make_doc(doc_size);
        group.throughput(Throughput::Bytes(doc_size as u64));

        let mut v0_buf: Vec<u8> = Vec::new();
        v0_buf.put(MRecord::Doc(doc.clone()).encode());
        let v0_bytes = Bytes::from(v0_buf);
        group.bench_with_input(
            BenchmarkId::new("v0", doc_size),
            &v0_bytes,
            |bencher, bytes| {
                bencher.iter(|| black_box(MRecord::decode(bytes.clone())));
            },
        );

        let v1_bytes = MRecord::Doc(doc.clone()).encode_v1();
        group.bench_with_input(
            BenchmarkId::new("v1", doc_size),
            &v1_bytes,
            |bencher, bytes| {
                bencher.iter(|| black_box(MRecord::decode(bytes.clone())));
            },
        );
    }
    group.finish();
}

const BENCH_QUEUE: &str = "bench-queue";

/// Number of documents per appended batch and their (cycled) sizes in bytes, chosen to resemble a
/// mixed observability workload rather than a single uniform size.
const BATCH_NUM_DOCS: usize = 1_000;
const BATCH_DOC_SIZES: [usize; 5] = [100, 250, 500, 1_000, 2_000];

/// Builds the batch of documents and returns it alongside the total payload size in bytes (used for
/// throughput).
fn make_batch() -> (Vec<Bytes>, u64) {
    let mut docs = Vec::with_capacity(BATCH_NUM_DOCS);
    let mut total_bytes = 0u64;
    for index in 0..BATCH_NUM_DOCS {
        let doc_size = BATCH_DOC_SIZES[index % BATCH_DOC_SIZES.len()];
        total_bytes += doc_size as u64;
        docs.push(make_doc(doc_size));
    }
    (docs, total_bytes)
}

/// Opens a fresh WAL backed by a temporary directory, using the same persist policy as production
/// (`PersistPolicy::Always(PersistAction::Flush)`, the default of `MultiRecordLog::open`). The
/// returned `TempDir` must be kept alive for the lifetime of the WAL.
fn open_bench_wal() -> (tempfile::TempDir, MultiRecordLog) {
    let tempdir = tempfile::tempdir().expect("failed to create temp dir");
    let mut wal = MultiRecordLog::open(tempdir.path()).expect("failed to open WAL");
    wal.create_queue(BENCH_QUEUE)
        .expect("failed to create queue");
    (tempdir, wal)
}

/// Encodes a batch of documents and appends it to a real WAL, mirroring the write path. Only the
/// encode + append is timed; opening the WAL and tearing it down happen outside the measured region
/// (which also avoids accumulating open files or disk usage across iterations).
fn bench_append_batch(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mrecord_append_batch");
    let (docs, total_bytes) = make_batch();
    group.throughput(Throughput::Bytes(total_bytes));
    // The WAL is re-created (untimed) on every iteration, so keep the iteration count modest.
    group.sample_size(20);

    group.bench_function("v0", |bencher| {
        bencher.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let (_tempdir, mut wal) = open_bench_wal();
                let start = Instant::now();
                let encoded_mrecords = docs.iter().map(|doc| MRecord::Doc(doc.clone()).encode());
                let outcome = wal
                    .append_records(BENCH_QUEUE, None, encoded_mrecords)
                    .expect("failed to append records");
                elapsed += start.elapsed();
                black_box(outcome);
            }
            elapsed
        });
    });

    group.bench_function("v1", |bencher| {
        bencher.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let (_tempdir, mut wal) = open_bench_wal();
                let start = Instant::now();
                let encoded_mrecords = docs.iter().map(|doc| MRecord::Doc(doc.clone()).encode_v1());
                let outcome = wal
                    .append_records(BENCH_QUEUE, None, encoded_mrecords)
                    .expect("failed to append records");
                elapsed += start.elapsed();
                black_box(outcome);
            }
            elapsed
        });
    });

    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode, bench_append_batch);
criterion_main!(benches);
