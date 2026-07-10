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
//! Groups:
//! - `mrecord_encode` / `mrecord_decode`: per-record codec microbenchmarks (CPU only).
//! - `mrecord_append_batch`: a batch of documents encoded and appended to a real WAL, mirroring the
//!   write path (`append_non_empty_doc_batch`) at its production granularity.
//! - `mrecord_compress` / `mrecord_decompress`: per-document zstd compression/decompression on
//!   realistic JSON log documents, measuring the router-side and indexer-side CPU overhead of
//!   record compression. The compression ratio for each document size is printed to stderr.
//!
//! Run with: `cargo bench -p quickwit-ingest --bench mrecord_bench`

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mrecordlog::MultiRecordLog;
use quickwit_ingest::MRecord;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Representative document sizes (bytes): a small log line up to a large structured event.
const DOC_SIZES: [usize; 4] = [128, 1_024, 8_192, 65_536];

/// zstd compression level used by the record-compression write path (see `mrecord.rs`). Level 0
/// selects zstd's default level.
const ZSTD_COMPRESSION_LEVEL: i32 = 0;

fn make_doc(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// A small vocabulary of words that recur across log messages, as in real observability data.
const LOG_WORDS: &[&str] = &[
    "error",
    "warning",
    "info",
    "request",
    "response",
    "user",
    "timeout",
    "connection",
    "database",
    "query",
    "failed",
    "retry",
    "latency",
    "service",
    "endpoint",
    "success",
    "cache",
    "missed",
    "processed",
    "shard",
];

/// Builds a pseudo-realistic JSON log document of roughly `target_size` bytes. Field names repeat
/// across documents and the message is drawn from a small vocabulary, so the per-document zstd
/// ratio is representative of observability data rather than the degenerate ratio of a buffer of
/// identical bytes.
fn make_log_doc(target_size: usize, rng: &mut StdRng) -> Bytes {
    let level = ["INFO", "WARN", "ERROR", "DEBUG"][rng.random_range(0..4)];
    let status = rng.random_range(200..600);
    let latency_ms = rng.random_range(0..5_000);
    let mut message = String::with_capacity(target_size);
    while message.len() < target_size {
        message.push_str(LOG_WORDS[rng.random_range(0..LOG_WORDS.len())]);
        message.push(' ');
    }
    let doc = format!(
        "{{\"timestamp\":\"2026-07-10T12:34:56.{:06}Z\",\"level\":\"{level}\",\"status\":{status},\
         \"latency_ms\":{latency_ms},\"message\":\"{}\"}}",
        rng.random_range(0..1_000_000),
        message.trim_end()
    );
    Bytes::from(doc)
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

/// Measures the router-side CPU cost of compressing a single document with zstd, and prints the
/// achieved compression ratio for each document size.
fn bench_compress(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mrecord_compress");
    let mut rng = StdRng::seed_from_u64(42);

    for doc_size in DOC_SIZES {
        let doc = make_log_doc(doc_size, &mut rng);
        group.throughput(Throughput::Bytes(doc.len() as u64));

        let compressed_doc =
            zstd::encode_all(&doc[..], ZSTD_COMPRESSION_LEVEL).expect("compression should succeed");
        eprintln!(
            "compression ratio: target_size={doc_size} uncompressed={} compressed={} ratio={:.2}x",
            doc.len(),
            compressed_doc.len(),
            doc.len() as f64 / compressed_doc.len() as f64
        );

        group.bench_with_input(BenchmarkId::new("zstd", doc_size), &doc, |bencher, doc| {
            bencher.iter(|| {
                black_box(
                    zstd::encode_all(&doc[..], ZSTD_COMPRESSION_LEVEL)
                        .expect("compression should succeed"),
                )
            });
        });
    }
    group.finish();
}

/// Measures the indexer-side CPU cost of decompressing a single zstd-compressed document.
fn bench_decompress(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mrecord_decompress");
    let mut rng = StdRng::seed_from_u64(42);

    for doc_size in DOC_SIZES {
        let doc = make_log_doc(doc_size, &mut rng);
        let compressed_doc =
            zstd::encode_all(&doc[..], ZSTD_COMPRESSION_LEVEL).expect("compression should succeed");
        // Throughput is reported against the uncompressed size (the useful bytes produced).
        group.throughput(Throughput::Bytes(doc.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("zstd", doc_size),
            &compressed_doc,
            |bencher, compressed_doc| {
                bencher.iter(|| {
                    black_box(
                        zstd::decode_all(&compressed_doc[..]).expect("decoding should succeed"),
                    )
                });
            },
        );
    }
    group.finish();
}

/// Number of documents used to train the zstd dictionary (modelling the sample a single shard or
/// source would collect).
const DICT_TRAINING_DOCS: usize = 4_096;
/// Number of held-out documents used to measure the ratio, so the dictionary is not evaluated on
/// the very documents it was trained on.
const DICT_EVAL_DOCS: usize = 1_024;
/// Trained dictionary size cap. 16 KiB is a typical size for small-document dictionaries.
const DICT_MAX_SIZE: usize = 16 * 1_024;
/// Document sizes (bytes) at which to compare no-dictionary vs dictionary compression. Small
/// documents are where per-document compression without a dictionary is weakest.
const DICT_DOC_SIZES: [usize; 3] = [128, 256, 1_024];

/// Builds a corpus of `num_docs` realistic JSON log documents of roughly `target_size` bytes.
fn make_corpus(num_docs: usize, target_size: usize, rng: &mut StdRng) -> Vec<Bytes> {
    (0..num_docs)
        .map(|_| make_log_doc(target_size, rng))
        .collect()
}

/// Aggregate compression ratio (sum of uncompressed sizes / sum of compressed sizes) achieved by
/// `compress` over `docs`.
fn aggregate_ratio(docs: &[Bytes], mut compress: impl FnMut(&[u8]) -> Vec<u8>) -> f64 {
    let uncompressed: usize = docs.iter().map(|doc| doc.len()).sum();
    let compressed: usize = docs.iter().map(|doc| compress(&doc[..]).len()).sum();
    uncompressed as f64 / compressed as f64
}

/// Assesses whether a per-shard zstd dictionary is worth adding. For each small document size it
/// trains a dictionary on one set of documents and prints the compression ratio, over a held-out
/// set, with and without the dictionary. It also benchmarks the CPU cost of dictionary compression
/// and decompression against the dictionary-less path.
fn bench_dictionary(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mrecord_dictionary");
    let mut rng = StdRng::seed_from_u64(7);

    for doc_size in DICT_DOC_SIZES {
        let training_docs = make_corpus(DICT_TRAINING_DOCS, doc_size, &mut rng);
        let eval_docs = make_corpus(DICT_EVAL_DOCS, doc_size, &mut rng);

        let dictionary = zstd::dict::from_samples(&training_docs, DICT_MAX_SIZE)
            .expect("dictionary training should succeed");

        let no_dict_ratio = aggregate_ratio(&eval_docs, |doc| {
            zstd::encode_all(doc, ZSTD_COMPRESSION_LEVEL).expect("compression should succeed")
        });
        let dict_ratio = {
            let mut compressor =
                zstd::bulk::Compressor::with_dictionary(ZSTD_COMPRESSION_LEVEL, &dictionary)
                    .expect("dictionary compressor should build");
            aggregate_ratio(&eval_docs, |doc| {
                compressor
                    .compress(doc)
                    .expect("compression should succeed")
            })
        };
        eprintln!(
            "dictionary assessment: doc_size={doc_size} dict_size={} \
             no_dict_ratio={no_dict_ratio:.2}x dict_ratio={dict_ratio:.2}x improvement={:.2}x",
            dictionary.len(),
            dict_ratio / no_dict_ratio,
        );

        let doc = eval_docs[0].clone();
        group.throughput(Throughput::Bytes(doc.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("compress_no_dict", doc_size),
            &doc,
            |bencher, doc| {
                bencher.iter(|| {
                    black_box(
                        zstd::encode_all(&doc[..], ZSTD_COMPRESSION_LEVEL)
                            .expect("compression should succeed"),
                    )
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("compress_dict", doc_size),
            &doc,
            |bencher, doc| {
                let mut compressor =
                    zstd::bulk::Compressor::with_dictionary(ZSTD_COMPRESSION_LEVEL, &dictionary)
                        .expect("dictionary compressor should build");
                bencher.iter(|| black_box(compressor.compress(&doc[..]).expect("compression")));
            },
        );

        let compressed_doc = {
            let mut compressor =
                zstd::bulk::Compressor::with_dictionary(ZSTD_COMPRESSION_LEVEL, &dictionary)
                    .expect("dictionary compressor should build");
            compressor.compress(&doc[..]).expect("compression")
        };
        group.bench_with_input(
            BenchmarkId::new("decompress_dict", doc_size),
            &compressed_doc,
            |bencher, compressed_doc| {
                let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dictionary)
                    .expect("dictionary decompressor should build");
                bencher.iter(|| {
                    black_box(
                        decompressor
                            .decompress(&compressed_doc[..], doc_size * 4)
                            .expect("decoding should succeed"),
                    )
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_append_batch,
    bench_compress,
    bench_decompress,
    bench_dictionary
);
criterion_main!(benches);
