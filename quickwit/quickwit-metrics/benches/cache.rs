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

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use criterion::{Criterion, criterion_group, criterion_main};
use quickwit_metrics::*;

// ---------------------------------------------------------------------------
// Recorders
// ---------------------------------------------------------------------------

struct NoopRecorder;

impl metrics::Recorder for NoopRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _desc: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _desc: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _desc: metrics::SharedString,
    ) {
    }
    fn register_counter(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        metrics::Counter::noop()
    }
    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

static INSTALL_RECORDER: OnceLock<()> = OnceLock::new();

fn install_recorder() {
    INSTALL_RECORDER.get_or_init(|| {
        eprintln!("[bench/cache] Using noop recorder");
        metrics::set_global_recorder(NoopRecorder).expect("failed to install noop recorder");
    });
}

// ---------------------------------------------------------------------------
// Shared parent counter used by both benchmark groups.
// ---------------------------------------------------------------------------

static DYN_PARENT_COUNTER: LazyCounter = lazy_counter!(
        name: "cache_dyn_parent_counter",
        description: "bench dynamic parent counter for cache benches",
        subsystem: "bench",
        "service" => "api"
);

// ---------------------------------------------------------------------------
// KEY HASH
// ---------------------------------------------------------------------------

fn key_hash_bench(c: &mut Criterion) {
    use std::hint::black_box;

    install_recorder();
    let _ = &*DYN_PARENT_COUNTER;
    let parent_hash = DYN_PARENT_COUNTER.__hash();

    let mut group = c.benchmark_group("cache/key_hash");

    group.bench_function("1_label", |b| {
        b.iter(|| {
            black_box(quickwit_metrics::__key_hash(
                black_box(parent_hash),
                black_box([("method", "GET")]),
            ))
        });
    });

    group.bench_function("3_labels", |b| {
        b.iter(|| {
            black_box(quickwit_metrics::__key_hash(
                black_box(parent_hash),
                black_box([
                    ("method", "GET"),
                    ("endpoint", "/api/v1/search"),
                    ("status", "200"),
                ]),
            ))
        });
    });

    group.bench_function("5_labels", |b| {
        b.iter(|| {
            black_box(quickwit_metrics::__key_hash(
                black_box(parent_hash),
                black_box([
                    ("method", "GET"),
                    ("endpoint", "/api/v1/search"),
                    ("status", "200"),
                    ("region", "us-east"),
                    ("az", "us-east-1a"),
                ]),
            ))
        });
    });

    group.bench_function("10_labels", |b| {
        b.iter(|| {
            black_box(quickwit_metrics::__key_hash(
                black_box(parent_hash),
                black_box([
                    ("method", "GET"),
                    ("endpoint", "/api/v1/search"),
                    ("status", "200"),
                    ("region", "us-east"),
                    ("az", "us-east-1a"),
                    ("version", "v2"),
                    ("instance", "i-1234"),
                    ("cluster", "main"),
                    ("env", "prod"),
                    ("tenant", "acme"),
                ]),
            ))
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// THREAD_LOCAL REFCELL
// ---------------------------------------------------------------------------

fn thread_local_refcell_bench(c: &mut Criterion) {
    use std::cell::RefCell;

    install_recorder();
    let _ = &*DYN_PARENT_COUNTER;

    let mut group = c.benchmark_group("cache/thread_local_refcell");

    group.bench_function("hit", |b| {
        thread_local! {
            static CACHE: RefCell<Option<(u64, Counter)>> = const { RefCell::new(None) };
        }
        CACHE.with(|cell| {
            let counter = DYN_PARENT_COUNTER.clone();
            *cell.borrow_mut() = Some((42, counter));
        });
        b.iter(|| {
            CACHE.with(|cell| {
                let slot = cell.borrow();
                if let Some((h, ref cached)) = *slot
                    && h == 42
                {
                    return cached.clone();
                }
                unreachable!()
            })
        });
    });

    group.bench_function("hit_no_clone", |b| {
        thread_local! {
            static CACHE: RefCell<Option<(u64, Counter)>> = const { RefCell::new(None) };
        }
        CACHE.with(|cell| {
            let counter = DYN_PARENT_COUNTER.clone();
            *cell.borrow_mut() = Some((42, counter));
        });
        b.iter(|| {
            CACHE.with(|cell| {
                let slot = cell.borrow();
                if let Some((h, ref cached)) = *slot
                    && h == 42
                {
                    cached.increment(1);
                    return;
                }
                unreachable!()
            })
        });
    });

    group.bench_function("hit_child_clone", |b| {
        thread_local! {
            static CACHE: RefCell<Option<(u64, Counter)>> = const { RefCell::new(None) };
        }
        CACHE.with(|cell| {
            let child = counter!(parent: DYN_PARENT_COUNTER, "method" => "GET");
            *cell.borrow_mut() = Some((42, child));
        });
        b.iter(|| {
            CACHE.with(|cell| {
                let slot = cell.borrow();
                if let Some((h, ref cached)) = *slot
                    && h == 42
                {
                    return cached.clone();
                }
                unreachable!()
            })
        });
    });

    group.bench_function("hit_child_no_clone", |b| {
        thread_local! {
            static CACHE: RefCell<Option<(u64, Counter)>> = const { RefCell::new(None) };
        }
        CACHE.with(|cell| {
            let child = counter!(parent: DYN_PARENT_COUNTER, "method" => "GET");
            *cell.borrow_mut() = Some((42, child));
        });
        b.iter(|| {
            CACHE.with(|cell| {
                let slot = cell.borrow();
                if let Some((h, ref cached)) = *slot
                    && h == 42
                {
                    cached.increment(1);
                    return;
                }
                unreachable!()
            })
        });
    });

    group.bench_function("hit_child_arc_clone", |b| {
        thread_local! {
            static CACHE: RefCell<Option<(u64, Arc<Counter>)>> = const { RefCell::new(None) };
        }
        CACHE.with(|cell| {
            let child = counter!(parent: DYN_PARENT_COUNTER, "method" => "GET");
            *cell.borrow_mut() = Some((42, Arc::new(child)));
        });
        b.iter(|| {
            CACHE.with(|cell| {
                let slot = cell.borrow();
                if let Some((h, ref cached)) = *slot
                    && h == 42
                {
                    return Arc::clone(cached);
                }
                unreachable!()
            })
        });
    });

    group.bench_function("hit_child_arc_clone_increment", |b| {
        thread_local! {
            static CACHE: RefCell<Option<(u64, Arc<Counter>)>> = const { RefCell::new(None) };
        }
        CACHE.with(|cell| {
            let child = counter!(parent: DYN_PARENT_COUNTER, "method" => "GET");
            *cell.borrow_mut() = Some((42, Arc::new(child)));
        });
        b.iter(|| {
            let c = CACHE.with(|cell| {
                let slot = cell.borrow();
                if let Some((h, ref cached)) = *slot
                    && h == 42
                {
                    return Arc::clone(cached);
                }
                unreachable!()
            });
            c.increment(1);
        });
    });

    group.bench_function("hit_child_closure_increment", |b| {
        thread_local! {
            static CACHE: RefCell<Option<(u64, Counter)>> = const { RefCell::new(None) };
        }
        CACHE.with(|cell| {
            let child = counter!(parent: DYN_PARENT_COUNTER, "method" => "GET");
            *cell.borrow_mut() = Some((42, child));
        });
        b.iter(|| {
            CACHE.with(|cell| {
                let slot = cell.borrow();
                if let Some((h, ref cached)) = *slot
                    && h == 42
                {
                    cached.increment(1);
                    return;
                }
                unreachable!()
            })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// CONCURRENT MAP: DashMap vs papaya
// ---------------------------------------------------------------------------

fn dashmap_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache/concurrent_map/dashmap");

    {
        let map: dashmap::DashMap<u64, Arc<AtomicU64>> = dashmap::DashMap::new();
        map.insert(42, Arc::new(AtomicU64::new(0)));

        group.bench_function("get_existing", |b| {
            b.iter(|| {
                let entry = map.entry(42).or_insert_with(|| Arc::new(AtomicU64::new(0)));
                Arc::clone(&entry)
            });
        });
    }

    {
        let map: dashmap::DashMap<u64, Arc<AtomicU64>> = dashmap::DashMap::new();
        for i in 0u64..64 {
            map.insert(i, Arc::new(AtomicU64::new(0)));
        }

        let mut idx = 0u64;
        group.bench_function("get_existing_64", |b| {
            b.iter(|| {
                let key = idx % 64;
                idx += 1;
                let entry = map
                    .entry(key)
                    .or_insert_with(|| Arc::new(AtomicU64::new(0)));
                Arc::clone(&entry)
            });
        });
    }

    {
        let map: dashmap::DashMap<u64, Arc<AtomicU64>> = dashmap::DashMap::new();
        let mut idx = 0u64;
        group.bench_function("insert_miss", |b| {
            b.iter(|| {
                idx += 1;
                let entry = map
                    .entry(idx)
                    .or_insert_with(|| Arc::new(AtomicU64::new(0)));
                Arc::clone(&entry)
            });
        });
    }

    group.finish();
}

fn papaya_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache/concurrent_map/papaya");

    {
        let map: papaya::HashMap<u64, Arc<AtomicU64>> = papaya::HashMap::new();
        map.pin().insert(42, Arc::new(AtomicU64::new(0)));

        group.bench_function("get_existing", |b| {
            let guard = map.guard();
            b.iter(|| {
                map.get(&42, &guard).map(Arc::clone).unwrap_or_else(|| {
                    let new = Arc::new(AtomicU64::new(0));
                    map.insert(42, Arc::clone(&new), &guard);
                    new
                })
            });
        });
    }

    {
        let map: papaya::HashMap<u64, Arc<AtomicU64>> = papaya::HashMap::new();
        {
            let pinned = map.pin();
            for i in 0u64..64 {
                pinned.insert(i, Arc::new(AtomicU64::new(0)));
            }
        }

        let mut idx = 0u64;
        group.bench_function("get_existing_64", |b| {
            let guard = map.guard();
            b.iter(|| {
                let key = idx % 64;
                idx += 1;

                map.get(&key, &guard).map(Arc::clone).unwrap_or_else(|| {
                    let new = Arc::new(AtomicU64::new(0));
                    map.insert(key, Arc::clone(&new), &guard);
                    new
                })
            });
        });
    }

    {
        let map: papaya::HashMap<u64, Arc<AtomicU64>> = papaya::HashMap::new();
        let mut idx = 0u64;
        group.bench_function("insert_miss", |b| {
            let guard = map.guard();
            b.iter(|| {
                idx += 1;

                map.get(&idx, &guard).map(Arc::clone).unwrap_or_else(|| {
                    let new = Arc::new(AtomicU64::new(0));
                    map.insert(idx, Arc::clone(&new), &guard);
                    new
                })
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    cache_benches,
    key_hash_bench,
    thread_local_refcell_bench,
    dashmap_benches,
    papaya_benches,
);

criterion_main!(cache_benches);
