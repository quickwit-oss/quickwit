# Quickwit Benchmarking Guide

## Philosophy

**Always measure before and after optimizations.**

Reference: [Jeff Dean & Sanjay Ghemawat Performance Hints](https://abseil.io/fast/hints.html)

## Performance Verification Pyramid

Similar to the correctness Verification Pyramid, performance is verified across layers:

```
         Local Benchmarks (cargo bench)
                    │ defines thresholds
         Shared Baselines (performance/)  ← SINGLE SOURCE
                    │ verified by
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
Production      Observability   APM Profiles
Metrics         Metrics         (perf/samply)
```

## Microbenchmarks

### Other Crates

```bash
# Document transforms (log/trace preprocessing)
cargo bench -p quickwit-doc-transforms

# Document mapper (routing expressions, doc-to-JSON)
cargo bench -p quickwit-doc-mapper

# Query (tokenizers, multilang tokenizers)
cargo bench -p quickwit-query

# Common utilities (serialized JSON size)
cargo bench -p quickwit-common

# Actor framework (mailbox throughput)
cargo bench -p quickwit-actors

# Compare against baseline
cargo bench -p quickwit-doc-mapper -- --save-baseline before
# ... make changes ...
cargo bench -p quickwit-doc-mapper -- --baseline before
```

### Key Metrics

| Benchmark | What It Measures | Crate |
|-----------|------------------|-------|
| `processors_bench` | Document transform throughput | `quickwit-doc-transforms` |
| `tokenizers_bench` | Tokenizer performance | `quickwit-query` |

## End-to-End Benchmarks

```bash
# Start quickwit
cargo run --release -p quickwit-cli -- run --config ../config/quickwit.yaml

# Send metrics via OTLP gRPC (port 4317)
# Then query via REST API
curl http://localhost:7280/api/v1/<index>/search -d '{"query": "*"}'

# Timing
time curl -s "http://localhost:7280/api/v1/<index>/search" -d '{"query": "*"}' > /dev/null
```

## Performance Baselines

Performance baselines should be defined in a shared location and checked in both benchmarks and production:

```rust
pub const QUERY_LATENCY_P99_BASELINE: PerformanceBaseline = PerformanceBaseline {
    name: "query_latency_p99",
    target: 500.0,      // 500ms target
    warning: 2000.0,    // 2s warning
    critical: 10000.0,  // 10s critical
};
```

### Key Baselines (targets)

| Baseline | Target | Warning | Critical |
|----------|--------|---------|----------|
| `query_latency_p99` | 500ms | 2s | 10s |
| `ingest_bytes_throughput` | 100MB/s | 50MB/s | 10MB/s |
| `split_build_latency` | 500ms | 1s | 5s |
| `metrics_ingestion_throughput` | TBD | TBD | TBD |

## Production Checking

```rust
// After every query execution
let result = check_performance(&QUERY_LATENCY_P99_BASELINE, latency_ms);
quickwit_observability::record_performance(result.name, result.actual, result.status);
```

### Observability Metrics

| Metric | Purpose |
|--------|---------|
| `quickwit.performance.checks.total` | Total checks |
| `quickwit.performance.checks.warning` | Warnings |
| `quickwit.performance.checks.critical` | Critical (investigate now) |
| `quickwit.performance.health` | Health gauge (0=healthy, 2=critical) |

## APM Correlation

When performance degrades, use CPU profiling tools (perf, samply, Instruments) to identify the hot path. See the Profiling section below.

## Optimization Checklist

Before making performance changes:

1. [ ] Run relevant benchmarks, save baseline
2. [ ] Identify specific metric to improve
3. [ ] Make targeted change
4. [ ] Run benchmarks, compare against baseline
5. [ ] Verify no regression in other metrics
6. [ ] Document improvement in commit message

## Common Optimizations

### Memory Allocation

```rust
// BAD: Allocates per-iteration
for item in items {
    let s = format!("{}", item);
}

// GOOD: Reuse buffer
let mut buf = String::new();
for item in items {
    buf.clear();
    write!(&mut buf, "{}", item)?;
}
```

### String Processing

```rust
// BAD: Multiple allocations
let s = s.replace("foo", "bar").replace("baz", "qux");

// GOOD: Single pass
let s = MULTI_REPLACE_REGEX.replace_all(&s, |caps: &Captures| {
    match &caps[0] {
        "foo" => "bar",
        "baz" => "qux",
        _ => unreachable!(),
    }
});
```

### Batch Processing

```rust
// BAD: Individual inserts
for item in items {
    insert_one(item).await?;
}

// GOOD: Batch insert
insert_batch(items).await?;
```

### Avoid Copies

```rust
// BAD: Unnecessary clone
let data = source.clone();
process(data);

// GOOD: Borrow when possible
process(&source);

// GOOD: Move when done
let data = source;  // source no longer needed
process(data);
```

## Profiling

### CPU Profiling

```bash
# With perf (Linux)
perf record -g ./target/release/quickwit ...
perf report

# With samply (cross-platform, good for macOS)
samply record ./target/release/quickwit ...

# With Instruments (macOS)
xcrun xctrace record --template "Time Profiler" --launch ./target/release/quickwit ...
```

### Memory Profiling

```bash
# With heaptrack (Linux)
heaptrack ./target/release/quickwit ...
heaptrack_gui heaptrack.quickwit.*.gz

# With jemalloc profiling (Docker build)
# From repo root:
make docker-build-profiled
make k8s-deploy-profiled
make k8s-profile-control-plane
```

### Async Profiling

```bash
# tokio-console for async debugging
# Build with tokio-console feature:
RUSTFLAGS="--cfg tokio_unstable" cargo install --path quickwit-cli --features tokio-console
QW_ENABLE_TOKIO_CONSOLE=1 quickwit run ...
tokio-console
```

## References

- [Jeff Dean & Sanjay Ghemawat Performance Hints](https://abseil.io/fast/hints.html)
- [Criterion.rs User Guide](https://bheisler.github.io/criterion.rs/book/)
- [Binggan Benchmarking](https://github.com/PSeitz/binggan)
- [samply Profiler](https://github.com/mstange/samply)
