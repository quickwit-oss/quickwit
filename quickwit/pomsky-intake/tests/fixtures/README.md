# USM intake dump-equivalence workflow

This directory holds the tooling for validating that `pomsky-intake`'s
`connections_to_apm_metrics` transform produces the same metric events as the
SaaS-side USM pipeline does for the same input.

## Pipeline

```
 DD Agent dump file (pinned, not checked in — too large)
        │
        ▼
 ┌──────────────────────────────────────────────────┐
 │ dd-source byoc-usm-stats, EMIT_MODE=jsonall      │   Go reference
 └──────────────────────────────────────────────────┘
        │
        ▼                  ┌─────────────────────┐
 /tmp/go-reference.ndjson ─┤ compare_ndjson.py   ├─── EQUIVALENT / DIVERGENT
                           │   (this directory)  │
 /tmp/rust-reference.ndjson┘─────────────────────┘
        ▲
 ┌──────────────────────────────────────────────────┐
 │ pomsky-intake::dump_equivalence_write_ndjson     │   Rust under test
 └──────────────────────────────────────────────────┘
```

## Running end-to-end

```bash
# 1. Produce the Go reference.
cd ~/go/src/github.com/DataDog/dd-source/domains/quickhouse/apps/byoc-usm-stats
EMIT_MODE=jsonall EMIT_FILE=/tmp/go-reference.ndjson \
    go run -tags=dynamic . ~/Downloads/dump_1775477179

# 2. Produce the Rust output.
cd ~/dd/pomsky/quickwit
USM_DUMP_PATH=~/Downloads/dump_1775477179 \
USM_RUST_NDJSON=/tmp/rust-reference.ndjson \
    cargo test -p pomsky-intake --release --lib \
        dump_equivalence_write_ndjson -- --ignored --nocapture

# 3. Compare.
python3 pomsky-intake/tests/fixtures/compare_ndjson.py \
    /tmp/go-reference.ndjson /tmp/rust-reference.ndjson
```

## Complementary smoke test

```bash
USM_DUMP_PATH=~/Downloads/dump_1775477179 \
    cargo test -p pomsky-intake --release --lib \
        dump_smoke_runs_transform_across_full_dump -- --ignored --nocapture
```

Runs only the Rust side and reports aggregate stats (messages processed,
decode errors, metric families emitted). Useful when the Go side isn't
available.

## Equivalence relation

See the docstring at the top of `compare_ndjson.py`.
