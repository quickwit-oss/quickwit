# Thread Pool Implementations

This module has two Rayon-backed executors for CPU work.

## `simple.rs`

`SimpleThreadPool` is the default small-task executor behind
`quickwit_common::thread_pool::run_cpu_intensive`.

It submits work directly to Rayon and keeps only the original cancellation
check: if the caller drops the returned future before Rayon starts executing
the closure, the closure is skipped. This implementation has the smallest
bookkeeping overhead and is meant for short CPU tasks such as decompression,
checksum computation, and small serialization work.

The `SimpleThreadPool` type is private. Callers should use the re-exported
`run_cpu_intensive` function for ordinary work. The module also exposes the
underlying Rayon pool for integration points, such as Tantivy executors, that
need a Rayon handle instead of a future-returning submission API.

## `with_priority.rs`

`ThreadPoolWithPriority` adds a Quickwit-owned queue in front of Rayon. It
keeps pending work outside Rayon until a worker slot is available, then
schedules high-priority tasks before normal-priority tasks.

This is useful when a long queue of normal work can delay latency-sensitive
follow-up work. The search thread pool uses this implementation so final result
merges can jump ahead of queued split-search CPU tasks.
