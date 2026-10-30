# Counting live heap bytes

Quickwit's CLI wraps System, jemalloc, and profiled jemalloc with
`quickwit_common::CountingAllocator`. Read `quickwit_common::allocated_bytes()`
for approximate live Rust heap bytes without refreshing jemalloc statistics.
Other binaries must install the wrapper as their global allocator to use it.

## What is counted

The counter tracks requested allocation sizes, including zeroed allocations,
resizing, and frees. Failed allocations leave it unchanged. Allocator overhead,
retained pages, stacks, memory mappings, and native allocations that bypass the
Rust allocator are outside the count. Existing jemalloc metrics are unchanged.

## Accuracy

Small changes accumulate per thread and publish at 64 KiB to one of 64 shared
counters. Individual changes of at least 64 KiB publish immediately. Reads flush
the calling thread's changes and sum the counters. Thread exit flushes remaining
changes.

Other live threads can each leave less than 64 KiB unpublished in either
direction. Concurrent reads sample the counters sequentially, so allocations and
frees during a read can add further error. The final sum is clamped to zero.

## Using it for memory pressure

Read the counter before admitting work or from an existing task. The application
owns the heap budget, backpressure, and task shutdown. The counter starts no
background task and enforces no memory limit.

Leave headroom for uncounted memory, unpublished changes, and work already
admitted. Comparing this value with a container's memory limit alone cannot
prevent OOM kills.
