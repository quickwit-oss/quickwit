# Root search resource logging

Each `root_search` invocation emits a `root_resource_stats` event when its guard
is dropped, including planning errors, execution errors, and cancellation of the
search future (for example, by a timeout). As with other drop-based logging, this
cannot guarantee delivery on process abort or termination.

The guard records resource stats from each returned leaf response before joining
all responses. A later leaf error or cancellation therefore does not discard
stats already received. Hits and aggregation payloads are not retained for logging.

- `status`: `success`, `error`, or `cancelled`.
- `root_wall_time_microsecs`: elapsed time across planning and execution.
- `leaf_num_calls`, `leaf_num_calls_including_retries`: dispatched leaf batches
  and attempts, respectively.
- `leaf_num_responses`, `leaf_num_responses_with_stats`: response coverage.
- `resource_stats_available`: whether any leaf returned resource stats.
- `resource_stats_partial`: true on error/cancellation, missing leaf stats, or
  reported split failures. A successful metadata-only count can have no leaf calls
  and no resource stats without being partial.
- `sleaf_ssplit_cpu_search_microsecs`, `sleaf_ssplit_warmup_microsecs`, and the
  corresponding wait fields: sums over received leaf resource stats, not wall time.
- `leaf_resources_sum` and `leaf_resources_worst`: debug representations of the
  full received leaf stats for inspection.

A root cannot report work still running remotely, or stats lost with a failed
RPC. Missing CPU timings are omitted, **not reported as zero**. Partial sums must
not be treated as total resource usage for the request. Leaf-local metrics remain
necessary for work whose response never reaches the root.

This guard covers `root_search`; subsequent scroll-page execution that calls the
partial-hits phase directly does not emit an additional root event.
