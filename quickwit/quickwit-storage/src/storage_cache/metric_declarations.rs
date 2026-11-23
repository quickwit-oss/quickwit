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

use mixtrics::metrics::Buckets;

use super::{
    CreateMetric, Declaration, MetricKind, MetricsCounter, MetricsGauge, MetricsHistogram,
};

// The native schema, typed registrations, inventory entries, and exporter buckets all come
// from this list. Keep descriptions and ordered labels identical to the pinned Feuer revision.
macro_rules! declare_metrics {
    ($(
        $kind:ident, $lazy_type:ident, $lazy:ident, $metric:ident, $wrapper:ident {
            $(
                $id:ident: $name:literal, $description:literal,
                [$($label:literal => $value:ident),*] $(, buckets: $buckets:expr)?;
            )*
        }
    )*) => {
        $($(
            static $id: quickwit_metrics::$lazy_type = quickwit_metrics::$lazy!(
                name: $name,
                description: $description,
                system: "",
                subsystem: "",
                $(buckets: $buckets,)?
            );
        )*)*

        pub(super) static DECLARATIONS: &[Declaration] = &[$($(
            Declaration {
                name: $name,
                description: $description,
                kind: MetricKind::$kind,
                label_names: &[$($label),*],
                buckets: declare_metrics!(@buckets $($buckets)?),
                create_metric: CreateMetric::$kind(|cache_name, label_values| {
                    let [$($value),*] = label_values else {
                        panic!("incorrect Feuer label value count: {}", $name);
                    };
                    Box::new($wrapper(quickwit_metrics::$metric!(
                        parent: $id,
                        "cache" => cache_name,
                        $($label => $value.clone().into_owned(),)*
                    )))
                }),
            },
        )*)*];
    };
    (@buckets) => { None };
    (@buckets $buckets:expr) => { Some(|| $buckets) };
}

declare_metrics! {
    Counter, LazyCounter, lazy_counter, counter, MetricsCounter {
        LOOKUP_TOTAL: "feuer_lookup_total",
        "Completed Feuer lookups",
        ["outcome" => outcome];

        LOOKUP_BYTES: "feuer_lookup_bytes_total",
        "Object payload bytes returned by successful Feuer lookups",
        ["source" => source];

        ADMISSION_FILTER: "feuer_admission_filter_total",
        "Completed caller-supplied admission filter decisions",
        ["tier" => tier, "outcome" => outcome];

        MEMORY_OPERATIONS: "feuer_memory_operations_total",
        "Operations completed by Feuer's in-memory object tier",
        ["operation" => operation];

        MEMORY_INSERTIONS_TRIGGERING_EVICTION: "feuer_memory_eviction_triggering_insertions_total",
        "Completed insertion attempts that evicted at least one entry under capacity pressure",
        [];

        DISK_IO: "feuer_disk_io_total",
        "Completed Feuer data-file operations",
        ["operation" => operation, "outcome" => outcome];

        DISK_IO_BYTES: "feuer_disk_io_bytes_total",
        "Bytes completed by Feuer data-file operations",
        ["operation" => operation];

        DISK_LOOKUP: "feuer_disk_lookup_total",
        "Completed disk-tier lookups, including buffered entries. Errors become misses",
        ["outcome" => outcome];

        DISK_WRITE_ENTRIES: "feuer_disk_write_entries_total",
        "Terminal outcomes of entries handled by the disk writer",
        ["outcome" => outcome];

        DISK_WRITE_QUEUE: "feuer_disk_write_queue_total",
        "Disk-write queue admissions, rejections and pre-write discards. The queued outcome measures admission",
        ["outcome" => outcome];

        DISK_INSERTIONS_TRIGGERING_EVICTION: "feuer_disk_eviction_triggering_insertions_total",
        "Terminal insertion attempts that evicted at least one entry under capacity pressure",
        [];

        DISK_WRITTEN_ENTRIES: "feuer_disk_written_entries_total",
        "Entries in successfully written payload regions, whether published or discarded",
        [];

        DISK_BATCH_BYTES: "feuer_disk_batch_bytes_total",
        "Payload and payload-chunk bytes in successfully written regions before publication",
        ["kind" => kind];
    }
    Gauge, LazyGauge, lazy_gauge, gauge, MetricsGauge {
        MEMORY_USED_BYTES: "feuer_memory_used_bytes",
        "Allocation bytes charged to cached objects plus idle buffer capacity",
        [];

        MEMORY_CAPACITY_BYTES: "feuer_memory_capacity_bytes",
        "Shared capacity of live memory caches and their buffer pools",
        [];

        IO_BUFFER_POOL_BYTES: "feuer_io_buffer_pool_bytes",
        "Aligned buffer allocation bytes by size and status",
        ["bucket" => bucket, "status" => status];

        MEMORY_ENTRIES: "feuer_memory_entries",
        "Object entries in Feuer's memory tier",
        [];

        DISK_READ_QUEUE_WAITERS: "feuer_disk_read_queue_waiters",
        "Read-piece requests waiting for I/O channel capacity, excluding admitted and active requests",
        [];

        DISK_WRITE_QUEUED_ENTRIES: "feuer_disk_write_queued_entries",
        "Entries waiting to start disk writes",
        [];

        DISK_WRITE_PENDING_BYTES: "feuer_disk_write_pending_bytes",
        "Logical payload bytes queued, prepared, or being written",
        [];

        DISK_RECOVERED_CHUNKS: "feuer_disk_recovered_chunks",
        "Currently allocated chunks restored by recovery, excluding temporary scan reservations",
        [];

        DISK_CHUNKS: "feuer_disk_chunks",
        "1-MiB chunks by state. Allocated includes payloads, metadata, and unfinished writes",
        ["state" => state];

        DISK_PAYLOAD_BYTES: "feuer_disk_payload_bytes",
        "Payload bytes in indexed disk entries, excluding metadata and padding",
        [];

        DISK_ENTRIES: "feuer_disk_entries",
        "Indexed disk range entries",
        [];
    }
    Histogram, LazyHistogram, lazy_histogram, histogram, MetricsHistogram {
        LOOKUP_DURATION: "feuer_lookup_duration_seconds",
        "Successful Feuer lookup duration after a memory miss, including callback work",
        ["outcome" => outcome, "size_range" => size_range], buckets: Buckets::exponential(0.000_001, 2.0, 30);

        DISK_IO_DURATION: "feuer_disk_io_duration_seconds",
        "Successful Feuer data-file operation duration in seconds",
        ["operation" => operation, "outcome" => outcome],
        buckets: Buckets::exponential(0.000_001, 2.0, 25);

        DISK_READ_SIZE: "feuer_disk_read_size_bytes",
        "Requested bytes per successful Feuer data-file read, excluding alignment padding",
        [], buckets: Buckets::exponential(1024.0, 2.0, 21);

        DISK_READ_QUEUE_WAIT_DURATION: "feuer_disk_read_queue_wait_duration_seconds",
        "Read-piece channel-capacity waits ending in admission, cancellation or queue closure",
        [], buckets: Buckets::exponential(0.000_001, 2.0, 25);

        DISK_LOOKUP_DURATION: "feuer_disk_lookup_duration_seconds",
        "Completed disk-tier hit duration, including buffered copies or verified reads",
        [], buckets: Buckets::exponential(0.000_001, 2.0, 25);

        DISK_WRITE_QUEUE_DURATION: "feuer_disk_write_queue_duration_seconds",
        "Time from queue admission to dequeue",
        [], buckets: Buckets::exponential(0.000_001, 2.0, 25);
    }
}
