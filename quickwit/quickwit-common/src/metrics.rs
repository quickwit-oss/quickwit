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

use std::sync::LazyLock;

pub use prometheus::{exponential_buckets, linear_buckets};
use quickwit_metrics::{Gauge, gauge};

pub fn index_label(index_id: &str) -> &str {
    static PER_INDEX_METRICS_ENABLED: LazyLock<bool> =
        LazyLock::new(|| !crate::get_bool_from_env("QW_DISABLE_PER_INDEX_METRICS", false));

    if *PER_INDEX_METRICS_ENABLED {
        index_id
    } else {
        "__any__"
    }
}

pub static MEMORY_ACTIVE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "active_bytes",
        description: "Total number of bytes in active pages allocated by the application, as reported by jemalloc `stats.active`.",
        subsystem: "memory",
    )
});

pub static MEMORY_ALLOCATED_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "allocated_bytes",
        description: "Total number of bytes allocated by the application, as reported by jemalloc `stats.allocated`.",
        subsystem: "memory",
    )
});

pub static MEMORY_RESIDENT_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "resident_bytes",
        description: " Total number of bytes in physically resident data pages mapped by the allocator, as reported by jemalloc `stats.resident`.",
        subsystem: "memory",
    )
});

static IN_FLIGHT_DATA_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "in_flight_data_bytes",
        description: "Amount of data in-flight in various buffers in bytes.",
        subsystem: "memory",
    )
});

pub static IN_FLIGHT_REST_SERVER: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("rest_server"));

pub static IN_FLIGHT_INGEST_ROUTER: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingest_router"));

pub static IN_FLIGHT_INGESTER_PERSIST: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingester_persist"));

pub static IN_FLIGHT_INGESTER_REPLICATE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingester_replicate"));

pub static IN_FLIGHT_WAL: LazyLock<Gauge> = LazyLock::new(|| in_flight_data_gauge("wal"));

pub static IN_FLIGHT_FETCH_STREAM: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("fetch_stream"));

pub static IN_FLIGHT_MULTI_FETCH_STREAM: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("multi_fetch_stream"));

pub static IN_FLIGHT_DOC_PROCESSOR_MAILBOX: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("doc_processor_mailbox"));

pub static IN_FLIGHT_INDEXER_MAILBOX: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("indexer_mailbox"));

pub static IN_FLIGHT_INDEX_WRITER: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("index_writer"));

pub static IN_FLIGHT_FILE_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("file_source"));

pub static IN_FLIGHT_INGEST_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingest_source"));

pub static IN_FLIGHT_KAFKA_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("kafka_source"));

pub static IN_FLIGHT_KINESIS_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("kinesis_source"));

pub static IN_FLIGHT_PUBSUB_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("pubsub_source"));

pub static IN_FLIGHT_PULSAR_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("pulsar_source"));

pub static IN_FLIGHT_OTHER_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("other_source"));

fn in_flight_data_gauge(component: &'static str) -> Gauge {
    gauge!(parent: IN_FLIGHT_DATA_BYTES, "component" => component)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_helpers_are_reexported() {
        assert_eq!(linear_buckets(0.0, 1.0, 3).unwrap(), vec![0.0, 1.0, 2.0]);
        assert_eq!(
            exponential_buckets(1.0, 2.0, 3).unwrap(),
            vec![1.0, 2.0, 4.0]
        );
    }
}
