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

use std::collections::BTreeMap;
use std::sync::{LazyLock, OnceLock};

use super::{Gauge, SYSTEM};
use crate::gauge;

pub fn register_info(name: &'static str, help: &'static str, kvs: BTreeMap<&'static str, String>) {
    let key_name = metric_key_name("", name);
    let labels = kvs
        .into_iter()
        .map(|(label, value)| metrics::Label::new(label, value))
        .collect::<Vec<_>>();
    let key = metrics::Key::from_parts(key_name.clone(), labels);
    let metadata = metrics::Metadata::new("", metrics::Level::INFO, Some(module_path!()));
    metrics::with_recorder(|recorder| {
        recorder.describe_counter(metrics::KeyName::from(key_name), None, help.into());
        recorder.register_counter(&key, &metadata).increment(1);
    });
}

#[derive(Clone)]
pub struct MemoryMetrics {
    pub active_bytes: Gauge,
    pub allocated_bytes: Gauge,
    pub resident_bytes: Gauge,
    pub in_flight: InFlightDataGauges,
}

impl Default for MemoryMetrics {
    fn default() -> Self {
        Self {
            active_bytes: MEMORY_ACTIVE_BYTES.clone(),
            allocated_bytes: MEMORY_ALLOCATED_BYTES.clone(),
            resident_bytes: MEMORY_RESIDENT_BYTES.clone(),
            in_flight: InFlightDataGauges::default(),
        }
    }
}

#[derive(Clone)]
pub struct InFlightDataGauges {
    pub rest_server: Gauge,
    pub ingest_router: Gauge,
    pub ingester_persist: Gauge,
    pub ingester_replicate: Gauge,
    pub wal: Gauge,
    pub fetch_stream: Gauge,
    pub multi_fetch_stream: Gauge,
    pub doc_processor_mailbox: Gauge,
    pub indexer_mailbox: Gauge,
    pub index_writer: Gauge,
}

impl Default for InFlightDataGauges {
    fn default() -> Self {
        Self {
            rest_server: in_flight_data_gauge("rest_server"),
            ingest_router: in_flight_data_gauge("ingest_router"),
            ingester_persist: in_flight_data_gauge("ingester_persist"),
            ingester_replicate: in_flight_data_gauge("ingester_replicate"),
            wal: in_flight_data_gauge("wal"),
            fetch_stream: in_flight_data_gauge("fetch_stream"),
            multi_fetch_stream: in_flight_data_gauge("multi_fetch_stream"),
            doc_processor_mailbox: in_flight_data_gauge("doc_processor_mailbox"),
            indexer_mailbox: in_flight_data_gauge("indexer_mailbox"),
            index_writer: in_flight_data_gauge("index_writer"),
        }
    }
}

impl InFlightDataGauges {
    #[inline]
    pub fn file(&self) -> &'static Gauge {
        static GAUGE: OnceLock<Gauge> = OnceLock::new();
        GAUGE.get_or_init(|| in_flight_data_gauge("file_source"))
    }

    #[inline]
    pub fn ingest(&self) -> &'static Gauge {
        static GAUGE: OnceLock<Gauge> = OnceLock::new();
        GAUGE.get_or_init(|| in_flight_data_gauge("ingest_source"))
    }

    #[inline]
    pub fn kafka(&self) -> &'static Gauge {
        static GAUGE: OnceLock<Gauge> = OnceLock::new();
        GAUGE.get_or_init(|| in_flight_data_gauge("kafka_source"))
    }

    #[inline]
    pub fn kinesis(&self) -> &'static Gauge {
        static GAUGE: OnceLock<Gauge> = OnceLock::new();
        GAUGE.get_or_init(|| in_flight_data_gauge("kinesis_source"))
    }

    #[inline]
    pub fn pubsub(&self) -> &'static Gauge {
        static GAUGE: OnceLock<Gauge> = OnceLock::new();
        GAUGE.get_or_init(|| in_flight_data_gauge("pubsub_source"))
    }

    #[inline]
    pub fn pulsar(&self) -> &'static Gauge {
        static GAUGE: OnceLock<Gauge> = OnceLock::new();
        GAUGE.get_or_init(|| in_flight_data_gauge("pulsar_source"))
    }

    #[inline]
    pub fn other(&self) -> &'static Gauge {
        static GAUGE: OnceLock<Gauge> = OnceLock::new();
        GAUGE.get_or_init(|| in_flight_data_gauge("pulsar_source"))
    }
}

pub fn index_label(index_id: &str) -> &str {
    static PER_INDEX_METRICS_ENABLED: LazyLock<bool> =
        LazyLock::new(|| !crate::get_bool_from_env("QW_DISABLE_PER_INDEX_METRICS", false));

    if *PER_INDEX_METRICS_ENABLED {
        index_id
    } else {
        "__any__"
    }
}

pub static MEMORY_METRICS: LazyLock<MemoryMetrics> = LazyLock::new(MemoryMetrics::default);

static MEMORY_ACTIVE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "active_bytes",
        description: "Total number of bytes in active pages allocated by the application, as reported by jemalloc `stats.active`.",
        subsystem: "memory",
    )
});

static MEMORY_ALLOCATED_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "allocated_bytes",
        description: "Total number of bytes allocated by the application, as reported by jemalloc `stats.allocated`.",
        subsystem: "memory",
    )
});

static MEMORY_RESIDENT_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
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

fn in_flight_data_gauge(component: &'static str) -> Gauge {
    gauge!(parent: &*IN_FLIGHT_DATA_BYTES, "component" => component)
}

fn metric_key_name(subsystem: &str, name: &str) -> String {
    if subsystem.is_empty() {
        format!("{SYSTEM}_{name}")
    } else {
        format!("{SYSTEM}_{subsystem}_{name}")
    }
}
