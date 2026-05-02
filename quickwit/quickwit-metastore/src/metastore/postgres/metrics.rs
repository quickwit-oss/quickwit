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

use quickwit_metrics::{Gauge, gauge};

#[derive(Clone)]
pub(super) struct PostgresMetrics {
    pub acquire_connections: Gauge,
    pub active_connections: Gauge,
    pub idle_connections: Gauge,
}

static ACQUIRE_CONNECTIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "acquire_connections",
        description: "Number of connections being acquired.",
        subsystem: "metastore",
    )
});

static ACTIVE_CONNECTIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "active_connections",
        description: "Number of active (used + idle) connections.",
        subsystem: "metastore",
    )
});

static IDLE_CONNECTIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "idle_connections",
        description: "Number of idle connections.",
        subsystem: "metastore",
    )
});

impl Default for PostgresMetrics {
    fn default() -> Self {
        Self {
            acquire_connections: ACQUIRE_CONNECTIONS.clone(),
            active_connections: ACTIVE_CONNECTIONS.clone(),
            idle_connections: IDLE_CONNECTIONS.clone(),
        }
    }
}

pub(super) static POSTGRES_METRICS: LazyLock<PostgresMetrics> =
    LazyLock::new(PostgresMetrics::default);
