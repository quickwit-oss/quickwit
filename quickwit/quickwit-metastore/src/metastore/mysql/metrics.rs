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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{IntGauge, new_gauge};

#[derive(Clone)]
pub(super) struct MysqlMetrics {
    pub acquire_connections: IntGauge,
    pub active_connections: IntGauge,
    pub idle_connections: IntGauge,
}

impl Default for MysqlMetrics {
    fn default() -> Self {
        Self {
            acquire_connections: new_gauge(
                "mysql_acquire_connections",
                "Number of MySQL connections being acquired.",
                "metastore",
                &[],
            ),
            active_connections: new_gauge(
                "mysql_active_connections",
                "Number of active (used + idle) MySQL connections.",
                "metastore",
                &[],
            ),
            idle_connections: new_gauge(
                "mysql_idle_connections",
                "Number of idle MySQL connections.",
                "metastore",
                &[],
            ),
        }
    }
}

pub(super) static MYSQL_METRICS: Lazy<MysqlMetrics> = Lazy::new(MysqlMetrics::default);
