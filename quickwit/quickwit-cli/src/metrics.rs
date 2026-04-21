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

use quickwit_common::metrics::Histogram;

quickwit_common::define_histogram! {
    THREAD_UNPARK_DURATION_MICROSECONDS,
    name: "thread_unpark_duration_microseconds",
    help: "Duration for which a thread of the main tokio runtime is unparked.",
    subsystem: "cli",
    buckets: quickwit_common::metrics::exponential_buckets(5.0, 5.0, 5).unwrap(),
}

pub struct CliMetrics {
    pub thread_unpark_duration_microseconds: Histogram,
}

impl Default for CliMetrics {
    fn default() -> Self {
        CliMetrics {
            thread_unpark_duration_microseconds: THREAD_UNPARK_DURATION_MICROSECONDS.clone(),
        }
    }
}

/// CLI counters exposes a set of metrics about the main tokio runtime.
pub static CLI_METRICS: LazyLock<CliMetrics> = LazyLock::new(CliMetrics::default);
