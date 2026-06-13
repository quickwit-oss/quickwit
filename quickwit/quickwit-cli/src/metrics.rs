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

use quickwit_common::metrics::exponential_buckets;
use quickwit_metrics::{LazyGauge, LazyHistogram, gauge, labels, lazy_gauge, lazy_histogram};
use quickwit_serve::BuildInfo;

static BUILD_INFO: LazyGauge = lazy_gauge!(
        name: "build_info",
        description: "Quickwit's build info",
        subsystem: "",
);
pub fn register_build_info_metric(build_info: &BuildInfo) {
    let commit_tags = build_info.commit_tags.join(",");
    let labels = labels!(
        "build_date" => build_info.build_date,
        "commit_hash" => build_info.commit_short_hash,
        "version" => build_info.version.clone(),
        "commit_tags" => commit_tags,
        "target" => build_info.build_target,
    );
    gauge!(parent: BUILD_INFO, labels: [labels]).set(1.0);
}

pub(crate) static THREAD_UNPARK_DURATION_MICROSECONDS: LazyHistogram = lazy_histogram!(
        name: "thread_unpark_duration_microseconds",
        description: "Duration for which a thread of the main tokio runtime is unparked.",
        subsystem: "cli",
        buckets: exponential_buckets(5.0, 5.0, 5).unwrap(),
);
