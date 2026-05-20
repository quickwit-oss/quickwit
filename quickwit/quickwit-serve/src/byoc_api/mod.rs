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

mod rest_handler;

use quickwit_common::metrics::DEFAULT_BUCKETS;
use quickwit_metrics::{
    LabelNames, LazyCounter, LazyHistogram, label_names, lazy_counter, lazy_histogram,
};
pub(crate) use rest_handler::byoc_api_handlers;

const SIGNAL: LabelNames<1> = label_names!("signal");
const SIGNAL_STATUS_CODE: LabelNames<2> = label_names!("signal", "status_code");

static BYOC_INGEST_REQUESTS_TOTAL: LazyCounter = lazy_counter!(
    name: "byoc_ingest_requests.count",
    description: "Number of BYOC ingest requests by signal and status code.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static BYOC_INGEST_REQUEST_DURATION_SECONDS: LazyHistogram = lazy_histogram!(
    name: "byoc_ingest_requests.duration_seconds",
    description: "Duration of BYOC ingest requests in seconds by signal and status code.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
    buckets: DEFAULT_BUCKETS.to_vec(),
);

static BYOC_INGEST_BYTES_TOTAL: LazyCounter = lazy_counter!(
    name: "byoc_ingest_bytes.count",
    description: "Number of BYOC ingest bytes by signal.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static BYOC_INGEST_UNMATCHED_EVENTS_TOTAL: LazyCounter = lazy_counter!(
    name: "byoc_ingest_unmatched_events.count",
    description: "Number of BYOC log events with no matching routing rule.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);
