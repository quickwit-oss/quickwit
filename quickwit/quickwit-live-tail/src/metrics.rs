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
use quickwit_metrics::{LazyCounter, LazyHistogram, lazy_counter, lazy_histogram};

pub(crate) static LIVE_TAIL_DOCS_PUBLISHED: LazyCounter = lazy_counter!(
    name: "docs_published_total",
    description: "Number of documents published by indexing pipelines to live tail sessions.",
    subsystem: "live_tail",
);

static LIVE_TAIL_FILTERED_DOCS_TOTAL: LazyCounter = lazy_counter!(
    name: "filtered_docs_total",
    description: "Number of documents evaluated by live tail filters, by outcome.",
    subsystem: "live_tail",
);

pub(crate) static LIVE_TAIL_FILTERED_DOCS_MATCHED: LazyCounter =
    lazy_counter!(parent: LIVE_TAIL_FILTERED_DOCS_TOTAL, "outcome" => "matched");

pub(crate) static LIVE_TAIL_FILTERED_DOCS_UNMATCHED: LazyCounter =
    lazy_counter!(parent: LIVE_TAIL_FILTERED_DOCS_TOTAL, "outcome" => "unmatched");

pub(crate) static LIVE_TAIL_FILTER_DURATION_SECS: LazyHistogram = lazy_histogram!(
    name: "filter_duration_secs",
    description: "Duration of the evaluation of a live tail filter on a batch of documents.",
    subsystem: "live_tail",
    buckets: exponential_buckets(0.0005, 2.0, 12).unwrap(),
);
