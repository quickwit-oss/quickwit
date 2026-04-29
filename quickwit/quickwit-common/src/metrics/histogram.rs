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

use metrics::HistogramFn;

use super::MetricInfo;

#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct HistogramConfig {
    pub info: &'static MetricInfo,
    pub buckets_fn: fn() -> Vec<f64>,
}

inventory::collect!(HistogramConfig);

#[derive(Clone)]
pub struct Histogram {
    pub(crate) info: &'static HistogramConfig,
    pub(crate) key: metrics::Key,
    pub(crate) inner: metrics::Histogram,
}

impl std::fmt::Debug for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Histogram").field("key", &self.key).finish()
    }
}

impl Histogram {
    #[doc(hidden)]
    pub fn __new(
        info: &'static HistogramConfig,
        key: metrics::Key,
        inner: metrics::Histogram,
    ) -> Self {
        Self { info, key, inner }
    }

    #[doc(hidden)]
    pub const fn __info(&self) -> &'static HistogramConfig {
        self.info
    }

    pub const fn key(&self) -> &metrics::Key {
        &self.key
    }

    pub fn record(&self, value: f64) {
        self.inner.record(value);
    }
}

impl HistogramFn for Histogram {
    fn record(&self, value: f64) {
        Self::record(self, value);
    }
}

#[macro_export]
macro_rules! histogram {
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: "",
        buckets: $buckets:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Histogram,
            observable: false,
            name: $name,
            description: $description,
            subsystem: ""
            $(, $label => $value)*
        );

        static HISTOGRAM_CONFIG: $crate::metrics::HistogramConfig =
            $crate::metrics::HistogramConfig {
                info: &INFO,
                buckets_fn: || $buckets,
            };
        $crate::metrics::__inventory::submit!(HISTOGRAM_CONFIG);

        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_histogram(&KEY, &METADATA)
        });
        $crate::metrics::Histogram::__new(&HISTOGRAM_CONFIG, KEY.clone(), inner)
    }};

    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:literal,
        buckets: $buckets:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Histogram,
            observable: false,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );

        static HISTOGRAM_CONFIG: $crate::metrics::HistogramConfig =
            $crate::metrics::HistogramConfig {
                info: &INFO,
                buckets_fn: || $buckets,
            };
        $crate::metrics::__inventory::submit!(HISTOGRAM_CONFIG);

        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_histogram(&KEY, &METADATA)
        });
        $crate::metrics::Histogram::__new(&HISTOGRAM_CONFIG, KEY.clone(), inner)
    }};

    (
        parent: $parent:expr,
        $($label:literal => $value:expr),+ $(,)?
    ) => {{
        let parent_key = $parent.key();
        let mut labels =
            Vec::with_capacity(parent_key.labels().len() + $crate::count!($($label)*));
        labels.extend(parent_key.labels().cloned());
        $(labels.push($crate::metrics::__metrics::Label::new($label, $value));)+

        let info = $parent.__info();
        let key = $crate::metrics::__metrics::Key::from_parts(info.info.key_name, labels);
        let metadata = $crate::metadata!(info.info.subsystem);

        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_histogram(&key, &metadata)
        });

        $crate::metrics::Histogram::__new(info, key, inner)
    }};
}
