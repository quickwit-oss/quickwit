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

#[doc(hidden)]
pub use atomic_float as __atomic_float;
#[doc(hidden)]
pub use const_format::concatcp as __concatcp;
#[doc(hidden)]
pub use inventory as __inventory;
#[doc(hidden)]
pub use metrics as __metrics;
pub use metrics::{CounterFn, GaugeFn, HistogramFn};
pub use metrics_util::MetricKind;
pub use prometheus::{exponential_buckets, linear_buckets};

mod counter;
mod gauge;
mod histogram;
mod quickwit;

pub use counter::Counter;
#[doc(hidden)]
pub use counter::CounterShadow;
#[doc(hidden)]
pub use gauge::GaugeShadow;
pub use gauge::{Gauge, GaugeGuard};
pub use histogram::{Histogram, HistogramConfig};
pub use quickwit::{
    InFlightDataGauges, MEMORY_METRICS, MemoryMetrics, index_label, metrics_text_payload,
    register_info,
};

#[cfg(test)]
mod tests;

/// System-level prefix prepended to every metric name.
pub const SYSTEM: &str = "quickwit";

#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct MetricInfo {
    pub name: &'static str,
    pub subsystem: &'static str,
    pub key_name: &'static str,
    pub description: &'static str,
    pub kind: MetricKind,
    pub observable: bool,
}

inventory::collect!(MetricInfo);

pub fn describe_metrics() {
    metrics::with_recorder(|recorder| {
        for info in inventory::iter::<MetricInfo> {
            let key_name = metrics::KeyName::from_const_str(info.key_name);
            let description: metrics::SharedString = info.description.into();
            match info.kind {
                MetricKind::Counter => recorder.describe_counter(key_name, None, description),
                MetricKind::Gauge => recorder.describe_gauge(key_name, None, description),
                MetricKind::Histogram => recorder.describe_histogram(key_name, None, description),
            }
        }
    });
}

pub fn metrics_info() -> impl Iterator<Item = &'static MetricInfo> {
    inventory::iter::<MetricInfo>.into_iter()
}

pub fn histogram_buckets() -> impl Iterator<Item = (&'static str, Vec<f64>)> {
    inventory::iter::<HistogramConfig>
        .into_iter()
        .map(|config| (config.info.key_name, (config.buckets_fn)()))
}

#[doc(hidden)]
#[macro_export]
macro_rules! key_name {
    ("", $name:literal) => {
        $crate::metrics::__concatcp!($crate::metrics::SYSTEM, "_", $name)
    };
    ($subsystem:literal, $name:literal) => {
        $crate::metrics::__concatcp!($crate::metrics::SYSTEM, "_", $subsystem, "_", $name)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! count {
    () => {
        0usize
    };
    ($head:tt $($tail:tt)*) => {
        1usize + $crate::count!($($tail)*)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! metadata {
    ($subsystem:expr) => {
        $crate::metrics::__metrics::Metadata::new(
            $subsystem,
            $crate::metrics::__metrics::Level::INFO,
            Some(module_path!()),
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! key_info_metadata {
    (
        kind: $kind:expr,
        observable: $observable:expr,
        name: $name:literal,
        description: $description:literal,
        subsystem: ""
        $(, $label:literal => $value:literal)* $(,)?
    ) => {
        const KEY_NAME: &str = $crate::metrics::__concatcp!($crate::metrics::SYSTEM, "_", $name);
        static INFO: $crate::metrics::MetricInfo = $crate::metrics::MetricInfo {
            name: $name,
            subsystem: "",
            key_name: KEY_NAME,
            description: $description,
            kind: $kind,
            observable: $observable,
        };
        $crate::metrics::__inventory::submit!(INFO);

        static LABELS: [$crate::metrics::__metrics::Label; $crate::count!($($label)*)] = [
            $($crate::metrics::__metrics::Label::from_static_parts($label, $value)),*
        ];
        static KEY: $crate::metrics::__metrics::Key =
            $crate::metrics::__metrics::Key::from_static_parts(KEY_NAME, &LABELS);
        static METADATA: $crate::metrics::__metrics::Metadata<'static> =
            $crate::metadata!("");
    };

    (
        kind: $kind:expr,
        observable: $observable:expr,
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:literal
        $(, $label:literal => $value:literal)* $(,)?
    ) => {
        const KEY_NAME: &str = $crate::key_name!($subsystem, $name);
        static INFO: $crate::metrics::MetricInfo = $crate::metrics::MetricInfo {
            name: $name,
            subsystem: $subsystem,
            key_name: KEY_NAME,
            description: $description,
            kind: $kind,
            observable: $observable,
        };
        $crate::metrics::__inventory::submit!(INFO);

        static LABELS: [$crate::metrics::__metrics::Label; $crate::count!($($label)*)] = [
            $($crate::metrics::__metrics::Label::from_static_parts($label, $value)),*
        ];
        static KEY: $crate::metrics::__metrics::Key =
            $crate::metrics::__metrics::Key::from_static_parts(KEY_NAME, &LABELS);
        static METADATA: $crate::metrics::__metrics::Metadata<'static> =
            $crate::metadata!($subsystem);
    };
}

pub use crate::{counter, gauge, histogram};
