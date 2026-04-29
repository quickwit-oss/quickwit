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

use std::sync::Arc;
use std::sync::atomic::Ordering;

use atomic_float::AtomicF64;
use metrics::GaugeFn;

use super::MetricInfo;

#[doc(hidden)]
pub enum GaugeShadow {
    Noop,
    Ref(&'static AtomicF64),
    Arc(Arc<AtomicF64>),
}

impl Clone for GaugeShadow {
    fn clone(&self) -> Self {
        match self {
            Self::Noop => Self::Noop,
            Self::Ref(value) => Self::Ref(value),
            Self::Arc(value) => Self::Arc(Arc::clone(value)),
        }
    }
}

impl GaugeShadow {
    fn increment(&self, value: f64) {
        match self {
            Self::Noop => {}
            Self::Ref(atomic) => {
                atomic.fetch_add(value, Ordering::Relaxed);
            }
            Self::Arc(atomic) => {
                atomic.fetch_add(value, Ordering::Relaxed);
            }
        }
    }

    fn decrement(&self, value: f64) {
        match self {
            Self::Noop => {}
            Self::Ref(atomic) => {
                atomic.fetch_sub(value, Ordering::Relaxed);
            }
            Self::Arc(atomic) => {
                atomic.fetch_sub(value, Ordering::Relaxed);
            }
        }
    }

    fn set(&self, value: f64) {
        match self {
            Self::Noop => {}
            Self::Ref(atomic) => atomic.store(value, Ordering::Relaxed),
            Self::Arc(atomic) => atomic.store(value, Ordering::Relaxed),
        }
    }

    fn get(&self) -> f64 {
        match self {
            Self::Noop => f64::NAN,
            Self::Ref(atomic) => atomic.load(Ordering::Relaxed),
            Self::Arc(atomic) => atomic.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct Gauge {
    pub(crate) info: &'static MetricInfo,
    pub(crate) key: metrics::Key,
    pub(crate) inner: metrics::Gauge,
    pub(crate) shadow: GaugeShadow,
}

impl std::fmt::Debug for Gauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gauge").field("key", &self.key).finish()
    }
}

impl Gauge {
    #[doc(hidden)]
    pub fn __new(
        info: &'static MetricInfo,
        key: metrics::Key,
        inner: metrics::Gauge,
        shadow: GaugeShadow,
    ) -> Self {
        Self {
            info,
            key,
            inner,
            shadow,
        }
    }

    #[doc(hidden)]
    pub const fn __info(&self) -> &'static MetricInfo {
        self.info
    }

    pub const fn key(&self) -> &metrics::Key {
        &self.key
    }

    pub fn increment(&self, value: f64) {
        self.shadow.increment(value);
        self.inner.increment(value);
    }

    pub fn decrement(&self, value: f64) {
        self.shadow.decrement(value);
        self.inner.decrement(value);
    }

    pub fn set(&self, value: f64) {
        self.shadow.set(value);
        self.inner.set(value);
    }

    pub fn get(&self) -> f64 {
        self.shadow.get()
    }
}

impl GaugeFn for Gauge {
    fn increment(&self, value: f64) {
        Self::increment(self, value);
    }

    fn decrement(&self, value: f64) {
        Self::decrement(self, value);
    }

    fn set(&self, value: f64) {
        Self::set(self, value);
    }
}

#[derive(Debug)]
pub struct GaugeGuard {
    gauge: Gauge,
    delta: f64,
}

impl GaugeGuard {
    pub fn from_gauge(gauge: &Gauge) -> Self {
        Self {
            gauge: gauge.clone(),
            delta: 0.0,
        }
    }

    pub fn increment(gauge: &Gauge, value: f64) -> Self {
        let mut guard = Self::from_gauge(gauge);
        guard.add_f64(value);
        guard
    }

    pub fn add(&mut self, delta: i64) {
        self.add_f64(delta as f64);
    }

    pub fn sub(&mut self, delta: i64) {
        self.sub_f64(delta as f64);
    }

    pub fn add_f64(&mut self, delta: f64) {
        self.gauge.increment(delta);
        self.delta += delta;
    }

    pub fn sub_f64(&mut self, delta: f64) {
        self.gauge.decrement(delta);
        self.delta -= delta;
    }

    pub fn get(&self) -> i64 {
        self.delta as i64
    }

    pub fn value(&self) -> f64 {
        self.delta
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.gauge.decrement(self.delta);
    }
}

#[macro_export]
macro_rules! gauge {
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: ""
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Gauge,
            observable: false,
            name: $name,
            description: $description,
            subsystem: ""
            $(, $label => $value)*
        );
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_gauge(&KEY, &METADATA)
        });
        $crate::metrics::Gauge::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::GaugeShadow::Noop,
        )
    }};

    (
        name: $name:literal,
        description: $description:literal,
        subsystem: "",
        observable: true
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Gauge,
            observable: true,
            name: $name,
            description: $description,
            subsystem: ""
            $(, $label => $value)*
        );
        static SHADOW: $crate::metrics::__atomic_float::AtomicF64 =
            $crate::metrics::__atomic_float::AtomicF64::new(0.0);
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_gauge(&KEY, &METADATA)
        });
        $crate::metrics::Gauge::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::GaugeShadow::Ref(&SHADOW),
        )
    }};

    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:literal
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Gauge,
            observable: false,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_gauge(&KEY, &METADATA)
        });
        $crate::metrics::Gauge::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::GaugeShadow::Noop,
        )
    }};

    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:literal,
        observable: true
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Gauge,
            observable: true,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );
        static SHADOW: $crate::metrics::__atomic_float::AtomicF64 =
            $crate::metrics::__atomic_float::AtomicF64::new(0.0);
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_gauge(&KEY, &METADATA)
        });
        $crate::metrics::Gauge::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::GaugeShadow::Ref(&SHADOW),
        )
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
        let key = $crate::metrics::__metrics::Key::from_parts(info.key_name, labels);
        let metadata = $crate::metadata!(info.subsystem);

        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_gauge(&key, &metadata)
        });

        let shadow = if info.observable {
            $crate::metrics::GaugeShadow::Arc(std::sync::Arc::new(
                $crate::metrics::__atomic_float::AtomicF64::new(0.0),
            ))
        } else {
            $crate::metrics::GaugeShadow::Noop
        };

        $crate::metrics::Gauge::__new(info, key, inner, shadow)
    }};
}
