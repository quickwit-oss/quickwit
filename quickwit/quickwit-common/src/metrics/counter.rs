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
use std::sync::atomic::{AtomicU64, Ordering};

use metrics::CounterFn;

use super::MetricInfo;

#[doc(hidden)]
pub enum CounterShadow {
    Noop,
    Ref(&'static AtomicU64),
    Arc(Arc<AtomicU64>),
}

impl Clone for CounterShadow {
    fn clone(&self) -> Self {
        match self {
            Self::Noop => Self::Noop,
            Self::Ref(value) => Self::Ref(value),
            Self::Arc(value) => Self::Arc(Arc::clone(value)),
        }
    }
}

impl CounterShadow {
    fn increment(&self, value: u64) {
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

    fn absolute(&self, value: u64) {
        match self {
            Self::Noop => {}
            Self::Ref(atomic) => atomic.store(value, Ordering::Relaxed),
            Self::Arc(atomic) => atomic.store(value, Ordering::Relaxed),
        }
    }

    fn get(&self) -> u64 {
        match self {
            Self::Noop => u64::MAX,
            Self::Ref(atomic) => atomic.load(Ordering::Relaxed),
            Self::Arc(atomic) => atomic.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct Counter {
    pub(crate) info: &'static MetricInfo,
    pub(crate) key: metrics::Key,
    pub(crate) inner: metrics::Counter,
    pub(crate) shadow: CounterShadow,
}

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counter").field("key", &self.key).finish()
    }
}

impl Counter {
    #[doc(hidden)]
    pub fn __new(
        info: &'static MetricInfo,
        key: metrics::Key,
        inner: metrics::Counter,
        shadow: CounterShadow,
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

    pub fn increment(&self, value: u64) {
        self.shadow.increment(value);
        self.inner.increment(value);
    }

    pub fn absolute(&self, value: u64) {
        self.shadow.absolute(value);
        self.inner.absolute(value);
    }

    pub fn get(&self) -> u64 {
        self.shadow.get()
    }
}

impl CounterFn for Counter {
    fn increment(&self, value: u64) {
        Self::increment(self, value);
    }

    fn absolute(&self, value: u64) {
        Self::absolute(self, value);
    }
}

#[macro_export]
macro_rules! counter {
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: ""
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Counter,
            observable: false,
            name: $name,
            description: $description,
            subsystem: ""
            $(, $label => $value)*
        );
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_counter(&KEY, &METADATA)
        });
        $crate::metrics::Counter::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::CounterShadow::Noop,
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
            kind: $crate::metrics::MetricKind::Counter,
            observable: true,
            name: $name,
            description: $description,
            subsystem: ""
            $(, $label => $value)*
        );
        static SHADOW: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_counter(&KEY, &METADATA)
        });
        $crate::metrics::Counter::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::CounterShadow::Ref(&SHADOW),
        )
    }};

    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:literal
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::key_info_metadata!(
            kind: $crate::metrics::MetricKind::Counter,
            observable: false,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_counter(&KEY, &METADATA)
        });
        $crate::metrics::Counter::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::CounterShadow::Noop,
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
            kind: $crate::metrics::MetricKind::Counter,
            observable: true,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );
        static SHADOW: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let inner = $crate::metrics::__metrics::with_recorder(|recorder| {
            recorder.register_counter(&KEY, &METADATA)
        });
        $crate::metrics::Counter::__new(
            &INFO,
            KEY.clone(),
            inner,
            $crate::metrics::CounterShadow::Ref(&SHADOW),
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
            recorder.register_counter(&key, &metadata)
        });

        let shadow = if info.observable {
            $crate::metrics::CounterShadow::Arc(std::sync::Arc::new(
                std::sync::atomic::AtomicU64::new(0),
            ))
        } else {
            $crate::metrics::CounterShadow::Noop
        };

        $crate::metrics::Counter::__new(info, key, inner, shadow)
    }};
}
