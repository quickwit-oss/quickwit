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

//! Reusable label templates for metric extension.
//!
//! [`LabelNames<N>`] holds label *names* at compile time; pair them with
//! values via the [`label_values!`] macro to get a [`Labels<N>`] that
//! the `labels:` macro arm can consume. This avoids repeating the same
//! label names at every call site and lets a single `Labels<N>` be
//! shared across counter, gauge, and histogram extensions.

use metrics::SharedString;

use crate::__key_hash;

/// Creates a const [`LabelNames<N>`] from a list of label name literals.
///
/// # Example
///
/// ```rust,ignore
/// const ROUTE: LabelNames<2> = label_names!("method", "path");
/// ```
#[macro_export]
macro_rules! label_names {
    ($($name:expr),+ $(,)?) => {
        $crate::LabelNames::__new([$($name),+])
    };
}

/// Pairs a [`LabelNames<N>`] template with concrete values, one per label name.
///
/// Each value is converted individually via `Into<SharedString>`, so you
/// can freely mix `&'static str`, `String`, `Cow<'static, str>`, etc.
///
/// Use this macro when the same label names are shared across multiple
/// metrics or call sites. For single-use labels, prefer the inline
/// `"key" => value` syntax directly in the metric macro.
///
/// # Example
///
/// ```rust,ignore
/// const GC_KEYS: LabelNames<2> = label_names!("status", "split_type");
///
/// // All-static — zero allocation:
/// let lv = label_values!(GC_KEYS => "success", "tantivy");
///
/// // Mixed types — &'static str and String — just work:
/// let lv = label_values!(GC_KEYS => "success", split_type.to_string());
///
/// // Reuse the same Labels across multiple metrics:
/// counter!(parent: GC_COUNTER, labels: [lv]).increment(1);
/// gauge!(parent: GC_GAUGE, labels: [lv]).set(42.0);
/// ```
#[macro_export]
macro_rules! label_values {
    ($labels:expr => $($val:expr),+ $(,)?) => {
        $labels.__with_values([$(Into::<$crate::__metrics::SharedString>::into($val)),+])
    };
}

/// Creates a const `Labels` from all-static key-value pairs.
///
/// Every key and value must be `&'static str` literals. The result is a
/// `const` value — zero allocation, zero runtime cost.
///
/// # Example
///
/// ```rust,ignore
/// const LABELS: Labels<2> = labels!("env" => "prod", "region" => "us-east-1");
/// ```
#[macro_export]
macro_rules! labels {
    ($($key:expr => $val:expr),+ $(,)?) => {
        $crate::Labels::__from_parts(
            [$($key),+],
            [$($crate::__metrics::SharedString::from($val)),+],
        )
    };
}

/// A label-name template with a fixed number of slots.
///
/// `LabelNames<N>` holds only the label *names* — it is `const`-constructible
/// and carries no runtime data. Use the [`label_values!`] macro to pair
/// the names with concrete values, producing a [`Labels<N>`] that
/// the metric macros can consume.
///
/// # Example
///
/// ```rust,ignore
/// const SPLIT_KEYS: LabelNames<2> = label_names!("source", "level");
///
/// // All the same type:
/// let lv = label_values!(SPLIT_KEYS => "prod", "info");
///
/// // Mixed types:
/// let lv = label_values!(SPLIT_KEYS => source_uid, level.to_string());
///
/// // Reuse the same Labels across metrics:
/// let c = counter!(parent: BASE_COUNTER, labels: [lv]);
/// let g = gauge!(parent: BASE_GAUGE, labels: [lv]);
/// ```
pub struct LabelNames<const N: usize> {
    names: [&'static str; N],
}

impl<const N: usize> LabelNames<N> {
    /// Internal plumbing used by [`label_names!`]. Not part of the public API.
    #[doc(hidden)]
    pub const fn __new(names: [&'static str; N]) -> Self {
        Self { names }
    }

    /// Internal plumbing used by [`label_values!`]. Not part of the public API.
    #[doc(hidden)]
    pub fn __with_values<V: Into<metrics::SharedString>>(&self, values: [V; N]) -> Labels<N> {
        Labels {
            names: self.names,
            values: values.map(Into::into),
        }
    }
}

/// Concrete label names + values produced by [`label_values!`] or [`labels!`].
///
/// The `labels:` macro arm borrows the value internally, so a single
/// instance can be reused across multiple metric calls. Cloning of the
/// inner `SharedString` values only happens on the cold path (cache miss
/// in the thread-local or global DashMap).
#[derive(Clone)]
pub struct Labels<const N: usize> {
    names: [&'static str; N],
    values: [metrics::SharedString; N],
}

impl<const N: usize> Labels<N> {
    /// Creates a fully-static `Labels` at compile time.
    ///
    /// Prefer the [`labels!`] macro which calls this constructor and
    /// avoids repeating the array-index boilerplate.
    #[doc(hidden)]
    pub const fn __from_parts(names: [&'static str; N], values: [SharedString; N]) -> Self {
        Self { names, values }
    }

    /// Computes an order-independent cache-key hash by folding per-label
    /// hashes into `seed` via commutative wrapping addition, so the result
    /// is fully composable with the parent's hash.
    #[doc(hidden)]
    pub fn __hash(&self, seed: u64) -> u64 {
        __key_hash(seed, self.iter())
    }

    /// Builds `metrics::Label`s by cloning the stored names and values.
    /// Only called on the cold path (global DashMap miss).
    #[doc(hidden)]
    pub fn __to_labels(&self) -> impl Iterator<Item = metrics::Label> + '_ {
        self.names
            .iter()
            .zip(self.values.iter())
            .map(|(n, v)| metrics::Label::new(*n, v.clone()))
    }

    /// Returns an iterator of `(&str, &str)` name-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.names
            .iter()
            .zip(self.values.iter())
            .map(|(n, v)| (*n, v.as_ref()))
    }

    /// Number of labels.
    pub const fn len(&self) -> usize {
        N
    }

    /// Returns `true` if there are no labels.
    pub const fn is_empty(&self) -> bool {
        N == 0
    }
}
