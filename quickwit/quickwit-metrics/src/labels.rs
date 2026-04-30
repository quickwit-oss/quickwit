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
//! [`Labels<N>`] holds label *names* at compile time; pair them with
//! values via [`Labels::with_values`] to get a [`LabelValues<N>`] that
//! the `labels:` macro arm can consume. This avoids repeating the same
//! label names at every call site and lets a single `LabelValues<N>` be
//! shared across counter, gauge, and histogram extensions.

use crate::__key_hash;

/// A label-name template with a fixed number of slots.
///
/// `Labels<N>` holds only the label *names* — it is `const`-constructible
/// and carries no runtime data. Call [`with_values`](Self::with_values) to
/// pair the names with concrete values, producing a [`LabelValues<N>`] that
/// the metric macros can consume.
///
/// # Example
///
/// ```rust,ignore
/// const SPLIT_LABELS: Labels<2> = Labels::new(["source", "level"]);
///
/// // &'static str values — zero allocation (Cow::Borrowed).
/// let lv = SPLIT_LABELS.with_values(["prod", "info"]);
///
/// // Runtime String values — allocates (Cow::Owned).
/// let lv = SPLIT_LABELS.with_values([source_uid, level.to_string()]);
///
/// // Pass by reference — reuse the same LabelValues across metrics.
/// let c = counter!(parent: BASE_COUNTER, labels: &lv);
/// let g = gauge!(parent: BASE_GAUGE, labels: &lv);
/// ```
pub struct Labels<const N: usize> {
    names: [&'static str; N],
}

impl<const N: usize> Labels<N> {
    /// Creates a label template from an array of label names.
    pub const fn new(names: [&'static str; N]) -> Self {
        Self { names }
    }

    /// Pairs this template's names with values, returning a
    /// [`LabelValues<N>`] ready to be passed to a metric macro.
    ///
    /// Each value can be anything that converts to [`metrics::SharedString`]
    /// (`Cow<'static, str>`): `&'static str` is zero-alloc, `String` or
    /// non-`'static` `&str` allocates.
    pub fn with_values<V: Into<metrics::SharedString>>(&self, values: [V; N]) -> LabelValues<N> {
        LabelValues {
            names: self.names,
            values: values.map(Into::into),
        }
    }
}

/// Concrete label names + values produced by [`Labels::with_values`].
///
/// Passed by reference (`&LabelValues<N>`) to the `labels:` macro arm so
/// a single instance can be reused across multiple metric calls. Cloning
/// of the inner `SharedString` values only happens on the cold path
/// (global DashMap miss).
#[derive(Clone)]
pub struct LabelValues<const N: usize> {
    names: [&'static str; N],
    values: [metrics::SharedString; N],
}

impl<const N: usize> LabelValues<N> {
    /// Computes the order-independent cache-key hash, seeded with a
    /// parent hash so the result is fully composable:
    /// `hash(parent, [A,B]) == __key_hash(parent, [A,B])`.
    #[doc(hidden)]
    pub fn hash(&self, seed: u64) -> u64 {
        __key_hash(
            seed,
            self.names
                .iter()
                .zip(self.values.iter())
                .map(|(n, v)| (*n, v.as_ref())),
        )
    }

    /// Builds `metrics::Label`s by cloning the stored names and values.
    /// Only called on the cold path (global DashMap miss).
    #[doc(hidden)]
    pub fn to_labels(&self) -> impl Iterator<Item = metrics::Label> + '_ {
        self.names
            .iter()
            .zip(self.values.iter())
            .map(|(n, v)| metrics::Label::new(*n, v.clone()))
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
