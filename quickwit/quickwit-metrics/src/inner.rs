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

//! Internal helpers re-exported for macro expansion.
//!
//! Everything in this module is `#[doc(hidden)]` — it exists only so that
//! the `counter!`, `gauge!`, and `histogram!` macros can reference these
//! items via `$crate::`. **Do not use directly.**

use std::hash::{Hash, Hasher};

#[doc(hidden)]
pub use const_format::concatcp as __concatcp;
use rustc_hash::FxHasher;

// ─── Helper macros ───

/// Builds the fully-qualified metric key name at compile time:
/// `"{SYSTEM}_{name}"` or `"{SYSTEM}_{subsystem}_{name}"`.
#[doc(hidden)]
#[macro_export]
macro_rules! __key_name {
    ("", $name:literal) => {
        $crate::__concatcp!($crate::SYSTEM, "_", $name)
    };
    ($subsystem:literal, $name:literal) => {
        $crate::__concatcp!($crate::SYSTEM, "_", $subsystem, "_", $name)
    };
}

/// Counts the number of token-tree arguments at compile time.
#[doc(hidden)]
#[macro_export]
macro_rules! __count {
    () => {
        0usize
    };
    ($head:tt $($tail:tt)*) => {
        1usize + $crate::__count!($($tail)*)
    };
}

/// Constructs a `metrics::Metadata` with the subsystem, INFO level,
/// and the caller's module path.
#[doc(hidden)]
#[macro_export]
macro_rules! __metadata {
    ($subsystem:expr) => {
        $crate::__metrics::Metadata::new(
            $subsystem,
            $crate::__metrics::Level::INFO,
            Some(module_path!()),
        )
    };
}

/// Declares the compile-time statics that every metric declaration arm needs:
/// `KEY_NAME`, `INFO`, `KEY`, `LABELS`, and `METADATA`.
/// Also registers `INFO` with the `inventory` crate for runtime discovery.
#[doc(hidden)]
#[macro_export]
macro_rules! __key_info_metadata {
    (
        kind: $kind:expr,
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:tt
        $(, $label:literal => $value:literal)* $(,)?
    ) => {
        const KEY_NAME: &str = $crate::__key_name!($subsystem, $name);
        static METADATA: $crate::__metrics::Metadata<'static> = $crate::__metadata!($subsystem);
        static INFO: $crate::MetricInfo = $crate::MetricInfo {
            key_name: KEY_NAME,
            description: $description,
            kind: $kind,
            metadata: &METADATA,
            static_labels: &[$(($label, $value)),*],
        };
        $crate::__inventory::submit!(INFO);

        static LABELS: [$crate::__metrics::Label; $crate::__count!($($label)*)] = [
            $($crate::__metrics::Label::from_static_parts($label, $value)),*
        ];
        static KEY: $crate::__metrics::Key = $crate::__metrics::Key::from_static_parts(KEY_NAME, &LABELS);
    };
}

// ─── Cache-key hashing ───
//
// We need a hash that can be computed **incrementally**: the final hash
// must equal the hash of the metric name combined with the hashes of all
// labels, regardless of the order they are declared or added. This is
// critical for the `parent:` extension pattern, where a child metric
// inherits its parent's hash and folds in only the new labels — the
// result must be identical to hashing everything from scratch.
//
// This requires the combining operator to be **commutative**
// (order-independent) and **associative** (composable):
//   `combine(combine(seed, A), B) == combine(seed, combine(A, B))`
//
// We use wrapping addition (mod 2^64) rather than XOR because XOR is
// self-inverse — duplicate labels would cancel each other out
// (`a ^ a == 0`).

/// Folds per-label hashes into `seed` via wrapping addition (mod 2^64).
///
/// The combining operator (wrapping add) is both **commutative** and
/// **associative**, which guarantees order-independence and composability:
/// `hash(seed, [A,B]) == hash(hash(seed, [A]), [B])`.
#[doc(hidden)]
#[inline]
pub fn __key_hash<'a>(seed: u64, labels: impl IntoIterator<Item = (&'a str, &'a str)>) -> u64 {
    let mut acc = seed;
    for (name, value) in labels {
        let mut h = FxHasher::default();
        name.hash(&mut h);
        value.hash(&mut h);
        acc = acc.wrapping_add(h.finish());
    }
    acc
}

/// Convenience macro that coerces label name/value expressions into `&str`
/// and delegates to [`__key_hash`].
#[doc(hidden)]
#[macro_export]
macro_rules! __key_hash {
    ($start:expr, $(($label:expr, $value:expr)),+ $(,)?) => {
        $crate::__key_hash($start, [$((&*$label, &*$value)),+])
    };
}

// ─── Metric declaration helper ───
//
// Shared implementation for the metric declaration arm of counter!, gauge!,
// and histogram! (name + description + subsystem + observable + static labels).
// The caller is responsible for declaring `__key_info_metadata!` statics first
// (and, for histograms, the `HistogramConfig` + `inventory::submit!`).

/// Provides per-call-site caching (thread-local) backed by a global `DashMap`
/// for the initial registration. Returns a cloned `Arc`-wrapped metric handle.
///
/// On the first call the metric is looked up or registered in the global map;
/// subsequent calls on the same thread return the cached clone in O(1).
#[doc(hidden)]
#[macro_export]
macro_rules! __metric_declaration {
    (
        metric_type: $metric_type:ty,
        register_fn: $register_fn:path,
        metric_info: $metric_info:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        // Thread-local single-slot cache — avoids DashMap lookups on hot paths.
        thread_local! {
            static CACHE: std::cell::RefCell<Option<$metric_type>> = const {
                std::cell::RefCell::new(None)
            };
        }
        CACHE.with(|cell| {
            let mut slot = cell.borrow_mut();
            // Fast path: already cached on this thread — return the clone.
            if let Some(ref cached) = *slot {
                return cached.clone();
            }

            // Slow path: building the full metrics::Key is expensive, so we
            // compute only the cheap hash first and probe the global DashMap.
            // The closure that constructs the Key is called only on a cache miss.
            let hash = $crate::__key_hash!(0, ("", KEY_NAME) $(, ($label, $value))*);
            let metric = $register_fn(hash, || {
                ($metric_info, KEY.clone(), METADATA.clone())
            });
            *slot = Some(metric.clone());
            metric
        })
    }};
}

// ─── Metric extension helper ───
//
// Shared implementation for both metric extension arms (Labels and
// inline labels). Each metric macro pre-computes the hash and passes the
// label-building logic as expression fragments.

/// Derives a child metric from a `parent`, inheriting its key name and labels,
/// then appending additional dynamic labels supplied by the caller.
///
/// Caching is two-level:
///   1. A **thread-local** `(hash, metric)` pair — cache hit when the same call site is re-invoked
///      with identical label values (common case).
///   2. A **global `DashMap`** keyed by hash — first-time registration or cross-thread
///      deduplication.
#[doc(hidden)]
#[macro_export]
macro_rules! __metric_extension {
    (
        metric_type: $metric_type:ty,
        register_fn: $register_fn:path,
        parent: $parent:expr,
        metric_info: $metric_info:expr,
        hash: $hash:expr,
        label_count: $label_count:expr,
        labels_iter: $labels_iter:expr
    ) => {{
        // Thread-local (hash, metric) pair — keyed by hash because dynamic
        // labels may change between invocations of the same call site.
        thread_local! {
            static CACHE: std::cell::RefCell<Option<(u64, $metric_type)>> = const {
                std::cell::RefCell::new(None)
            };
        }
        CACHE.with(|cell| {
            let mut slot = cell.borrow_mut();
            let hash = $hash;
            // Fast path: same labels as last call on this thread.
            if let Some((h, ref cached)) = *slot {
                if h == hash {
                    return cached.clone();
                }
            }

            // Slow path: building the full metrics::Key is expensive, so we
            // compute only the cheap hash first and probe the global DashMap.
            // The closure that constructs the Key is called only on a cache miss.
            let metric = $register_fn(hash, || {
                let parent_key = $parent.key();
                let mut all_labels = Vec::with_capacity(parent_key.labels().len() + $label_count);
                all_labels.extend(parent_key.labels().cloned());
                all_labels.extend($labels_iter);

                let mi = $metric_info;
                let key = $crate::__metrics::Key::from_parts(mi.key_name, all_labels);

                ($parent.__info(), key, mi.metadata.clone())
            });
            *slot = Some((hash, metric.clone()));
            metric
        })
    }};
}

/// Recursive tt-muncher that binds each `Labels<N>` expression exactly once,
/// then folds hash, count, and iterator chain across all of them before
/// delegating to [`__metric_extension!`].
///
/// Each recursion step creates a nested scope so that earlier bindings remain
/// live when the base case finally emits the extension. Zero allocation on
/// the hot path.
#[doc(hidden)]
#[macro_export]
macro_rules! __bind_labels {
    // Base case: no more labels to peel. Emit __metric_extension!.
    (
        metric_type: $metric_type:ty,
        register_fn: $register_fn:path,
        parent: $parent:expr,
        metric_info: $metric_info:expr,
        hash: $hash:expr,
        count: $count:expr,
        iter: $iter:expr,
    ) => {
        $crate::__metric_extension!(
            metric_type: $metric_type,
            register_fn: $register_fn,
            parent: $parent,
            metric_info: $metric_info,
            hash: $hash,
            label_count: $count,
            labels_iter: $iter
        )
    };

    // Recursive case: bind the next labels expr, fold into hash/count/iter,
    // then recurse with remaining labels.
    (
        metric_type: $metric_type:ty,
        register_fn: $register_fn:path,
        parent: $parent:expr,
        metric_info: $metric_info:expr,
        hash: $hash:expr,
        count: $count:expr,
        iter: $iter:expr,
        next: $next:expr $(, next: $rest:expr)* $(,)?
    ) => {{
        let __ref = &$next;
        let hash = $crate::__key_hash($hash, __ref.iter());
        let count = $count + __ref.len();
        $crate::__bind_labels!(
            metric_type: $metric_type,
            register_fn: $register_fn,
            parent: $parent,
            metric_info: $metric_info,
            hash: hash,
            count: count,
            iter: $iter.chain(__ref.__to_labels()),
            $(next: $rest,)*
        )
    }};
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::__key_hash;

    fn label_vec(max_len: usize) -> impl Strategy<Value = Vec<(String, String)>> {
        prop::collection::vec(("[a-z]{1,8}", "[a-z0-9]{1,16}"), 1..=max_len)
    }

    proptest! {
        #[test]
        fn composable_at_every_split(
            seed: u64,
            labels in label_vec(12),
        ) {
            let refs: Vec<(&str, &str)> = labels.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
            let hash_all = __key_hash(seed, refs.iter().copied());

            for split in 1..refs.len() {
                let (first, second) = refs.split_at(split);
                let hash_composed = __key_hash(__key_hash(seed, first.iter().copied()), second.iter().copied());
                prop_assert_eq!(hash_all, hash_composed,
                    "split at {}: hash(all) != composed", split);
            }
        }

        #[test]
        fn order_independent(
            seed: u64,
            mut labels in label_vec(8),
        ) {
            let refs: Vec<(&str, &str)> = labels.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
            let hash_original = __key_hash(seed, refs.iter().copied());

            labels.reverse();
            let refs_rev: Vec<(&str, &str)> = labels.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
            let hash_reversed = __key_hash(seed, refs_rev.iter().copied());

            prop_assert_eq!(hash_original, hash_reversed);
        }

        #[test]
        fn name_value_binding(
            seed: u64,
            name in "[a-z]{1,8}",
            value in "[a-z]{1,8}",
        ) {
            prop_assume!(name != value);
            let a = __key_hash(seed, [(name.as_str(), value.as_str())]);
            let b = __key_hash(seed, [(value.as_str(), name.as_str())]);
            prop_assert_ne!(a, b, "swapping name and value must produce different hashes");
        }

        #[test]
        fn seed_matters(
            seed_a: u64,
            seed_b: u64,
            labels in label_vec(4),
        ) {
            prop_assume!(seed_a != seed_b);
            let refs: Vec<(&str, &str)> = labels.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
            prop_assert_ne!(__key_hash(seed_a, refs.iter().copied()), __key_hash(seed_b, refs.iter().copied()));
        }

        #[test]
        fn empty_labels_returns_seed(seed: u64) {
            prop_assert_eq!(__key_hash(seed, std::iter::empty()), seed);
        }

        #[test]
        fn duplicate_labels_do_not_cancel(
            seed: u64,
            name in "[a-z]{1,8}",
            value in "[a-z0-9]{1,16}",
        ) {
            let one = __key_hash(seed, [(&*name, &*value)]);
            let two = __key_hash(seed, [(&*name, &*value), (&*name, &*value)]);
            prop_assert_ne!(one, seed, "single label must change the seed");
            prop_assert_ne!(two, seed, "two identical labels must not cancel back to seed");
            prop_assert_ne!(one, two, "one vs two identical labels must produce different hashes");
        }
    }
}
