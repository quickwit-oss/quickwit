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

//! DDSketch plumbing: build Vector `AgentDDSketch` values from agent-embedded
//! `ddsketch_full` protobufs and from single-sample latency values.
//!
//! The agent embeds DDSketch values inside `bytes` fields (e.g.
//! `HTTPStats.Latencies`) using the sketches-go `ddsketch_full` wire format.
//! This module mirrors the Go sidecar's sketch handling:
//!
//! * [`decode_proto`] reads those bytes and reconstructs a Vector `AgentDDSketch` with the same bin
//!   keys the agent shipped. The top-level `count/min/max/sum/avg` scalars are computed from the
//!   bins using sketches-go's `LogarithmicMapping.Value(k)` formula (the "bin midpoint"), matching
//!   what sketches-go's `FromProto` + `GetSum/GetMinValue/...` would produce on the same input.
//! * [`from_single_sample`] builds the sketch for the single-sample optimisation the agent uses
//!   when `hits == 1` and it stores the raw latency in `FirstLatencySample` rather than in
//!   `Latencies` bytes. The Go sidecar uses a sketch with `sketchAccuracy = 0.01` for this path; we
//!   replicate that exactly so the output bins match.
//!
//! # Key-space note
//!
//! sketches-go uses `gamma = (1 + ra) / (1 - ra)` and `indexOffset = 0`.
//! Vector's internal `Config::default()` uses a different `gamma_v` and a
//! non-zero `norm_bias`. The bin keys for the same value therefore differ
//! between the two. We always use `AgentDDSketch::from_raw`, which bypasses
//! Vector's `Config::key()` remapping and stores bins as-is — so the keys
//! shipped by the agent (or computed by sketches-go's `Index(v)`) survive
//! pass-through to the downstream sink exactly.

use prost::Message;
use tracing::warn;
use vector_lib::metrics::AgentDDSketch;

use crate::protos::sketch::DdSketch;

/// Accuracy for single-sample sketches, matching the Go sidecar's
/// `sketchAccuracy` constant at
/// `internal/emitter/vector.go` (value `0.01`).
const SINGLE_SAMPLE_RELATIVE_ACCURACY: f64 = 0.01;

/// Decodes an agent-embedded `ddsketch_full` proto payload into a Vector
/// `AgentDDSketch`. Returns `None` if the proto is invalid, empty, or
/// carries no mapping.
pub(super) fn decode_proto(bytes: &[u8]) -> Option<AgentDDSketch> {
    let proto = DdSketch::decode(bytes).ok()?;

    let mapping = proto.mapping?;
    let gamma = mapping.gamma;
    if !gamma.is_finite() || gamma <= 1.0 {
        warn!(gamma, "invalid ddsketch mapping gamma");
        return None;
    }
    let index_offset = mapping.index_offset;

    // Collect (key, count) pairs from both stores. Negative-store keys are
    // emitted as `-k` so they live in the same i16 index space.
    let mut positive = collect_store_pairs(proto.positive_values.as_ref());
    let mut negative = collect_store_pairs(proto.negative_values.as_ref());
    for (k, _) in &mut negative {
        *k = -*k;
    }

    // Sort by key ascending so `sum` accumulates in deterministic order and
    // the resulting `BinMap` satisfies the ordering `from_bins` expects.
    positive.extend(negative);
    positive.sort_by_key(|(k, _)| *k);
    let pairs = positive;

    if pairs.is_empty() && proto.zero_count <= 0.0 {
        return None;
    }

    let mut bins_saturated: u32 = 0;
    let mut bins_fractional: u32 = 0;

    // Build the surviving (key, count) list first, discarding bins that
    // round to zero. Compute top-level scalars in the same pass so we don't
    // need to iterate twice.
    let zero_contrib = proto.zero_count.max(0.0).round();
    let mut keys: Vec<i16> = Vec::with_capacity(pairs.len());
    let mut counts: Vec<u16> = Vec::with_capacity(pairs.len());
    let mut count_u32: u32 = 0;
    let mut sum: f64 = 0.0;
    let mut min_value: Option<f64> = None;
    let mut max_value: Option<f64> = None;

    for (k_i32, count_f) in &pairs {
        let k = saturate_key(*k_i32);
        let rounded = count_f.round();
        if rounded != *count_f {
            bins_fractional = bins_fractional.saturating_add(1);
        }
        let n = if rounded >= f64::from(u16::MAX) {
            bins_saturated = bins_saturated.saturating_add(1);
            u16::MAX
        } else if rounded <= 0.0 {
            continue;
        } else {
            rounded as u16
        };
        keys.push(k);
        counts.push(n);

        let v = value_from_key(gamma, index_offset, *k_i32);
        sum += v * f64::from(n);
        count_u32 = count_u32.saturating_add(u32::from(n));
        match min_value {
            None => min_value = Some(v),
            Some(cur) if v < cur => min_value = Some(v),
            _ => {}
        }
        match max_value {
            None => max_value = Some(v),
            Some(cur) if v > cur => max_value = Some(v),
            _ => {}
        }
    }

    if bins_saturated > 0 {
        warn!(bins_saturated, "ddsketch bin count saturated u16");
    }
    if bins_fractional > 0 {
        warn!(bins_fractional, "ddsketch bin count had fractional value");
    }

    if zero_contrib > 0.0 {
        count_u32 = count_u32.saturating_add(zero_contrib as u32);
        // Zero values contribute 0 to sum. min/max become 0 if no other
        // value was populated.
        min_value = Some(min_value.map(|m| m.min(0.0)).unwrap_or(0.0));
        max_value = Some(max_value.map(|m| m.max(0.0)).unwrap_or(0.0));
    }

    if count_u32 == 0 {
        return None;
    }

    let min = min_value.unwrap_or(0.0);
    let max = max_value.unwrap_or(0.0);
    let avg = sum / f64::from(count_u32);

    AgentDDSketch::from_raw(count_u32, min, max, sum, avg, &keys, &counts)
}

/// Builds an `AgentDDSketch` representing a single sample, matching the Go
/// sidecar's `decodeOrCreateSketch` behaviour for the `count == 1` path
/// (`ddsketch.LogUnboundedDenseDDSketch(sketchAccuracy).Add(v)`).
pub(super) fn from_single_sample(v: f64) -> Option<AgentDDSketch> {
    if !v.is_finite() || v <= 0.0 {
        return None;
    }

    let ra = SINGLE_SAMPLE_RELATIVE_ACCURACY;
    let gamma = (1.0 + ra) / (1.0 - ra);
    let multiplier = 1.0 / gamma.ln();

    let index_f = v.ln() * multiplier;
    let key_i32: i32 = if index_f >= 0.0 {
        index_f as i32
    } else {
        (index_f as i32) - 1
    };
    let key = saturate_key(key_i32);

    // sketches-go's GetSum/GetMin/GetMax return `mapping.Value(index) * count`,
    // NOT the raw input value — see `ddsketch.go:GetSum` which sums
    // `value * count` over bins, where `value = mapping.Value(index)`.
    let midpoint = value_from_key(gamma, 0.0, key_i32);

    AgentDDSketch::from_raw(1, midpoint, midpoint, midpoint, midpoint, &[key], &[1])
}

/// Expands a sketches-go `Store` into `(key, count)` pairs, summing both the
/// sparse and contiguous encodings. Returned `key` is the raw sketches-go
/// `int32` index.
fn collect_store_pairs(store: Option<&crate::protos::sketch::Store>) -> Vec<(i32, f64)> {
    let Some(store) = store else {
        return Vec::new();
    };
    let mut by_key: std::collections::HashMap<i32, f64> =
        std::collections::HashMap::with_capacity(store.bin_counts.len());
    for (k, c) in &store.bin_counts {
        let entry = by_key.entry(*k).or_insert(0.0);
        *entry += *c;
    }
    let offset = store.contiguous_bin_index_offset;
    for (i, c) in store.contiguous_bin_counts.iter().enumerate() {
        if *c == 0.0 {
            continue;
        }
        let k = offset.saturating_add(i as i32);
        let entry = by_key.entry(k).or_insert(0.0);
        *entry += *c;
    }
    let mut out: Vec<(i32, f64)> = by_key.into_iter().collect();
    out.sort_by_key(|(k, _)| *k);
    out
}

/// Saturating cast of a sketches-go `int32` index to Vector's `i16` bin key.
/// In practice keys are bounded by `bin_limit = 4096` so the saturation is
/// defensive.
fn saturate_key(k: i32) -> i16 {
    k.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16
}

/// sketches-go `LogarithmicMapping.Value(index)`:
///
/// ```text
/// Value(k) = LowerBound(k) * (1 + relativeAccuracy)
/// LowerBound(k) = exp((k - indexOffset) / multiplier)
/// multiplier = 1 / ln(gamma)
/// relativeAccuracy = 1 - 2 / (1 + gamma)
/// ```
fn value_from_key(gamma: f64, index_offset: f64, k: i32) -> f64 {
    let relative_accuracy = 1.0 - 2.0 / (1.0 + gamma);
    let exponent = (f64::from(k) - index_offset) * gamma.ln();
    exponent.exp() * (1.0 + relative_accuracy)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protos::sketch::index_mapping::Interpolation;
    use crate::protos::sketch::{DdSketch, IndexMapping, Store};

    fn default_mapping() -> IndexMapping {
        // sketches-go NewLogarithmicMapping(1/128) (agent's proto-shipped default).
        let ra = 1.0 / 128.0_f64;
        let gamma = (1.0 + ra) / (1.0 - ra);
        IndexMapping {
            gamma,
            index_offset: 0.0,
            interpolation: Interpolation::None as i32,
        }
    }

    fn encode_sketch(sk: &DdSketch) -> Vec<u8> {
        let mut buf = Vec::new();
        sk.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn empty_bytes_decode_to_default_with_no_mapping_returns_none() {
        assert!(decode_proto(&[]).is_none());
    }

    #[test]
    fn missing_mapping_returns_none() {
        let sk = DdSketch {
            mapping: None,
            ..Default::default()
        };
        assert!(decode_proto(&encode_sketch(&sk)).is_none());
    }

    #[test]
    fn invalid_gamma_returns_none() {
        let sk = DdSketch {
            mapping: Some(IndexMapping {
                gamma: 0.5,
                index_offset: 0.0,
                interpolation: 0,
            }),
            ..Default::default()
        };
        assert!(decode_proto(&encode_sketch(&sk)).is_none());
    }

    #[test]
    fn sparse_only_store_roundtrips_bins() {
        let mut bin_counts = HashMap::new();
        bin_counts.insert(5_i32, 3.0);
        bin_counts.insert(7_i32, 2.0);
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: Some(Store {
                bin_counts,
                contiguous_bin_counts: Vec::new(),
                contiguous_bin_index_offset: 0,
            }),
            negative_values: None,
            zero_count: 0.0,
        };
        let decoded = decode_proto(&encode_sketch(&sk)).expect("decode");
        let bins = decoded.bins();
        assert_eq!(bins.len(), 2);
        let map = decoded.bin_map();
        assert_eq!(map.keys, vec![5, 7]);
        assert_eq!(map.counts, vec![3, 2]);
        assert_eq!(decoded.count(), 5);
    }

    #[test]
    fn contiguous_only_store_reconstructs_keys() {
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: Some(Store {
                bin_counts: HashMap::new(),
                contiguous_bin_counts: vec![1.0, 0.0, 4.0],
                contiguous_bin_index_offset: 10,
            }),
            negative_values: None,
            zero_count: 0.0,
        };
        let decoded = decode_proto(&encode_sketch(&sk)).expect("decode");
        let map = decoded.bin_map();
        assert_eq!(map.keys, vec![10, 12]);
        assert_eq!(map.counts, vec![1, 4]);
        assert_eq!(decoded.count(), 5);
    }

    #[test]
    fn both_encodings_sum_counts_per_key() {
        let mut bin_counts = HashMap::new();
        bin_counts.insert(5_i32, 3.0);
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: Some(Store {
                bin_counts,
                contiguous_bin_counts: vec![2.0],
                contiguous_bin_index_offset: 5,
            }),
            negative_values: None,
            zero_count: 0.0,
        };
        let decoded = decode_proto(&encode_sketch(&sk)).expect("decode");
        let map = decoded.bin_map();
        assert_eq!(map.keys, vec![5]);
        assert_eq!(map.counts, vec![5]);
    }

    #[test]
    fn zero_count_contributes_to_count_not_bins() {
        let mut bin_counts = HashMap::new();
        bin_counts.insert(3_i32, 1.0);
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: Some(Store {
                bin_counts,
                contiguous_bin_counts: Vec::new(),
                contiguous_bin_index_offset: 0,
            }),
            negative_values: None,
            zero_count: 7.0,
        };
        let decoded = decode_proto(&encode_sketch(&sk)).expect("decode");
        assert_eq!(decoded.count(), 8);
        assert_eq!(decoded.bins().len(), 1);
    }

    #[test]
    fn empty_returns_none() {
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: None,
            negative_values: None,
            zero_count: 0.0,
        };
        assert!(decode_proto(&encode_sketch(&sk)).is_none());
    }

    #[test]
    fn u16_saturation_still_decodes() {
        let mut bin_counts = HashMap::new();
        bin_counts.insert(5_i32, 70_000.0);
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: Some(Store {
                bin_counts,
                contiguous_bin_counts: Vec::new(),
                contiguous_bin_index_offset: 0,
            }),
            negative_values: None,
            zero_count: 0.0,
        };
        let decoded = decode_proto(&encode_sketch(&sk)).expect("decode");
        let map = decoded.bin_map();
        assert_eq!(map.counts, vec![u16::MAX]);
    }

    #[test]
    fn fractional_rounds_half_away_from_zero() {
        let mut bin_counts = HashMap::new();
        bin_counts.insert(5_i32, 0.5);
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: Some(Store {
                bin_counts,
                contiguous_bin_counts: Vec::new(),
                contiguous_bin_index_offset: 0,
            }),
            negative_values: None,
            zero_count: 0.0,
        };
        let decoded = decode_proto(&encode_sketch(&sk)).expect("decode");
        let map = decoded.bin_map();
        assert_eq!(map.counts, vec![1]);
    }

    #[test]
    fn fractional_below_half_rounds_down_and_drops_bin() {
        let mut bin_counts = HashMap::new();
        bin_counts.insert(5_i32, 0.4);
        let sk = DdSketch {
            mapping: Some(default_mapping()),
            positive_values: Some(Store {
                bin_counts,
                contiguous_bin_counts: Vec::new(),
                contiguous_bin_index_offset: 0,
            }),
            negative_values: None,
            zero_count: 0.0,
        };
        // The bin has count=0 after rounding and no other content → None.
        assert!(decode_proto(&encode_sketch(&sk)).is_none());
    }

    #[test]
    fn from_single_sample_inserts_one() {
        let sk = from_single_sample(12.5).expect("sketch");
        assert_eq!(sk.count(), 1);
        assert_eq!(sk.bin_map().counts, vec![1]);
    }

    #[test]
    fn from_single_sample_matches_go_key_space() {
        // Expected key: sketches-go's LogarithmicMapping(0.01).Index(0.1)
        //   gamma = 1.01 / 0.99
        //   multiplier = 1 / ln(gamma)
        //   index = floor(ln(0.1) * multiplier)
        let ra = 0.01_f64;
        let gamma = (1.0 + ra) / (1.0 - ra);
        let multiplier = 1.0 / gamma.ln();
        let index_f = 0.1_f64.ln() * multiplier;
        let expected_key = if index_f >= 0.0 {
            index_f as i32
        } else {
            index_f as i32 - 1
        } as i16;
        let sk = from_single_sample(0.1).expect("sketch");
        assert_eq!(sk.bin_map().keys, vec![expected_key]);
    }

    #[test]
    fn from_single_sample_rejects_non_positive_or_nan() {
        assert!(from_single_sample(0.0).is_none());
        assert!(from_single_sample(-1.0).is_none());
        assert!(from_single_sample(f64::NAN).is_none());
        assert!(from_single_sample(f64::INFINITY).is_none());
    }

    #[test]
    fn value_matches_sketches_go_formula() {
        // Hand-computed: gamma = 129/127, indexOffset = 0, k = 100
        // lower_bound = gamma^100
        // value = lower_bound * (1 + ra), ra = 1 - 2/(1+gamma) = 1/128
        let gamma: f64 = 129.0 / 127.0;
        let ra = 1.0 - 2.0 / (1.0 + gamma);
        let expected = gamma.powi(100) * (1.0 + ra);
        let actual = value_from_key(gamma, 0.0, 100);
        let rel_err = ((actual - expected) / expected).abs();
        assert!(rel_err < 1e-12, "rel_err = {rel_err}");
    }
}
