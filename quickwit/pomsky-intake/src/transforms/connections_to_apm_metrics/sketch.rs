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
//! * [`decode_proto`] reads those bytes, computes the representative value for each sketches-go
//!   bin, and inserts those values into a Vector `AgentDDSketch`. The top-level
//!   `count/min/max/sum/avg` scalars are computed from sketches-go's `LogarithmicMapping.Value(k)`
//!   formula (the "bin midpoint"), matching what sketches-go's `FromProto` +
//!   `GetSum/GetMinValue/...` would produce on the same input.
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
//! between the two. We normalize agent-shipped sketches-go bins into Vector's
//! default key space before emission so downstream metrics storage and query
//! code see the same bin coordinate system as other `AgentDDSketch` metrics.

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
///
/// Decode failures are logged at `warn` so that a corrupt sketch doesn't
/// silently yield a hit-without-distribution metric downstream.
pub(super) fn decode_proto(bytes: &[u8]) -> Option<AgentDDSketch> {
    let proto = match DdSketch::decode(bytes) {
        Ok(proto) => proto,
        Err(err) => {
            warn!(%err, bytes = bytes.len(), "ddsketch proto decode failed, dropping sketch");
            return None;
        }
    };

    let Some(mapping) = proto.mapping else {
        warn!("ddsketch payload missing mapping, dropping sketch");
        return None;
    };
    let gamma = mapping.gamma;
    if !gamma.is_finite() || gamma <= 1.0 {
        warn!(gamma, "invalid ddsketch mapping gamma");
        return None;
    }
    let index_offset = mapping.index_offset;

    let mut warnings = DecodeWarnings::default();

    // Build a normal Vector AgentDDSketch from the surviving bins. This
    // remaps sketches-go bin keys into Vector's default AgentDDSketch key
    // space while preserving sketches-go scalar semantics by inserting each
    // bin midpoint with the bin count.
    let zero_contrib = proto.zero_count.max(0.0).round();
    let mut sketch = AgentDDSketch::with_agent_defaults();

    insert_store_bins(
        proto.positive_values.as_ref(),
        gamma,
        index_offset,
        false,
        &mut sketch,
        &mut warnings,
    );
    insert_store_bins(
        proto.negative_values.as_ref(),
        gamma,
        index_offset,
        true,
        &mut sketch,
        &mut warnings,
    );

    if warnings.bins_saturated > 0 {
        warn!(
            bins_saturated = warnings.bins_saturated,
            "ddsketch bin count saturated u16"
        );
    }
    if warnings.bins_fractional > 0 {
        warn!(
            bins_fractional = warnings.bins_fractional,
            "ddsketch bin count had fractional value"
        );
    }

    if zero_contrib > 0.0 {
        // Zero values contribute 0 to sum. Vector key 0 is the canonical zero
        // bucket for AgentDDSketch.
        sketch.insert_n(0.0, zero_contrib as u32);
    }

    if sketch.count() == 0 {
        return None;
    }
    Some(sketch)
}

#[derive(Default)]
struct DecodeWarnings {
    bins_saturated: u32,
    bins_fractional: u32,
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

    // sketches-go's GetSum/GetMin/GetMax return `mapping.Value(index) * count`,
    // NOT the raw input value — see `ddsketch.go:GetSum` which sums
    // `value * count` over bins, where `value = mapping.Value(index)`.
    let midpoint = value_from_key(gamma, 0.0, key_i32);

    let mut sketch = AgentDDSketch::with_agent_defaults();
    sketch.insert_n(midpoint, 1);
    Some(sketch)
}

/// Inserts all non-empty bins from a sketches-go `Store` into `sketch`.
///
/// A store can carry the same raw key in both sparse and contiguous encodings,
/// so sparse keys add the matching contiguous count and the contiguous pass
/// skips keys that already appeared in the sparse map. This preserves the old
/// per-raw-key rounding/saturation behavior without allocating a merge map.
fn insert_store_bins(
    store: Option<&crate::protos::sketch::Store>,
    gamma: f64,
    index_offset: f64,
    negative: bool,
    sketch: &mut AgentDDSketch,
    warnings: &mut DecodeWarnings,
) {
    let Some(store) = store else {
        return;
    };

    for (k, count) in &store.bin_counts {
        let count = *count + contiguous_count_for_key(store, *k);
        insert_bin(sketch, gamma, index_offset, *k, count, negative, warnings);
    }

    let offset = store.contiguous_bin_index_offset;
    for (i, count) in store.contiguous_bin_counts.iter().enumerate() {
        if *count == 0.0 {
            continue;
        }
        let k = offset.saturating_add(i as i32);
        if store.bin_counts.contains_key(&k) {
            continue;
        }
        insert_bin(sketch, gamma, index_offset, k, *count, negative, warnings);
    }
}

fn contiguous_count_for_key(store: &crate::protos::sketch::Store, k: i32) -> f64 {
    let idx = i64::from(k) - i64::from(store.contiguous_bin_index_offset);
    if idx < 0 {
        return 0.0;
    }
    usize::try_from(idx)
        .ok()
        .and_then(|idx| store.contiguous_bin_counts.get(idx))
        .copied()
        .unwrap_or(0.0)
}

fn insert_bin(
    sketch: &mut AgentDDSketch,
    gamma: f64,
    index_offset: f64,
    k: i32,
    count: f64,
    negative: bool,
    warnings: &mut DecodeWarnings,
) {
    let rounded = count.round();
    if rounded != count {
        warnings.bins_fractional = warnings.bins_fractional.saturating_add(1);
    }
    let n = if rounded >= f64::from(u16::MAX) {
        warnings.bins_saturated = warnings.bins_saturated.saturating_add(1);
        u16::MAX
    } else if !rounded.is_finite() || rounded <= 0.0 {
        return;
    } else {
        rounded as u16
    };

    let value = value_from_key(gamma, index_offset, k);
    let value = if negative { -value } else { value };
    sketch.insert_n(value, u32::from(n));
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
