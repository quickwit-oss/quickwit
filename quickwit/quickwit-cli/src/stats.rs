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

pub(crate) fn mean(values: &[u64]) -> f32 {
    assert!(!values.is_empty());
    let sum: u64 = values.iter().sum();
    sum as f32 / values.len() as f32
}

pub(crate) fn std_deviation(values: &[u64]) -> f32 {
    assert!(!values.is_empty());
    let mean = mean(values);
    let variance = values
        .iter()
        .map(|value| {
            let diff = mean - (*value as f32);
            diff * diff
        })
        .sum::<f32>()
        / values.len() as f32;
    variance.sqrt()
}

/// Return percentile of sorted values using linear interpolation.
pub(crate) fn percentile(sorted_values: &[u64], percent: usize) -> f32 {
    assert!(!sorted_values.is_empty());
    assert!(percent <= 100);
    if sorted_values.len() == 1 {
        return sorted_values[0] as f32;
    }
    if percent == 100 {
        return sorted_values[sorted_values.len() - 1] as f32;
    }
    let length = (sorted_values.len() - 1) as f32;
    let rank = (percent as f32 / 100f32) * length;
    let lrank = rank.floor();
    let d = rank - lrank;
    let n = lrank as usize;
    let lo = sorted_values[n] as f32;
    let hi = sorted_values[n + 1] as f32;
    lo + (hi - lo) * d
}
