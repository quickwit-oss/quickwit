// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

pub(crate) fn mean(values: &[usize]) -> f32 {
    assert!(!values.is_empty());
    let sum: usize = values.iter().sum();
    sum as f32 / values.len() as f32
}

pub(crate) fn std_deviation(values: &[usize]) -> f32 {
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
pub(crate) fn percentile(sorted_values: &[usize], percent: usize) -> f32 {
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
