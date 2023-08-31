// Copyright (C) 2023 Quickwit, Inc.
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

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(into = "f32", try_from = "f32")]
pub struct NotNaNf32(f32);

impl NotNaNf32 {
    pub const ZERO: Self = NotNaNf32(0.0f32);
    pub const ONE: Self = NotNaNf32(1.0f32);
}

impl From<NotNaNf32> for f32 {
    fn from(not_nan_f32: NotNaNf32) -> f32 {
        not_nan_f32.0
    }
}

impl TryFrom<f32> for NotNaNf32 {
    type Error = &'static str;

    fn try_from(possibly_nan: f32) -> Result<NotNaNf32, &'static str> {
        if possibly_nan.is_nan() {
            return Err("NaN is not supported as a boost value.");
        }
        Ok(NotNaNf32(possibly_nan))
    }
}

impl Eq for NotNaNf32 {}

impl PartialOrd for NotNaNf32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for NotNaNf32 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
