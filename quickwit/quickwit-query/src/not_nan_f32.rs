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
            return Err("NaN is not supported as a boost value");
        }
        Ok(NotNaNf32(possibly_nan))
    }
}

impl Eq for NotNaNf32 {}
