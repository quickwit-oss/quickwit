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

pub mod grok_auto_step;
pub mod grok_rules;
pub mod grok_step;

pub use grok_step::*;
use vrl::core::Value as VrlValue;

use crate::error::PipelineError;

/// A helper function to convert VRL's `Value` into `serde_json::Value`.
pub(crate) fn vrl_value_to_serde_json(v: VrlValue) -> crate::Result<serde_json::Value> {
    let value = match v {
        VrlValue::Bytes(s) => {
            // This can't fail, because the grok parser only returns strings.
            serde_json::Value::String(String::from_utf8(s.to_vec()).map_err(|err| {
                PipelineError::Other {
                    error: err.to_string(),
                }
            })?)
        }
        VrlValue::Float(f) => serde_json::Value::from(f.into_inner()),
        VrlValue::Array(arr) => {
            let json_arr: crate::Result<Vec<_>> =
                arr.into_iter().map(vrl_value_to_serde_json).collect();
            serde_json::Value::Array(json_arr?)
        }
        VrlValue::Object(map) => {
            let json_map = map
                .into_iter()
                .map(|(k, v)| Ok((String::from(k.clone()), vrl_value_to_serde_json(v)?)))
                .collect::<crate::Result<_>>()?;
            serde_json::Value::Object(json_map)
        }
        VrlValue::Null => serde_json::Value::Null,
        VrlValue::Boolean(b) => b.into(),
        VrlValue::Regex(_value_regex) => serde_json::Value::Null,
        VrlValue::Integer(i) => i.into(),
        VrlValue::Timestamp(date_time) => date_time.to_rfc3339().into(),
    };
    Ok(value)
}
