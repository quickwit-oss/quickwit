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

use serde_json::Value;

use crate::ProcessedLog;

/// Holds the data needed to handle a "field alias" mapping.
/// - `from` is a list of aliases to look for in the data, e.g. ["@timestamp", "timestamp"]
/// - `to` is the final field name, e.g. "timestamp"
/// - `remove_old` indicates whether to delete the old field from the data after copying.
#[derive(Debug)]
pub struct NormalizeField {
    pub from: Vec<String>,
    pub to: String,
    pub remove_old: bool,
}

impl NormalizeField {
    /// Creates a NormalizeField by splitting a comma-separated list of aliases,
    /// e.g. `("@timestamp, timestamp, time", "timestamp", true)`
    pub fn from_comma_sep(from_csv: &str, to: &str, remove_old: bool) -> Self {
        let from = from_csv.split(',').map(|s| s.trim().to_owned()).collect();
        NormalizeField {
            from,
            to: to.to_owned(),
            remove_old,
        }
    }
}

/// A generic function that applies a list of `NormalizeField` entries
/// to a `serde_json::Map<String, Value>`.
///
/// For each `NormalizeField`:
///  1) We look for the first matching alias in `map`.
///  2) If found, we call the `on_remap` closure with the new field name and the value.
///  3) If `remove_old == true`, we remove the old alias from `map`.
pub fn normalize_fields(
    log: &mut ProcessedLog,
    normalize_fields: &[NormalizeField],
    mut on_remap: impl FnMut(&mut ProcessedLog, &str, Value),
) {
    for normalize_field in normalize_fields {
        // find the first alias that exists in the map
        // TODO: This does not handle JSON paths, e.g. "syslog.timestamp" yet
        if let Some(alias) = normalize_field
            .from
            .iter()
            .find(|alias| log.custom.contains_key(*alias))
        {
            if normalize_field.remove_old {
                let val = log.custom.remove(alias).unwrap();
                on_remap(log, &normalize_field.to, val);
            } else {
                on_remap(
                    log,
                    &normalize_field.to,
                    log.custom.get(alias).unwrap().clone(),
                );
            }
        }
    }
}
