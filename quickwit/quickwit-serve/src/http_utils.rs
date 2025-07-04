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

//! HTTP utilities for serialization and other common HTTP-related functionality.

use axum::http::StatusCode;
use serde::{Deserialize, Deserializer, Serializer};

/// Custom serialization for StatusCode to handle http crate version conflicts.
///
/// This function serializes a StatusCode as a u16 to avoid dependency issues
/// between different versions of the http crate used by different dependencies.
pub fn serialize_status_code<S>(status: &StatusCode, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    serializer.serialize_u16(status.as_u16())
}

/// Custom deserialization for StatusCode to handle http crate version conflicts.
///
/// This function deserializes a u16 into a StatusCode to avoid dependency issues
/// between different versions of the http crate used by different dependencies.
pub fn deserialize_status_code<'de, D>(deserializer: D) -> Result<StatusCode, D::Error>
where D: Deserializer<'de> {
    let status_code_u16: u16 = Deserialize::deserialize(deserializer)?;
    StatusCode::from_u16(status_code_u16).map_err(serde::de::Error::custom)
}
