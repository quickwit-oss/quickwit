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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct FieldCapabilityResponse {
    indices: Vec<String>,
    fields: HashMap<String, HashMap<String, FieldCapabilityEntryResponse>>,
}
#[derive(Serialize, Deserialize, Debug)]
struct FieldCapabilityFieldTypesResponse {
    long: Option<FieldCapabilityEntryResponse>,
    keyword: Option<FieldCapabilityEntryResponse>,
    text: Option<FieldCapabilityEntryResponse>,
    date_nanos: Option<FieldCapabilityEntryResponse>,
    double: Option<FieldCapabilityEntryResponse>, // Duplicate to float ?
    boolean: Option<FieldCapabilityEntryResponse>,
    ip: Option<FieldCapabilityEntryResponse>,
}

#[derive(Serialize, Deserialize, Debug)]
struct FieldCapabilityEntryResponse {
    metadata_field: bool, // Always false
    searchable: bool,
    aggregatable: bool,
    indices: Vec<String>,                  // [ "index1", "index2" ],
    non_aggregatable_indices: Vec<String>, // [ "index1" ]
    non_searchable_indices: Vec<String>,   // [ "index1" ]
}

#[derive(Serialize, Deserialize, Debug)]
struct FieldCapabilityEntry {
    searchable: bool,
    aggregatable: bool,
}
