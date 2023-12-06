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
