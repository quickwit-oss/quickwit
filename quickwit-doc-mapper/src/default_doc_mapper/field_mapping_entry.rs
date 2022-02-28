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

use std::convert::TryFrom;

use anyhow::bail;
use chrono::{FixedOffset, Utc};
use itertools::{process_results, Itertools};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use tantivy::schema::{
    BytesOptions, Cardinality, DocParsingError as TantivyDocParser, FieldType, IndexRecordOption,
    NumericOptions, TextFieldIndexing, TextOptions, Value,
};
use thiserror::Error;

use super::{default_as_true, FieldMappingType};
use crate::default_doc_mapper::validate_field_mapping_name;

/// A `FieldMappingEntry` defines how a field is indexed, stored,
/// and mapped from a JSON document to the related index fields.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(
    try_from = "FieldMappingEntryForSerialization",
    into = "FieldMappingEntryForSerialization"
)]
pub struct FieldMappingEntry {
    /// Field name in the index schema.
    pub name: String,
    /// Property parameters which defines the type and the way the value must be indexed.
    pub mapping_type: FieldMappingType,
}

impl FieldMappingEntry {
    /// Creates a new [`FieldMappingEntry`].
    pub fn new(name: String, mapping_type: FieldMappingType) -> Self {
        assert!(validate_field_mapping_name(&name).is_ok());
        FieldMappingEntry { name, mapping_type }
    }

    /// Creates a new root [`FieldMappingEntry`].
    pub fn root(mapping_type: FieldMappingType) -> Self {
        FieldMappingEntry {
            name: "".to_string(),
            mapping_type,
        }
    }

    /// Returns the field entries that must be added to the schema.
    // TODO: can be more efficient to pass a collector in argument (a schema builder)
    // on which we add entry fields.
    pub fn field_entries(&self) -> Vec<(FieldPath, FieldType)> {
        let field_path = FieldPath::new(&self.name);
        match &self.mapping_type {
            FieldMappingType::Text(options, _) => {
                vec![(field_path, FieldType::Str(options.clone()))]
            }
            FieldMappingType::I64(options, _) => {
                vec![(field_path, FieldType::I64(options.clone()))]
            }
            FieldMappingType::U64(options, _) => {
                vec![(field_path, FieldType::U64(options.clone()))]
            }
            FieldMappingType::F64(options, _) => {
                vec![(field_path, FieldType::F64(options.clone()))]
            }
            FieldMappingType::Date(options, _) => {
                vec![(field_path, FieldType::Date(options.clone()))]
            }
            FieldMappingType::Bytes(options, _) => {
                vec![(field_path, FieldType::Bytes(options.clone()))]
            }
            FieldMappingType::Object(field_mappings) => field_mappings
                .iter()
                .map(|entry| entry.field_entries())
                .flatten()
                .map(|(path, entry)| (path.with_parent(&self.name), entry))
                .collect(),
        }
    }

    /// Returns the fields entries that map to fast fields.
    pub fn fast_field_entries(&self) -> Vec<(FieldPath, FieldType)> {
        self.field_entries()
            .into_iter()
            .filter(|(_, field_type)| match field_type {
                FieldType::U64(options)
                | FieldType::I64(options)
                | FieldType::F64(options)
                | FieldType::Date(options) => options.is_fast(),
                FieldType::Bytes(option) => option.is_fast(),
                _ => false,
            })
            .collect_vec()
    }

    /// Returns the field mappings.
    pub fn field_mappings(&self) -> Option<Vec<FieldMappingEntry>> {
        match &self.mapping_type {
            FieldMappingType::Object(entries) => Some(entries.clone()),
            _ => None,
        }
    }

    /// Returns tantivy field path and associated values to be added in the document.
    // TODO: can be more efficient to pass a collector in argument (a kind of DocumentWriter)
    // on which we add field values, thus we directly build the doucment instead of returning
    // a Vec.
    pub fn parse(&self, json_value: JsonValue) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        match &self.mapping_type {
            FieldMappingType::Text(options, cardinality) => {
                self.parse_text(json_value, options, cardinality)
            }
            FieldMappingType::I64(options, cardinality) => {
                self.parse_i64(json_value, options, cardinality)
            }
            FieldMappingType::U64(options, cardinality) => {
                self.parse_u64(json_value, options, cardinality)
            }
            FieldMappingType::F64(options, cardinality) => {
                self.parse_f64(json_value, options, cardinality)
            }
            FieldMappingType::Date(options, cardinality) => {
                self.parse_date(json_value, options, cardinality)
            }
            FieldMappingType::Bytes(options, cardinality) => {
                self.parse_bytes(json_value, options, cardinality)
            }
            FieldMappingType::Object(field_mappings) => {
                self.parse_object(json_value, field_mappings)
            }
        }
    }

    fn parse_text(
        &self,
        json_value: JsonValue,
        options: &TextOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_text(element, options, cardinality)),
                    |iter| iter.flatten().collect(),
                )?
            }
            JsonValue::String(value_as_str) => {
                vec![(FieldPath::new(&self.name), Value::Str(value_as_str))]
            }
            JsonValue::Null => {
                vec![]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    format!("Expected JSON string, got '{}'.", json_value),
                ));
            }
        };
        Ok(parsed_values)
    }

    fn parse_i64(
        &self,
        json_value: JsonValue,
        options: &NumericOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_i64(element, options, cardinality)),
                    |iter| iter.flatten().collect(),
                )?
            }
            JsonValue::Number(value_as_number) => {
                if let Some(value_as_i64) = value_as_number.as_i64() {
                    vec![(FieldPath::new(&self.name), Value::I64(value_as_i64))]
                } else {
                    return Err(DocParsingError::ValueError(
                        self.name.clone(),
                        format!("Expected i64, got '{}'.", value_as_number),
                    ));
                }
            }
            JsonValue::Null => {
                vec![]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    format!(
                        "Expected JSON number or array of JSON numbers, got '{}'.",
                        json_value
                    ),
                ))
            }
        };
        Ok(parsed_values)
    }

    fn parse_u64(
        &self,
        json_value: JsonValue,
        options: &NumericOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_u64(element, options, cardinality)),
                    |iter| iter.flatten().collect(),
                )?
            }
            JsonValue::Number(value_as_number) => {
                if let Some(value_as_u64) = value_as_number.as_u64() {
                    vec![(FieldPath::new(&self.name), Value::U64(value_as_u64))]
                } else {
                    return Err(DocParsingError::ValueError(
                        self.name.clone(),
                        format!("Expected u64, got '{}'.", value_as_number),
                    ));
                }
            }
            JsonValue::Null => {
                vec![]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    format!(
                        "Expected JSON number or array of JSON numbers, got '{}'.",
                        json_value
                    ),
                ))
            }
        };
        Ok(parsed_values)
    }

    fn parse_f64(
        &self,
        json_value: JsonValue,
        options: &NumericOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_f64(element, options, cardinality)),
                    |iter| iter.flatten().collect(),
                )?
            }
            JsonValue::Number(value_as_number) => {
                if let Some(value_as_f64) = value_as_number.as_f64() {
                    vec![(FieldPath::new(&self.name), Value::F64(value_as_f64))]
                } else {
                    return Err(DocParsingError::ValueError(
                        self.name.clone(),
                        format!(
                            "Expected f64, got inconvertible JSON number '{}'.",
                            value_as_number
                        ),
                    ));
                }
            }
            JsonValue::Null => {
                vec![]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    format!(
                        "Expected JSON number or array of JSON numbers, got '{}'.",
                        json_value
                    ),
                ))
            }
        };
        Ok(parsed_values)
    }

    fn parse_date(
        &self,
        json_value: JsonValue,
        options: &NumericOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_date(element, options, cardinality)),
                    |iter| iter.flatten().collect(),
                )?
            }
            JsonValue::String(value_as_str) => {
                let dt_with_fixed_tz: chrono::DateTime<FixedOffset> =
                    chrono::DateTime::parse_from_rfc3339(&value_as_str).map_err(|err| {
                        DocParsingError::ValueError(
                            self.name.clone(),
                            format!("Expected RFC 3339 date, got '{}'. {:?}", value_as_str, err),
                        )
                    })?;
                vec![(
                    FieldPath::new(&self.name),
                    Value::Date(dt_with_fixed_tz.with_timezone(&Utc)),
                )]
            }
            JsonValue::Null => {
                vec![]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    format!("Expected RFC 3339 date, got '{}'.", json_value),
                ))
            }
        };
        Ok(parsed_values)
    }

    fn parse_bytes(
        &self,
        json_value: JsonValue,
        options: &BytesOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_bytes(element, options, cardinality)),
                    |iter| iter.flatten().collect(),
                )?
            }
            JsonValue::String(value_as_str) => {
                let value = base64::decode(&value_as_str)
                    .map(Value::Bytes)
                    .map_err(|_| {
                        DocParsingError::ValueError(
                            self.name.clone(),
                            format!("Expected Base64 string, got '{}'.", value_as_str),
                        )
                    })?;
                vec![(FieldPath::new(&self.name), value)]
            }
            JsonValue::Null => {
                vec![]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    format!("Expected JSON string for bytes field, got '{}'", json_value),
                ))
            }
        };
        Ok(parsed_values)
    }

    fn parse_object<'a>(
        &'a self,
        json_value: JsonValue,
        entries: &'a [FieldMappingEntry],
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(_) => {
                // TODO: we can support it but we need to do some extra validation on
                // the field mappings as they must be all multivalued.
                return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
            }
            JsonValue::Object(mut object) => process_results(
                entries
                    .iter()
                    .flat_map(|entry| object.remove(&entry.name).map(|child| entry.parse(child))),
                |iter| {
                    iter.flatten()
                        .map(|(path, entry)| (path.with_parent(&self.name), entry))
                        .collect()
                },
            )?,
            JsonValue::Null => {
                vec![]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    format!("Expected JSON object, got '{}'.", json_value),
                ))
            }
        };
        Ok(parsed_values)
    }
}

/// A field path composed by the list of path components.
/// Used to build a tantivy valid field name by joining its
/// components with a special string `__dot__` as currently
/// tantivy does not support `.`.
pub struct FieldPath<'a> {
    components: Vec<&'a str>,
}

impl<'a> FieldPath<'a> {
    pub fn new(path: &'a str) -> Self {
        Self {
            components: vec![path],
        }
    }

    /// Returns the FieldPath with the additionnal parent component.
    /// This will consume your `FieldPath`.
    pub fn with_parent(mut self, parent: &'a str) -> Self {
        if !parent.is_empty() {
            self.components.insert(0, parent);
        }
        self
    }

    // Returns field name built by joining its components with a `.` separator.
    pub fn field_name(&self) -> String {
        // Some components can contains dots.
        self.components.join(".")
    }
}

// Struct used for serialization and deserialization
// Main advantage: having a flat structure and gain flexibility
// if we want to add some syntaxic sugar in the mapping.
// Main drawback: we have a bunch of mixed parameters in it but
// seems to be reasonable.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldMappingEntryForSerialization {
    name: String,
    #[serde(rename = "type")]
    type_with_cardinality: String,
    #[serde(default = "default_as_true")]
    stored: bool,
    #[serde(default)]
    fast: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tokenizer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record: Option<IndexRecordOption>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    field_mappings: Vec<FieldMappingEntryForSerialization>,
}

impl TryFrom<FieldMappingEntryForSerialization> for FieldMappingEntry {
    type Error = anyhow::Error;

    fn try_from(value: FieldMappingEntryForSerialization) -> anyhow::Result<Self> {
        let field_type = match value.field_type_str() {
            "text" => value.new_text()?,
            "i64" => value.new_i64()?,
            "u64" => value.new_u64()?,
            "f64" => value.new_f64()?,
            "date" => value.new_date()?,
            "bytes" => value.new_bytes()?,
            "object" => value.new_object()?,
            type_str => bail!(
                "Field `{}` has an unknown type: `{}`.",
                value.name,
                type_str
            ),
        };
        validate_field_mapping_name(&value.name)?;
        Ok(FieldMappingEntry::new(value.name, field_type))
    }
}

impl From<FieldMappingEntry> for FieldMappingEntryForSerialization {
    fn from(value: FieldMappingEntry) -> FieldMappingEntryForSerialization {
        let field_mappings = value
            .field_mappings()
            .unwrap_or_else(Vec::new)
            .into_iter()
            .map(FieldMappingEntryForSerialization::from)
            .collect();
        let type_with_cardinality = value.mapping_type.type_with_cardinality();
        let mut fast = false;
        let mut indexed = None;
        let mut record = None;
        let mut stored = false;
        let mut tokenizer: Option<String> = None;
        match value.mapping_type {
            FieldMappingType::Text(text_options, _) => {
                stored = text_options.is_stored();
                if let Some(indexing_options) = text_options.get_indexing_options() {
                    tokenizer = Some(indexing_options.tokenizer().to_owned());
                    record = Some(indexing_options.index_option());
                } else {
                    indexed = Some(false);
                }
            }
            FieldMappingType::I64(options, _)
            | FieldMappingType::U64(options, _)
            | FieldMappingType::F64(options, _)
            | FieldMappingType::Date(options, _) => {
                stored = options.is_stored();
                indexed = Some(options.is_indexed());
                fast = options.get_fastfield_cardinality().is_some();
            }
            FieldMappingType::Bytes(options, _) => {
                stored = options.is_stored();
                indexed = Some(options.is_indexed());
                fast = options.is_fast();
            }
            _ => (),
        }

        FieldMappingEntryForSerialization {
            name: value.name,
            type_with_cardinality,
            fast,
            indexed,
            record,
            stored,
            tokenizer,
            field_mappings,
        }
    }
}

impl FieldMappingEntryForSerialization {
    fn is_array(&self) -> bool {
        self.type_with_cardinality.starts_with("array<")
            && self.type_with_cardinality.ends_with('>')
    }

    fn cardinality(&self) -> Cardinality {
        if self.is_array() {
            Cardinality::MultiValues
        } else {
            Cardinality::SingleValue
        }
    }

    fn field_type_str(&self) -> &str {
        if self.is_array() {
            &self.type_with_cardinality[6..self.type_with_cardinality.len() - 1]
        } else {
            &self.type_with_cardinality
        }
    }

    fn new_text(&self) -> anyhow::Result<FieldMappingType> {
        if self.fast {
            bail!(
                "Error when parsing field `{}`: fast=true not yet supported for text field.",
                self.name
            )
        }
        let mut options = TextOptions::default();
        if self.indexed.unwrap_or(true) {
            let mut indexing_options = TextFieldIndexing::default();
            if let Some(index_option) = self.record {
                indexing_options = indexing_options.set_index_option(index_option);
            }
            if let Some(tokenizer) = &self.tokenizer {
                indexing_options = indexing_options.set_tokenizer(tokenizer);
            }
            options = options.set_indexing_options(indexing_options);
        } else if self.record.is_some() || self.tokenizer.is_some() {
            bail!(
                "Error when parsing `{}`: `record` and `tokenizer` parameters are allowed only if \
                 indexed is true.",
                self.name
            )
        }
        if self.stored {
            options = options.set_stored();
        }
        Ok(FieldMappingType::Text(options, self.cardinality()))
    }

    fn new_i64(&self) -> anyhow::Result<FieldMappingType> {
        let options = self.int_options()?;
        Ok(FieldMappingType::I64(options, self.cardinality()))
    }

    fn new_u64(&self) -> anyhow::Result<FieldMappingType> {
        let options = self.int_options()?;
        Ok(FieldMappingType::U64(options, self.cardinality()))
    }

    fn new_f64(&self) -> anyhow::Result<FieldMappingType> {
        let options = self.int_options()?;
        Ok(FieldMappingType::F64(options, self.cardinality()))
    }

    fn new_date(&self) -> anyhow::Result<FieldMappingType> {
        let options = self.int_options()?;
        Ok(FieldMappingType::Date(options, self.cardinality()))
    }

    fn new_bytes(&self) -> anyhow::Result<FieldMappingType> {
        self.check_no_text_options()?;
        let mut options = BytesOptions::default();
        if self.stored {
            options = options.set_stored();
        }
        if self.indexed.unwrap_or(true) {
            options = options.set_indexed();
        }
        if self.fast {
            options = options.set_fast();
        }
        Ok(FieldMappingType::Bytes(options, self.cardinality()))
    }

    fn new_object(&self) -> anyhow::Result<FieldMappingType> {
        if self.record.is_some() || self.tokenizer.is_some() {
            bail!(
                "Error when parsing field `{}`: `field_mappings` is the only valid parameter.",
                self.name
            )
        }
        if self.is_array() {
            bail!(
                "Error when parsing field `{}`: array of object is not supported.",
                self.name
            )
        }
        let field_mappings = self
            .field_mappings
            .iter()
            .map(|entry| FieldMappingEntry::try_from(entry.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        if field_mappings.is_empty() {
            bail!(
                "Error when parsing field `{}`: object type must have at least one field mapping.",
                self.name
            )
        }
        Ok(FieldMappingType::Object(field_mappings))
    }

    fn int_options(&self) -> anyhow::Result<NumericOptions> {
        self.check_no_text_options()?;
        let mut options = NumericOptions::default();
        if self.stored {
            options = options.set_stored();
        }
        // If fast is true, always set cardinality to multivalues to make
        // simple cardinality changes.
        if self.fast {
            options = options.set_fast(self.cardinality());
        }
        if self.indexed.unwrap_or(true) {
            options = options.set_indexed();
        }
        Ok(options)
    }

    fn check_no_text_options(&self) -> anyhow::Result<()> {
        if self.record.is_some() || self.tokenizer.is_some() {
            bail!(
                "Error when parsing `{}`: `record` and `tokenizer` parameters are for text field \
                 only.",
                self.name
            )
        }
        Ok(())
    }
}

/// Error that may happen when parsing
/// a document from JSON.
#[derive(Debug, Error, PartialEq)]
pub enum DocParsingError {
    /// The provided string is not valid JSON.
    #[error("The provided string is not valid JSON")]
    NotJson(String),
    /// One of the value could not be parsed.
    #[error("The field '{0}' could not be parsed: {1}")]
    ValueError(String, String),
    /// The json-document contains a field that is not declared in the schema.
    #[error("The document contains a field that is not declared in the schema: {0:?}")]
    NoSuchFieldInSchema(String),
    /// The document contains a array of values but a single value is expected.
    #[error("The document contains an array of values but a single value is expected: {0:?}")]
    MultiValuesNotSupported(String),
    /// The document does not contains a field that is required.
    #[error("The document must contain field {0:?}. As a fast field, it is implicitly required.")]
    RequiredFastField(String),
}

impl From<TantivyDocParser> for DocParsingError {
    fn from(value: TantivyDocParser) -> Self {
        match value {
            TantivyDocParser::InvalidJson(text) => DocParsingError::NoSuchFieldInSchema(text),
            TantivyDocParser::ValueError(text, error) => {
                DocParsingError::ValueError(text, format!("{:?}", error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use matches::matches;
    use serde_json::json;
    use tantivy::schema::{Cardinality, Value};

    use super::FieldMappingEntry;
    use crate::default_doc_mapper::FieldMappingType;
    use crate::DocParsingError;

    const TEXT_MAPPING_ENTRY_VALUE: &str = r#"
        {
            "name": "my_field_name",
            "type": "text",
            "stored": true,
            "record": "basic",
            "tokenizer": "english"
        }
    "#;

    const OBJECT_MAPPING_ENTRY_VALUE: &str = r#"
        {
            "name": "my_field_name",
            "type": "object",
            "field_mappings": [
                {
                    "name": "my_field_name",
                    "type": "text"
                }
            ]
        }
    "#;

    #[test]
    fn test_deserialize_text_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(TEXT_MAPPING_ENTRY_VALUE)?;
        assert_eq!(mapping_entry.name, "my_field_name");
        match mapping_entry.mapping_type {
            FieldMappingType::Text(options, _) => {
                assert_eq!(options.is_stored(), true);
                let indexing_options = options
                    .get_indexing_options()
                    .expect("should have indexing option");
                assert_eq!(indexing_options.tokenizer(), "english");
            }
            _ => panic!("wrong property type"),
        }
        Ok(())
    }

    #[test]
    fn test_error_on_text_with_invalid_options() -> anyhow::Result<()> {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "text",
                "indexed": false,
                "tokenizer": "default",
                "record": "position"
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error when parsing `my_field_name`: `record` and `tokenizer` parameters are allowed \
             only if indexed is true."
        );
        Ok(())
    }

    #[test]
    fn test_error_on_unknown_fields() -> anyhow::Result<()> {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "text",
                "indexing": false,
                "tokenizer": "default",
                "record": "position"
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("unknown field `indexing`"));
        Ok(())
    }

    #[test]
    fn test_deserialize_object_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(OBJECT_MAPPING_ENTRY_VALUE)?;
        assert_eq!(mapping_entry.name, "my_field_name");
        match mapping_entry.mapping_type {
            FieldMappingType::Object(field_mappings) => {
                assert_eq!(field_mappings.len(), 1);
            }
            _ => panic!("wrong property type"),
        }
        Ok(())
    }

    #[test]
    fn test_deserialize_object_mapping_with_no_field_mappings() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "object",
                "field_mappings": []
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error when parsing field `my_field_name`: object type must have at least one field \
             mapping."
        );
    }

    #[test]
    fn test_deserialize_field_with_unknown_type() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "my custom type"
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Field `my_field_name` has an unknown type: `my custom type`."
        );
    }

    #[test]
    fn test_deserialize_i64_field_with_invalid_name() {
        assert!(serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "this is not ok",
                "type": "i64"
            }
            "#,
        )
        .unwrap_err()
        .to_string()
        .contains("illegal characters"));
    }

    #[test]
    fn test_deserialize_i64_field_with_text_options() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "i64",
                "tokenizer": "basic"
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error when parsing `my_field_name`: `record` and `tokenizer` parameters are for text \
             field only."
        );
    }

    #[test]
    fn test_deserialize_multivalued_i64_field() -> anyhow::Result<()> {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<i64>"
            }
            "#,
        )?;

        match result.mapping_type {
            FieldMappingType::I64(options, cardinality) => {
                assert_eq!(options.is_indexed(), true); // default
                assert_eq!(options.is_fast(), false); // default
                assert_eq!(options.is_stored(), true); // default
                assert_eq!(cardinality, Cardinality::MultiValues);
            }
            _ => bail!("Wrong type"),
        }

        Ok(())
    }

    #[test]
    fn test_deserialize_singlevalued_i64_field() -> anyhow::Result<()> {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "i64"
            }
            "#,
        )?;

        match result.mapping_type {
            FieldMappingType::I64(options, cardinality) => {
                assert_eq!(options.is_indexed(), true); // default
                assert_eq!(options.is_fast(), false); // default
                assert_eq!(options.is_stored(), true); // default
                assert_eq!(cardinality, Cardinality::SingleValue);
            }
            _ => bail!("Wrong type"),
        }

        Ok(())
    }

    #[test]
    fn test_serialize_i64_field() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "i64"
            }
            "#,
        )?;
        let entry_str = serde_json::to_string(&entry)?;
        assert_eq!(
            entry_str,
            "{\"name\":\"my_field_name\",\"type\":\"i64\",\"stored\":true,\"fast\":false,\"\
             indexed\":true}"
        );
        Ok(())
    }

    #[test]
    fn test_parse_i64() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "i64"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!(10))?;
        assert_eq!(parsed_value.len(), 1);
        assert_eq!(parsed_value[0].0.field_name(), "my_field_name".to_owned());
        assert_eq!(parsed_value[0].1, Value::I64(10));

        let parsed_value = entry.parse(json!(null));
        assert!(parsed_value.unwrap().is_empty());

        // Failed parsing
        let parsed_error = entry.parse(json!(1.2));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!([1, 2]));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::MultiValuesNotSupported(_))
        ));
        let parsed_error = entry.parse(json!(9223372036854775808_u64));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!("12"));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!("{}"));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        Ok(())
    }

    #[test]
    fn test_parse_mutivalued_i64() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<i64>"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!([10, 20]))?;
        assert_eq!(parsed_value.len(), 2);
        assert_eq!(parsed_value[0].1, Value::I64(10));
        assert_eq!(parsed_value[1].1, Value::I64(20));
        Ok(())
    }

    #[test]
    fn test_deserialize_u64_field_with_wrong_options() {
        assert_eq!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "my_field_name",
                "type": "u64",
                "tokenizer": "basic"
            }"#
            )
            .unwrap_err()
            .to_string(),
            "Error when parsing `my_field_name`: `record` and `tokenizer` parameters are for text \
             field only."
        );
    }

    #[test]
    fn test_deserialize_multivalued_u64_field() -> anyhow::Result<()> {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<u64>"
            }
            "#,
        )?;

        match result.mapping_type {
            FieldMappingType::U64(options, cardinality) => {
                assert_eq!(options.is_indexed(), true); // default
                assert_eq!(options.is_fast(), false); // default
                assert_eq!(options.is_stored(), true); // default
                assert_eq!(cardinality, Cardinality::MultiValues);
            }
            _ => bail!("Wrong type"),
        }

        Ok(())
    }

    #[test]
    fn test_deserialize_singlevalued_u64_field() -> anyhow::Result<()> {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "u64"
            }
            "#,
        )?;

        match result.mapping_type {
            FieldMappingType::U64(options, cardinality) => {
                assert_eq!(options.is_indexed(), true); // default
                assert_eq!(options.is_fast(), false); // default
                assert_eq!(options.is_stored(), true); // default
                assert_eq!(cardinality, Cardinality::SingleValue);
            }
            _ => bail!("Wrong type"),
        }

        Ok(())
    }

    #[test]
    fn test_serialize_u64_field() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "u64"
            }
            "#,
        )?;
        let entry_str = serde_json::to_string(&entry)?;
        assert_eq!(
            entry_str,
            "{\"name\":\"my_field_name\",\"type\":\"u64\",\"stored\":true,\"fast\":false,\"\
             indexed\":true}"
        );
        Ok(())
    }

    #[test]
    fn test_parse_u64() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "u64"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!(10))?;
        assert_eq!(parsed_value.len(), 1);
        assert_eq!(parsed_value[0].0.field_name(), "my_field_name".to_owned());
        assert_eq!(parsed_value[0].1, Value::U64(10));

        let parsed_value = entry.parse(json!(null));
        assert!(parsed_value.unwrap().is_empty());

        // Failed parsing
        let parsed_error = entry.parse(json!(1.2));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!(-3));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!([1, 2]));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::MultiValuesNotSupported(_))
        ));
        let parsed_error = entry.parse(json!("12"));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!("{}"));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        Ok(())
    }

    #[test]
    fn test_parse_mutivalued_u64() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<u64>"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!([10, 20]))?;
        assert_eq!(parsed_value.len(), 2);
        assert_eq!(parsed_value[0].1, Value::U64(10));
        assert_eq!(parsed_value[1].1, Value::U64(20));
        Ok(())
    }

    #[test]
    fn test_parse_f64() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "f64"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!(10.2))?;
        assert_eq!(parsed_value[0].0.field_name(), "my_field_name");
        assert_eq!(parsed_value[0].1, Value::F64(10.2));

        let parsed_value = entry.parse(json!(10))?;
        assert_eq!(parsed_value[0].1, Value::F64(10.0));

        let parsed_value = entry.parse(json!(null));
        assert!(parsed_value.unwrap().is_empty());

        // Failed parsing
        let parsed_error = entry.parse(json!("123"));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!([1, 2]));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::MultiValuesNotSupported(_))
        ));
        Ok(())
    }

    #[test]
    fn test_parse_mutivalued_f64() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<f64>"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!([10, 20.5]))?;
        assert_eq!(parsed_value.len(), 2);
        assert_eq!(parsed_value[0].1, Value::F64(10.0));
        assert_eq!(parsed_value[1].1, Value::F64(20.5));
        Ok(())
    }

    #[test]
    fn test_parse_text() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "text"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!("bacon and eggs"))?;
        assert_eq!(parsed_value[0].0.field_name(), "my_field_name");
        assert_eq!(parsed_value[0].1, Value::Str("bacon and eggs".to_owned()));

        let parsed_value = entry.parse(json!(null));
        assert!(parsed_value.unwrap().is_empty());

        // Failed parsing
        let parsed_error = entry.parse(json!(123));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!(["1"]));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::MultiValuesNotSupported(_))
        ));
        Ok(())
    }

    #[test]
    fn test_parse_mutivalued_text() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<text>"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!(["one", "two"]))?;
        assert_eq!(parsed_value.len(), 2);
        assert_eq!(parsed_value[0].1, Value::Str("one".to_owned()));
        assert_eq!(parsed_value[1].1, Value::Str("two".to_owned()));
        Ok(())
    }

    #[test]
    fn test_parse_date() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "date"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!("2021-12-19T16:39:57-01:00"))?;
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2021, 12, 19),
            NaiveTime::from_hms(17, 39, 57),
        );
        let datetime_utc = Utc.from_utc_datetime(&datetime);
        assert_eq!(parsed_value.len(), 1);
        assert_eq!(parsed_value[0].1, Value::Date(datetime_utc));

        let parsed_value = entry.parse(json!(null));
        assert!(parsed_value.unwrap().is_empty());

        // Failed parsing
        let parsed_error = entry.parse(json!(123));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!(["2021-12-19T16:39:57-08:00"]));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::MultiValuesNotSupported(_))
        ));
        Ok(())
    }

    #[test]
    fn test_parse_mutivalued_date() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<date>"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!([
            "2021-12-19T16:39:57-01:00",
            "2021-12-20T16:39:57-01:00"
        ]))?;
        assert_eq!(parsed_value.len(), 2);
        Ok(())
    }

    #[test]
    fn test_parse_bytes() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "bytes"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!("dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw=="))?;
        assert_eq!(parsed_value.len(), 1);
        assert_eq!(
            parsed_value[0].1,
            Value::Bytes("this is a base64 encoded string".as_bytes().to_vec())
        );

        let parsed_value = entry.parse(json!(null));
        assert!(parsed_value.unwrap().is_empty());

        // Failed parsing
        let parsed_error = entry.parse(json!("not base64 encoded string"));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::ValueError(_, _))
        ));
        let parsed_error = entry.parse(json!(["dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw=="]));
        assert!(matches!(
            parsed_error,
            Err(DocParsingError::MultiValuesNotSupported(_))
        ));
        Ok(())
    }

    #[test]
    fn test_parse_mutivalued_bytes() -> anyhow::Result<()> {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<bytes>"
            }
            "#,
        )?;

        // Successful parsing
        let parsed_value = entry.parse(json!([
            "dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw==",
            "dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw=="
        ]))?;
        assert_eq!(parsed_value.len(), 2);
        Ok(())
    }
}
