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
use itertools::process_results;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use tantivy::schema::{
    BytesOptions, Cardinality, DocParsingError as TantivyDocParser, FieldType, IndexRecordOption,
    NumericOptions, TextFieldIndexing, TextOptions, Type, Value,
};
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::OffsetDateTime;
use tantivy::DateTime;
use thiserror::Error;

use super::{default_as_true, FieldMappingType};
use crate::default_doc_mapper::field_mapping_type::{QuickwitFieldType, QuickwitObjectOptions};
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
    /// Returns the fields entries that map to fast fields.
    pub fast_field_entries: Vec<FieldPath>,
}

impl FieldMappingEntry {
    /// Creates a new [`FieldMappingEntry`].
    pub fn new(name: String, mapping_type: FieldMappingType) -> Self {
        assert!(validate_field_mapping_name(&name).is_ok());
        let fast_field_entries = Vec::new();
        FieldMappingEntry {
            name,
            mapping_type,
            fast_field_entries,
        }
    }

    /// Creates a new root [`FieldMappingEntry`].
    pub fn root(mapping_type: FieldMappingType) -> Self {
        let name = "".to_string();
        let fast_field_entries = Vec::new();
        FieldMappingEntry {
            name,
            mapping_type,
            fast_field_entries,
        }
    }

    /// Returns the field entries that must be added to the schema.
    // TODO: can be more efficient to pass a collector in argument (a schema builder)
    // on which we add entry fields.
    pub(crate) fn compute_field_entries(&self) -> Vec<(FieldPath, FieldType)> {
        let field_path = FieldPath::new(&self.name);
        match &self.mapping_type {
            FieldMappingType::Text(text_options, _) => {
                vec![(field_path, FieldType::Str(text_options.clone().into()))]
            }
            FieldMappingType::I64(options, cardinality) => {
                let opt = get_numeric_options(options, *cardinality);
                vec![(field_path, FieldType::I64(opt))]
            }
            FieldMappingType::U64(options, cardinality) => {
                let opt = get_numeric_options(options, *cardinality);
                vec![(field_path, FieldType::U64(opt))]
            }
            FieldMappingType::F64(options, cardinality) => {
                let opt = get_numeric_options(options, *cardinality);
                vec![(field_path, FieldType::F64(opt))]
            }
            FieldMappingType::Date(options, cardinality) => {
                let opt = get_numeric_options(options, *cardinality);
                vec![(field_path, FieldType::Date(opt))]
            }
            FieldMappingType::Bytes(options, _cardinality) => {
                let bytes_options = get_bytes_options(options);
                vec![(field_path, FieldType::Bytes(bytes_options))]
            }
            FieldMappingType::Object(field_mappings) => field_mappings
                .field_mappings
                .iter()
                .flat_map(|entry| entry.compute_field_entries())
                .map(|(path, entry)| (path.with_parent(&self.name), entry))
                .collect(),
        }
    }

    /// Returns the field mappings.
    pub fn field_mappings(&self) -> Option<Vec<FieldMappingEntry>> {
        match &self.mapping_type {
            FieldMappingType::Object(entries) => Some(entries.field_mappings.clone()),
            _ => None,
        }
    }

    /// Returns tantivy field path and associated values to be added in the document.
    // TODO: can be more efficient to pass a collector in argument (a kind of DocumentWriter)
    // on which we add field values, thus we directly build the doucment instead of returning
    // a Vec.
    pub fn parse(&self, json_value: JsonValue) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        match &self.mapping_type {
            FieldMappingType::Text(_options, cardinality) => {
                self.parse_text(json_value, *cardinality)
            }
            FieldMappingType::I64(_options, cardinality) => {
                self.parse_i64(json_value, *cardinality)
            }
            FieldMappingType::U64(_options, cardinality) => {
                self.parse_u64(json_value, *cardinality)
            }
            FieldMappingType::F64(_options, cardinality) => {
                self.parse_f64(json_value, *cardinality)
            }
            FieldMappingType::Date(_options, cardinality) => {
                self.parse_date(json_value, cardinality)
            }
            FieldMappingType::Bytes(_options, cardinality) => {
                self.parse_bytes(json_value, *cardinality)
            }
            FieldMappingType::Object(options) => self.parse_object(json_value, options),
        }
    }

    fn parse_text(
        &self,
        json_value: JsonValue,
        cardinality: Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_text(element, cardinality)),
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
        cardinality: Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_i64(element, cardinality)),
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
        cardinality: Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_u64(element, cardinality)),
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
        cardinality: Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_f64(element, cardinality)),
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
                        .map(|element| self.parse_date(element, cardinality)),
                    |iter| iter.flatten().collect(),
                )?
            }
            JsonValue::String(value_as_str) => {
                let date_time_utc = DateTime::new_utc(
                    OffsetDateTime::parse(&value_as_str, &Rfc3339).map_err(|err| {
                        DocParsingError::ValueError(
                            self.name.clone(),
                            format!("Expected RFC 3339 date, got '{}'. {:?}", value_as_str, err),
                        )
                    })?,
                );

                vec![(FieldPath::new(&self.name), Value::Date(date_time_utc))]
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
        cardinality: Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                process_results(
                    array
                        .into_iter()
                        .map(|element| self.parse_bytes(element, cardinality)),
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

    fn parse_object(
        &self,
        json_value: JsonValue,
        entries: &QuickwitObjectOptions,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(_) => {
                // TODO: we can support it but we need to do some extra validation on
                // the field mappings as they must be all multivalued.
                return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
            }
            JsonValue::Object(mut object) => process_results(
                entries
                    .field_mappings
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
#[derive(Debug, Clone)]
pub struct FieldPath {
    components: Vec<String>,
}

impl FieldPath {
    pub fn new(path: &str) -> Self {
        Self {
            components: vec![path.to_string()],
        }
    }

    /// Returns the FieldPath with the additionnal parent component.
    /// This will consume your `FieldPath`.
    pub fn with_parent(mut self, parent: &str) -> Self {
        if !parent.is_empty() {
            self.components.insert(0, parent.to_string());
        }
        self
    }

    // Returns field name built by joining its components with a `.` separator.
    pub fn field_name(&self) -> String {
        // Some components can contains dots.
        self.components.join(".")
    }
}

/// Struct used for serialization and deserialization
/// Main advantage: having a flat structure and gain flexibility
/// if we want to add some syntaxic sugar in the mapping.
/// Main drawback: we have a bunch of mixed parameters in it but
/// seems to be reasonable.
///
/// We do not rely on enum with inline tagging and flatten because
/// - serde does not support it in combination with `deny_unknown_field`
/// - it is clumsy to handle `array<type>` keys.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct FieldMappingEntryForSerialization {
    name: String,
    #[serde(rename = "type")]
    type_id: String,
    #[serde(flatten)]
    pub field_mapping_json: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QuickwitNumericOptions {
    #[serde(default = "default_as_true")]
    pub stored: bool,
    #[serde(default = "default_as_true")]
    pub indexed: bool,
    #[serde(default)]
    pub fast: bool,
}

fn get_numeric_options(
    quickwit_numeric_options: &QuickwitNumericOptions,
    cardinality: Cardinality,
) -> NumericOptions {
    let mut numeric_options = NumericOptions::default();
    if quickwit_numeric_options.stored {
        numeric_options = numeric_options.set_stored();
    }
    if quickwit_numeric_options.indexed {
        numeric_options = numeric_options.set_indexed();
    }
    if quickwit_numeric_options.fast {
        numeric_options = numeric_options.set_fast(cardinality);
    }
    numeric_options
}

fn get_bytes_options(quickwit_numeric_options: &QuickwitNumericOptions) -> BytesOptions {
    let mut bytes_options = BytesOptions::default();
    if quickwit_numeric_options.indexed {
        bytes_options = bytes_options.set_indexed();
    }
    if quickwit_numeric_options.fast {
        bytes_options = bytes_options.set_fast();
    }
    if quickwit_numeric_options.stored {
        bytes_options = bytes_options.set_stored();
    }
    bytes_options
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QuickwitTextOptions {
    #[serde(default = "default_as_true")]
    pub indexed: bool,
    #[serde(default)]
    pub tokenizer: Option<String>,
    #[serde(default)]
    pub record: IndexRecordOption,
    #[serde(default)]
    pub fieldnorms: bool,
    #[serde(default = "default_as_true")]
    pub stored: bool,
}

impl From<QuickwitTextOptions> for TextOptions {
    fn from(quickwit_text_options: QuickwitTextOptions) -> Self {
        let mut text_options = TextOptions::default();
        if quickwit_text_options.stored {
            text_options = text_options.set_stored();
        }
        if quickwit_text_options.indexed {
            let mut text_field_indexing = TextFieldIndexing::default();
            if let Some(tokenizer_name) = quickwit_text_options.tokenizer {
                text_field_indexing = text_field_indexing.set_tokenizer(&tokenizer_name);
            }
            text_field_indexing =
                text_field_indexing.set_index_option(quickwit_text_options.record);
            text_options = text_options.set_indexing_options(text_field_indexing);
        }
        text_options
    }
}

fn deserialize_mapping_type(
    quickwit_field_type: QuickwitFieldType,
    json: serde_json::Value,
) -> anyhow::Result<FieldMappingType> {
    let (typ, cardinality) = match quickwit_field_type {
        QuickwitFieldType::Simple(typ) => (typ, Cardinality::SingleValue),
        QuickwitFieldType::Array(typ) => (typ, Cardinality::MultiValues),
        QuickwitFieldType::Object => {
            let object_options: QuickwitObjectOptions = serde_json::from_value(json)?;
            if object_options.field_mappings.is_empty() {
                anyhow::bail!("object type must have at least one field mapping.");
            }
            return Ok(FieldMappingType::Object(object_options));
        }
    };
    match typ {
        Type::Str => {
            let text_options: QuickwitTextOptions = serde_json::from_value(json)?;
            #[allow(clippy::collapsible_if)]
            if !text_options.indexed {
                if text_options.tokenizer.is_some()
                    || text_options.record == IndexRecordOption::Basic
                    || !text_options.fieldnorms
                {
                    bail!(
                        "`record`, `tokenizer`, and `fieldnorms` parameters are allowed only if \
                         indexed is true."
                    );
                }
            }
            Ok(FieldMappingType::Text(text_options, cardinality))
        }
        Type::U64 => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::U64(numeric_options, cardinality))
        }
        Type::I64 => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::I64(numeric_options, cardinality))
        }
        Type::F64 => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::F64(numeric_options, cardinality))
        }
        Type::Date => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::Date(numeric_options, cardinality))
        }
        Type::Facet => unimplemented!("Facet are not supported in quickwit yet."),
        Type::Bytes => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            if numeric_options.fast && cardinality == Cardinality::MultiValues {
                bail!("fast field is not allowed for array<bytes>.");
            }
            Ok(FieldMappingType::Bytes(numeric_options, cardinality))
        }
        Type::Json => todo!(),
    }
}

impl TryFrom<FieldMappingEntryForSerialization> for FieldMappingEntry {
    type Error = String;

    fn try_from(value: FieldMappingEntryForSerialization) -> Result<Self, String> {
        validate_field_mapping_name(&value.name).map_err(|err| err.to_string())?;
        let quickwit_field_type =
            QuickwitFieldType::parse_type_id(&value.type_id).ok_or_else(|| {
                format!(
                    "Field `{}` has an unknown type: `{}`.",
                    &value.name, &value.type_id
                )
            })?;
        let mapping_type = deserialize_mapping_type(
            quickwit_field_type,
            serde_json::Value::Object(value.field_mapping_json),
        )
        .map_err(|err| format!("Error while parsing field `{}`: {}", value.name, err))?;
        Ok(FieldMappingEntry::new(value.name, mapping_type))
    }
}

/// Serialize object into a `Map` of json values.
fn serialize_to_map<S: Serialize>(val: &S) -> Option<serde_json::Map<String, serde_json::Value>> {
    let json_val = serde_json::to_value(val).ok()?;
    if let serde_json::Value::Object(map) = json_val {
        Some(map)
    } else {
        None
    }
}

fn typed_mapping_to_json_params(
    field_mapping_type: FieldMappingType,
) -> serde_json::Map<String, serde_json::Value> {
    match field_mapping_type {
        FieldMappingType::Text(text_options, _) => serialize_to_map(&text_options),
        FieldMappingType::U64(options, _)
        | FieldMappingType::I64(options, _)
        | FieldMappingType::Date(options, _)
        | FieldMappingType::Bytes(options, _)
        | FieldMappingType::F64(options, _) => serialize_to_map(&options),
        FieldMappingType::Object(object_options) => serialize_to_map(&object_options),
    }
    .unwrap()
}

impl From<FieldMappingEntry> for FieldMappingEntryForSerialization {
    fn from(field_mapping_entry: FieldMappingEntry) -> FieldMappingEntryForSerialization {
        let type_id = field_mapping_entry
            .mapping_type
            .quickwit_field_type()
            .to_type_id();
        let field_mapping_json = typed_mapping_to_json_params(field_mapping_entry.mapping_type);
        FieldMappingEntryForSerialization {
            name: field_mapping_entry.name,
            type_id,
            field_mapping_json,
        }
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
    use matches::matches;
    use serde_json::json;
    use tantivy::schema::{Cardinality, IndexRecordOption, Value};
    use tantivy::time::{Date, Month, PrimitiveDateTime, Time};
    use tantivy::DateTime;

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
                assert_eq!(options.stored, true);
                assert_eq!(options.indexed, true);
                assert_eq!(options.tokenizer.unwrap(), "english");
                assert_eq!(options.record, IndexRecordOption::Basic);
            }
            _ => panic!("wrong property type"),
        }
        Ok(())
    }

    #[test]
    fn test_deserialize_valid_fieldnorms() -> anyhow::Result<()> {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
        {
            "name": "my_field_name",
            "type": "text",
            "stored": true,
            "indexed": true,
            "fieldnorms": true,
            "record": "basic",
            "tokenizer": "english"
        }"#,
        );
        match result.unwrap().mapping_type {
            FieldMappingType::Text(options, _) => {
                assert_eq!(options.stored, true);
                assert_eq!(options.indexed, true);
                assert_eq!(options.fieldnorms, true);
            }
            _ => panic!("wrong property type"),
        }

        Ok(())
    }

    #[test]
    fn test_error_on_text_with_invalid_options() {
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
            "Error while parsing field `my_field_name`: `record`, `tokenizer`, and `fieldnorms` \
             parameters are allowed only if indexed is true."
        );
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
            FieldMappingType::Object(options) => {
                assert_eq!(options.field_mappings.len(), 1);
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
            "Error while parsing field `my_field_name`: object type must have at least one field \
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
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error while parsing field `my_field_name`: unknown field `tokenizer`, expected one \
             of `stored`, `indexed`, `fast`"
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
                assert_eq!(options.indexed, true); // default
                assert_eq!(options.fast, false); // default
                assert_eq!(options.stored, true); // default
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
                assert_eq!(options.indexed, true); // default
                assert_eq!(options.fast, false); // default
                assert_eq!(options.stored, true); // default
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
        let entry_str = serde_json::to_value(&entry)?;
        assert_eq!(
            entry_str,
            serde_json::json!({
                "name": "my_field_name",
                "type": "i64",
                "stored": true,
                "fast": false,
                "indexed": true
            })
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
            "Error while parsing field `my_field_name`: unknown field `tokenizer`, expected one \
             of `stored`, `indexed`, `fast`"
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
                assert_eq!(options.indexed, true); // default
                assert_eq!(options.fast, false); // default
                assert_eq!(options.stored, true); // default
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
                assert_eq!(options.indexed, true); // default
                assert_eq!(options.fast, false); // default
                assert_eq!(options.stored, true); // default
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
        let entry_str = serde_json::to_value(&entry)?;
        assert_eq!(
            entry_str,
            serde_json::json!({
                "name": "my_field_name",
                "type":"u64",
                "stored": true,
                "fast": false,
                "indexed": true
            })
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

        let datetime = PrimitiveDateTime::new(
            Date::from_calendar_date(2021, Month::December, 19).unwrap(),
            Time::from_hms(17, 39, 57).unwrap(),
        );

        let datetime_utc = DateTime::new_primitive(datetime);
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

    #[test]
    fn test_multivalued_bytes_fast_forbidden() -> anyhow::Result<()> {
        let field_mapping_entry_parse_error = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<bytes>",
                "fast": true
            }
            "#,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            field_mapping_entry_parse_error,
            "Error while parsing field `my_field_name`: fast field is not allowed for \
             array<bytes>."
        );
        Ok(())
    }
}
