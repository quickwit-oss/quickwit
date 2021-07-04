/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use crate::default_doc_mapper::is_valid_field_mapping_name;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use std::convert::TryFrom;
use tantivy::schema::{BytesOptions, FieldType};
use tantivy::schema::{
    Cardinality, DocParsingError as TantivyDocParser, IndexRecordOption, IntOptions,
    TextFieldIndexing, TextOptions, Value,
};
use thiserror::Error;

use super::FieldMappingType;
use super::{default_as_true, TANTIVY_DOT_SYMBOL};

/// A `FieldMappingEntry` defines how a field is indexed, stored
/// and how it is mapped from a json document to the related index fields.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(
    try_from = "FieldMappingEntryForSerialization",
    into = "FieldMappingEntryForSerialization"
)]
pub struct FieldMappingEntry {
    /// Field name in the index schema.
    name: String,
    /// Property parameters which defines the type and the way the value must be indexed.
    mapping_type: FieldMappingType,
}

impl FieldMappingEntry {
    pub fn new(name: String, mapping_type: FieldMappingType) -> Self {
        assert!(is_valid_field_mapping_name(&name));
        FieldMappingEntry { name, mapping_type }
    }

    pub fn root(mapping_type: FieldMappingType) -> Self {
        FieldMappingEntry {
            name: "".to_owned(),
            mapping_type,
        }
    }

    /// Return field entries that must be added to the schema.
    // TODO: can be more efficient to pass a collector in argument (a schema builder)
    // on which we add entry fields.
    pub fn field_entries(&self) -> anyhow::Result<Vec<(FieldPath, FieldType)>> {
        let field_path = FieldPath::new(&self.name);
        let results = match &self.mapping_type {
            FieldMappingType::Text(options, _) => {
                vec![(field_path, FieldType::Str(options.clone()))]
            }
            FieldMappingType::I64(options, _) => {
                vec![(field_path, FieldType::I64(options.clone()))]
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
                .map(|mapping_entry| mapping_entry.field_entries())
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .map(|(path, entry)| (path.with_parent(&self.name), entry))
                .collect(),
        };
        Ok(results)
    }

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
    pub fn parse(
        &self,
        json_value: &JsonValue,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        match &self.mapping_type {
            FieldMappingType::Text(options, cardinality) => {
                self.parse_text(json_value, options, cardinality)
            }
            FieldMappingType::I64(options, cardinality) => {
                self.parse_i64(&json_value, options, cardinality)
            }
            FieldMappingType::F64(options, cardinality) => {
                self.parse_f64(&json_value, options, cardinality)
            }
            FieldMappingType::Date(options, cardinality) => {
                self.parse_date(&json_value, options, cardinality)
            }
            FieldMappingType::Bytes(options, cardinality) => {
                self.parse_bytes(&json_value, options, cardinality)
            }
            FieldMappingType::Object(field_mappings) => {
                self.parse_object(json_value, field_mappings)
            }
        }
    }

    pub fn parse_text(
        &self,
        json_value: &JsonValue,
        options: &TextOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                array
                    .iter()
                    .map(|element| self.parse_text(&element, options, cardinality))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect()
            }
            JsonValue::String(value_as_str) => {
                vec![(
                    FieldPath::new(&self.name),
                    Value::Str(value_as_str.to_owned()),
                )]
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    "text type only support json string value".to_owned(),
                ));
            }
        };
        Ok(parsed_values)
    }

    pub fn parse_i64(
        &self,
        json_value: &JsonValue,
        options: &IntOptions,
        cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(array) => {
                if cardinality != &Cardinality::MultiValues {
                    return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
                }
                array
                    .iter()
                    .map(|element| self.parse_i64(&element, options, cardinality))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect()
            }
            JsonValue::Number(value_as_number) => {
                if let Some(value_as_i64) = value_as_number.as_i64() {
                    vec![(
                        FieldPath::new(&self.name),
                        Value::I64(value_as_i64.to_owned()),
                    )]
                } else {
                    return Err(DocParsingError::ValueError(
                        self.name.clone(),
                        "cannot convert json number to i64".to_owned(),
                    ));
                }
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    "i64 type only support json number".to_owned(),
                ))
            }
        };
        Ok(parsed_values)
    }

    pub fn parse_f64(
        &self,
        _json_value: &JsonValue,
        _options: &IntOptions,
        _cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        todo!()
    }

    pub fn parse_date(
        &self,
        _json_value: &JsonValue,
        _options: &IntOptions,
        _cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        todo!()
    }

    pub fn parse_bytes(
        &self,
        _json_value: &JsonValue,
        _options: &BytesOptions,
        _cardinality: &Cardinality,
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        todo!()
    }

    pub fn parse_object<'a>(
        &'a self,
        json_value: &JsonValue,
        entries: &'a [FieldMappingEntry],
    ) -> Result<Vec<(FieldPath, Value)>, DocParsingError> {
        let parsed_values = match json_value {
            JsonValue::Array(_) => {
                // TODO: we can support it but we need to do some extra validation on
                // the field mappings and they must be all multivalued.
                return Err(DocParsingError::MultiValuesNotSupported(self.name.clone()));
            }
            JsonValue::Object(object) => {
                entries
                    .iter()
                    .map(|entry| {
                        if let Some(child) = object.get(&entry.name) {
                            entry.parse(child)
                        } else {
                            // TODO: define if we need to raise an error or not.
                            Ok(vec![])
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .map(|(path, entry)| (path.with_parent(&self.name), entry))
                    .collect()
            }
            _ => {
                return Err(DocParsingError::ValueError(
                    self.name.clone(),
                    "object type only support json object".to_owned(),
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

    // Returns a tantivy compatible field name.
    pub fn tantivy_field_name(&self) -> String {
        // Some components can contains dots.
        self.field_name().replace(".", TANTIVY_DOT_SYMBOL)
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
        if !is_valid_field_mapping_name(&value.name) {
            bail!("Invalid field name: `{}`.", value.name)
        }
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
                }
            }
            FieldMappingType::I64(options, _)
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
        let mut indexing_options = TextFieldIndexing::default();
        if let Some(index_option) = self.record {
            indexing_options = indexing_options.set_index_option(index_option);
        }
        if let Some(tokenizer) = &self.tokenizer {
            indexing_options = indexing_options.set_tokenizer(tokenizer);
        }
        let mut options = TextOptions::default().set_indexing_options(indexing_options);
        if self.stored {
            options = options.set_stored();
        }
        Ok(FieldMappingType::Text(options, self.cardinality()))
    }

    fn new_i64(&self) -> anyhow::Result<FieldMappingType> {
        let options = self.int_options()?;
        Ok(FieldMappingType::I64(options, self.cardinality()))
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

    fn int_options(&self) -> anyhow::Result<IntOptions> {
        self.check_no_text_options()?;
        let mut options = IntOptions::default();
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
            bail!("Error when parsing `{}`: `record` and `tokenizer` parameters are for text field only.", self.name)
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
    #[error("The field '{0:?}' could not be parsed: {1:?}")]
    ValueError(String, String),
    /// The json-document contains a field that is not declared in the schema.
    #[error("The document contains a field that is not declared in the schema: {0:?}")]
    NoSuchFieldInSchema(String),
    /// The document contains a array of values but a single value is expected.
    #[error("The document contains an array of values but a single value is expected: {0:?}")]
    MultiValuesNotSupported(String),
}

impl From<TantivyDocParser> for DocParsingError {
    fn from(value: TantivyDocParser) -> Self {
        match value {
            TantivyDocParser::NoSuchFieldInSchema(text) => {
                DocParsingError::NoSuchFieldInSchema(text)
            }
            TantivyDocParser::NotJson(text) => DocParsingError::NoSuchFieldInSchema(text),
            TantivyDocParser::ValueError(text, error) => {
                DocParsingError::ValueError(text, format!("{:?}", error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use tantivy::schema::Cardinality;

    use crate::default_doc_mapper::FieldMappingType;

    use super::FieldMappingEntry;

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
        assert_eq!(error.to_string(), "Error when parsing field `my_field_name`: object type must have at least one field mapping.");
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
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "this is not ok",
                "type": "i64"
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid field name: `this is not ok`.");
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
            "Error when parsing `my_field_name`: `record` and `tokenizer` parameters are for text field only."
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
        assert_eq!(entry_str, "{\"name\":\"my_field_name\",\"type\":\"i64\",\"stored\":true,\"fast\":false,\"indexed\":true}");
        Ok(())
    }
}
