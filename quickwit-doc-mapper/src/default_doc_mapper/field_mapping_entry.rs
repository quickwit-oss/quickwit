// Copyright (C) 2022 Quickwit, Inc.
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
use serde::{Deserialize, Serialize};
use tantivy::schema::{
    Cardinality, IndexRecordOption, JsonObjectOptions, TextFieldIndexing, TextOptions, Type,
};

use super::date_time_type::QuickwitDateOptions;
use super::{default_as_true, FieldMappingType};
use crate::default_doc_mapper::field_mapping_type::QuickwitFieldType;
use crate::default_doc_mapper::validate_field_mapping_name;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct QuickwitObjectOptions {
    pub field_mappings: Vec<FieldMappingEntry>,
}

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

/// Struct used for serialization and deserialization
/// Main advantage: having a flat structure and gain flexibility
/// if we want to add some syntactic sugar in the mapping.
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
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "default_as_true")]
    pub stored: bool,
    #[serde(default = "default_as_true")]
    pub indexed: bool,
    #[serde(default)]
    pub fast: bool,
}

impl Default for QuickwitNumericOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            stored: true,
            fast: false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum QuickwitTextTokenizer {
    #[serde(rename = "raw")]
    Raw,
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "en_stem")]
    StemEn,
}

impl QuickwitTextTokenizer {
    pub fn get_name(&self) -> &str {
        match self {
            QuickwitTextTokenizer::Raw => "raw",
            QuickwitTextTokenizer::Default => "default",
            QuickwitTextTokenizer::StemEn => "en_stem",
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QuickwitTextOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "default_as_true")]
    pub indexed: bool,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<QuickwitTextTokenizer>,
    #[serde(default)]
    pub record: IndexRecordOption,
    #[serde(default)]
    pub fieldnorms: bool,
    #[serde(default = "default_as_true")]
    pub stored: bool,
    #[serde(default)]
    pub fast: bool,
}

impl Default for QuickwitTextOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            tokenizer: None,
            record: IndexRecordOption::Basic,
            fieldnorms: false,
            stored: true,
            fast: false,
        }
    }
}

impl From<QuickwitTextOptions> for TextOptions {
    fn from(quickwit_text_options: QuickwitTextOptions) -> Self {
        let mut text_options = TextOptions::default();
        if quickwit_text_options.stored {
            text_options = text_options.set_stored();
        }
        if quickwit_text_options.fast {
            text_options = text_options.set_fast();
        }
        if quickwit_text_options.indexed {
            let mut text_field_indexing = TextFieldIndexing::default();
            if let Some(tokenizer) = quickwit_text_options.tokenizer {
                text_field_indexing = text_field_indexing.set_tokenizer(tokenizer.get_name());
            }
            text_field_indexing =
                text_field_indexing.set_index_option(quickwit_text_options.record);
            text_options = text_options.set_indexing_options(text_field_indexing);
        }
        text_options
    }
}

fn default_json_tokenizer() -> QuickwitTextTokenizer {
    QuickwitTextTokenizer::Default
}

/// Options associated to a json field.
///
/// `QuickwitJsonOptions` is also used to configure
/// the dynamic mapping.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuickwitJsonOptions {
    /// Optional description of JSON object.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// If true, all of the element in the json object will be indexed.
    #[serde(default = "default_as_true")]
    pub indexed: bool,
    /// Sets the tokenize that should be used with the text fields in the
    /// json object.
    #[serde(default = "default_json_tokenizer")]
    pub tokenizer: QuickwitTextTokenizer,
    /// Sets how much information should be added in the index
    /// with each token.
    ///
    /// Setting `record` is only allowed if indexed == true.
    #[serde(default)]
    pub record: IndexRecordOption,
    /// If true, the field will be stored in the doc store.
    #[serde(default = "default_as_true")]
    pub stored: bool,
}

impl Default for QuickwitJsonOptions {
    fn default() -> Self {
        QuickwitJsonOptions {
            description: None,
            indexed: true,
            tokenizer: default_json_tokenizer(),
            record: IndexRecordOption::Basic,
            stored: true,
        }
    }
}

impl From<QuickwitJsonOptions> for JsonObjectOptions {
    fn from(quickwit_json_options: QuickwitJsonOptions) -> Self {
        let mut json_options = JsonObjectOptions::default();
        if quickwit_json_options.stored {
            json_options = json_options.set_stored();
        }
        if quickwit_json_options.indexed {
            let mut text_field_indexing = TextFieldIndexing::default();
            text_field_indexing =
                text_field_indexing.set_tokenizer(quickwit_json_options.tokenizer.get_name());
            text_field_indexing =
                text_field_indexing.set_index_option(quickwit_json_options.record);
            json_options = json_options.set_indexing_options(text_field_indexing);
        }
        json_options
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
        Type::Bool => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::Bool(numeric_options, cardinality))
        }
        Type::Date => {
            let date_time_options = serde_json::from_value::<QuickwitDateOptions>(json)?;
            Ok(FieldMappingType::Date(date_time_options, cardinality))
        }
        Type::Facet => unimplemented!("Facet are not supported in quickwit yet."),
        Type::Bytes => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            if numeric_options.fast && cardinality == Cardinality::MultiValues {
                bail!("fast field is not allowed for array<bytes>.");
            }
            Ok(FieldMappingType::Bytes(numeric_options, cardinality))
        }
        Type::Json => {
            let json_options: QuickwitJsonOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::Json(json_options, cardinality))
        }
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
        Ok(FieldMappingEntry {
            name: value.name,
            mapping_type,
        })
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
        | FieldMappingType::Bytes(options, _)
        | FieldMappingType::F64(options, _)
        | FieldMappingType::Bool(options, _) => serialize_to_map(&options),
        FieldMappingType::Date(date_time_options, _) => serialize_to_map(&date_time_options),
        FieldMappingType::Json(json_options, _) => serialize_to_map(&json_options),
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

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use matches::matches;
    use serde_json::json;
    use tantivy::schema::{Cardinality, IndexRecordOption};

    use super::FieldMappingEntry;
    use crate::default_doc_mapper::field_mapping_entry::{
        QuickwitJsonOptions, QuickwitTextTokenizer,
    };
    use crate::default_doc_mapper::FieldMappingType;

    const TEXT_MAPPING_ENTRY_VALUE: &str = r#"
        {
            "name": "my_field_name",
            "type": "text",
            "stored": true,
            "record": "basic",
            "tokenizer": "en_stem"
        }
    "#;

    const TEXT_MAPPING_ENTRY_VALUE_INVALID_TOKENIZER: &str = r#"
        {
            "name": "my_field_name",
            "type": "text",
            "stored": true,
            "record": "basic",
            "tokenizer": "notexist"
        }
    "#;

    const JSON_MAPPING_ENTRY_UNKNOWN_FIELD: &str = r#"
        {
            "name": "my_field_name",
            "type": "json",
            "blub": true
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
    fn test_deserialize_invalid_text_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry =
            serde_json::from_str::<FieldMappingEntry>(TEXT_MAPPING_ENTRY_VALUE_INVALID_TOKENIZER);
        assert!(mapping_entry.is_err());
        assert_eq!(
            mapping_entry.unwrap_err().to_string(),
            "Error while parsing field `my_field_name`: unknown variant `notexist`, expected one \
             of `raw`, `default`, `en_stem`"
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_invalid_json_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry =
            serde_json::from_str::<FieldMappingEntry>(JSON_MAPPING_ENTRY_UNKNOWN_FIELD);
        assert!(mapping_entry.is_err());
        assert_eq!(
            mapping_entry.unwrap_err().to_string(),
            "Error while parsing field `my_field_name`: unknown field `blub`, expected one of \
             `description`, `indexed`, `tokenizer`, `record`, `stored`"
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_text_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(TEXT_MAPPING_ENTRY_VALUE)?;
        assert_eq!(mapping_entry.name, "my_field_name");
        match mapping_entry.mapping_type {
            FieldMappingType::Text(options, _) => {
                assert_eq!(options.stored, true);
                assert_eq!(options.indexed, true);
                assert_eq!(options.tokenizer.unwrap().get_name(), "en_stem");
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
            "tokenizer": "en_stem"
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
    fn test_deserialize_object_mapping_entry() {
        let mapping_entry =
            serde_json::from_str::<FieldMappingEntry>(OBJECT_MAPPING_ENTRY_VALUE).unwrap();
        assert_eq!(mapping_entry.name, "my_field_name");
        match mapping_entry.mapping_type {
            FieldMappingType::Object(options) => {
                assert_eq!(options.field_mappings.len(), 1);
            }
            _ => panic!("wrong property type"),
        }
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
    fn test_deserialize_mapping_with_unknown_type() {
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
    fn test_deserialize_i64_mapping_with_invalid_name() {
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
    fn test_deserialize_i64_parsing_error_with_text_options() {
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
             of `description`, `stored`, `indexed`, `fast`"
        );
    }

    #[test]
    fn test_deserialize_i64_mapping_multivalued() -> anyhow::Result<()> {
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
    fn test_deserialize_i64_mappping_singlevalued() -> anyhow::Result<()> {
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
    fn test_serialize_i64_mapping() -> anyhow::Result<()> {
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
    fn test_deserialize_u64_mapping_with_wrong_options() {
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
             of `description`, `stored`, `indexed`, `fast`"
        );
    }

    #[test]
    fn test_deserialize_u64_u64_mapping_multivalued() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<u64>"
            }
            "#,
        )
        .unwrap();

        if let FieldMappingType::U64(options, cardinality) = result.mapping_type {
            assert_eq!(options.indexed, true); // default
            assert_eq!(options.fast, false); // default
            assert_eq!(options.stored, true); // default
            assert_eq!(cardinality, Cardinality::MultiValues);
        } else {
            panic!("Wrong type");
        }
    }

    #[test]
    fn test_deserialize_u64_mapping_singlevalued() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "u64"
            }
            "#,
        )
        .unwrap();
        if let FieldMappingType::U64(options, cardinality) = result.mapping_type {
            assert_eq!(options.indexed, true); // default
            assert_eq!(options.fast, false); // default
            assert_eq!(options.stored, true); // default
            assert_eq!(cardinality, Cardinality::SingleValue);
        } else {
            panic!("Wrong type");
        }
    }

    #[test]
    fn test_serialize_u64_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "u64"
            }
            "#,
        )
        .unwrap();
        let entry_str = serde_json::to_value(&entry).unwrap();
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
    }

    #[test]
    fn test_parse_f64_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "f64"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type":"f64",
                "stored": true,
                "fast": false,
                "indexed": true
            })
        );
    }

    #[test]
    fn test_parse_bool_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "bool"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "bool",
                "stored": true,
                "fast": false,
                "indexed": true
            })
        );
    }

    #[test]
    fn test_parse_text_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "text"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "text",
                "fast": false,
                "stored": true,
                "indexed": true,
                "fieldnorms": false,
                "record": "basic",
            })
        );
    }

    #[test]
    fn test_parse_text_mapping_multivalued() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<text>"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "array<text>",
                "stored": true,
                "indexed": true,
                "fieldnorms": false,
                "fast": false,
                "record": "basic",
            })
        );
    }

    #[test]
    fn test_parse_date_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "date"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "date",
                "input_formats": ["rfc3339", "unix_ts_secs"],
                "precision": "seconds",
                "stored": true,
                "indexed": true,
                "fast": false,
            })
        );
    }

    #[test]
    fn test_parse_date_arr_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<date>",
                "precision": "milliseconds"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "array<date>",
                "input_formats": ["rfc3339", "unix_ts_secs"],
                "precision": "milliseconds",
                "stored": true,
                "indexed": true,
                "fast": false,
            })
        );
    }

    #[test]
    fn test_parse_bytes_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "bytes"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "bytes",
                "stored": true,
                "indexed": true,
                "fast": false,
            })
        );
    }

    #[test]
    fn test_parse_bytes_mapping_arr() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<bytes>"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "array<bytes>",
                "stored": true,
                "indexed": true,
                "fast": false,
            })
        );
    }

    #[test]
    fn test_parse_bytes_mapping_arr_and_fast_forbidden() {
        let err = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "array<bytes>",
                "fast": true
            }
            "#,
        )
        .err()
        .unwrap();
        assert_eq!(
            err.to_string(),
            "Error while parsing field `my_field_name`: fast field is not allowed for \
             array<bytes>.",
        );
    }

    #[test]
    fn test_parse_json_mapping_singlevalue() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "type": "json",
                "name": "my_json_field",
                "stored": true
            }
            "#,
        )
        .unwrap();
        let expected_json_options = QuickwitJsonOptions {
            description: None,
            indexed: true,
            tokenizer: QuickwitTextTokenizer::Default,
            record: IndexRecordOption::Basic,
            stored: true,
        };
        assert_eq!(&field_mapping_entry.name, "my_json_field");
        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::Json(json_config,
            Cardinality::SingleValue) if json_config == expected_json_options)
        );
    }

    #[test]
    fn test_quickwit_json_options_default_tokenizer_is_default() {
        let quickwit_json_options = QuickwitJsonOptions::default();
        assert_eq!(
            quickwit_json_options.tokenizer,
            QuickwitTextTokenizer::Default
        );
    }

    #[test]
    fn test_quickwit_json_options_default_consistent_with_default() {
        let quickwit_json_options: QuickwitJsonOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(quickwit_json_options, QuickwitJsonOptions::default());
    }

    #[test]
    fn test_parse_json_mapping_multivalued() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "type": "array<json>",
                "name": "my_json_field_multi",
                "tokenizer": "raw",
                "stored": false
            }
            "#,
        )
        .unwrap();
        let expected_json_options = QuickwitJsonOptions {
            description: None,
            indexed: true,
            tokenizer: QuickwitTextTokenizer::Raw,
            record: IndexRecordOption::Basic,
            stored: false,
        };
        assert_eq!(&field_mapping_entry.name, "my_json_field_multi");
        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::Json(json_config,
    Cardinality::MultiValues) if json_config == expected_json_options)
        );
    }

    #[test]
    fn test_serialize_i64_with_description_field() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "description": "If you see this description, your test is failed",
                "type": "i64"
            }"#,
        )
        .unwrap();

        let entry_str = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_str,
            serde_json::json!({
                "name": "my_field_name",
                "description": "If you see this description, your test is failed",
                "type": "i64",
                "stored": true,
                "fast": false,
                "indexed": true
            })
        );
    }

    #[test]
    fn test_serialize_text_with_description_field() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "description": "If you see this description, your test is failed",
                "type": "text"
            }"#,
        )
        .unwrap();

        let entry_str = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_str,
            serde_json::json!({
                "name": "my_field_name",
                "description": "If you see this description, your test is failed",
                "type": "text",
                "fast": false,
                "stored": true,
                "indexed": true,
                "fieldnorms": false,
                "record": "basic"
            })
        );
    }
    #[test]
    fn test_serialize_json_with_description_field() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "description": "If you see this description, your test is failed",
                "type": "json"
            }"#,
        )
        .unwrap();

        let entry_str = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_str,
            serde_json::json!({
                "name": "my_field_name",
                "description": "If you see this description, your test is failed",
                "type": "json",
                "tokenizer": "default",
                "record": "basic",
                "stored": true,
                "indexed": true
            })
        );
    }
}
