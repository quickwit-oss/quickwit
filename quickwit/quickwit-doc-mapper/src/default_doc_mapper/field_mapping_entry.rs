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

use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::bail;
use indexmap::IndexSet;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use tantivy::schema::{
    IndexRecordOption, JsonObjectOptions, TextFieldIndexing, TextOptions, Type,
    Value as TantivyValue,
};

use super::date_time_type::QuickwitDateTimeOptions;
use super::{default_as_true, FieldMappingType};
use crate::default_doc_mapper::field_mapping_type::QuickwitFieldType;
use crate::default_doc_mapper::validate_field_mapping_name;
use crate::Cardinality;

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct QuickwitObjectOptions {
    pub field_mappings: Vec<FieldMappingEntry>,
}

/// A `FieldMappingEntry` defines how a field is indexed, stored,
/// and mapped from a JSON document to the related index fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    try_from = "FieldMappingEntryForSerialization",
    into = "FieldMappingEntryForSerialization"
)]
pub struct FieldMappingEntry {
    /// Field name in the index schema.
    pub name: String,
    /// Property parameters which defines the type and the way the value must be indexed.
    pub(crate) mapping_type: FieldMappingType,
}

// Struct used for serialization and deserialization
// Main advantage: having a flat structure and gain flexibility
// if we want to add some syntactic sugar in the mapping.
// Main drawback: we have a bunch of mixed parameters in it but
// seems to be reasonable.
//
// We do not rely on enum with inline tagging and flatten because
// - serde does not support it in combination with `deny_unknown_field`
// - it is clumsy to handle `array<type>` keys.

// Docs bellow used for OpenAPI generation:
/// A `FieldMappingEntry` defines how a field is indexed, stored,
/// and mapped from a JSON document to the related index fields.
///
/// Property parameters which defines the way the value must be indexed.
///
/// Properties are determined by the specified type, for more information
/// please see: <https://quickwit.io/docs/configuration/index-config#field-types>
#[derive(Clone, Serialize, Deserialize, Debug, utoipa::ToSchema)]
pub(crate) struct FieldMappingEntryForSerialization {
    /// Field name in the index schema.
    name: String,
    #[serde(rename = "type")]
    type_id: String,
    #[serde(flatten)]
    #[schema(value_type = HashMap<String, Object>)]
    pub field_mapping_json: serde_json::Map<String, JsonValue>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
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
    #[serde(default)]
    pub input_formats: NumericInputFormats,
    #[serde(default)]
    pub output_format: NumericFormat,
}

impl QuickwitNumericOptions {
    pub fn format_i64_to_json(&self, value: i64) -> serde_json::Value {
        match self.output_format {
            NumericFormat::Decimal => value.into(),
            NumericFormat::DecimalStr => value.to_string().into(),
            NumericFormat::Hex => format!("{value:x}").into(),
        }
    }

    pub fn format_u64_to_json(&self, value: u64) -> serde_json::Value {
        match self.output_format {
            NumericFormat::Decimal => value.into(),
            NumericFormat::DecimalStr => value.to_string().into(),
            NumericFormat::Hex => format!("{value:x}").into(),
        }
    }

    pub fn format_f64_to_json(&self, value: f64) -> serde_json::Value {
        match self.output_format {
            NumericFormat::Decimal => value.into(),
            NumericFormat::DecimalStr => value.to_string().into(),
            NumericFormat::Hex => format!("{:x}", value.to_bits()).into(),
        }
    }
}

impl Default for QuickwitNumericOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            stored: true,
            fast: false,
            input_formats: NumericInputFormats::default(),
            output_format: NumericFormat::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NumericInputFormats(pub Vec<NumericFormat>);

impl NumericInputFormats {
    pub fn parse_json_i64(&self, value: &serde_json::Value) -> Result<TantivyValue, String> {
        for format in &self.0 {
            if let Some(result) = format.parse_json_i64(value)? {
                return Ok(result);
            }
        }
        Err(format!("No format matched input value `{value}`."))
    }
    pub fn parse_json_u64(&self, value: &serde_json::Value) -> Result<TantivyValue, String> {
        for format in &self.0 {
            if let Some(result) = format.parse_json_u64(value)? {
                return Ok(result);
            }
        }
        Err(format!("No format matched input value `{value}`."))
    }
    pub fn parse_json_f64(&self, value: &serde_json::Value) -> Result<TantivyValue, String> {
        for format in &self.0 {
            if let Some(result) = format.parse_json_f64(value)? {
                return Ok(result);
            }
        }
        Err(format!("No format matched input value `{value}`."))
    }
}

impl From<Vec<NumericFormat>> for NumericInputFormats {
    fn from(formats: Vec<NumericFormat>) -> Self {
        Self(formats)
    }
}

impl<'de> Deserialize<'de> for NumericInputFormats {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let numeric_formats = IndexSet::<NumericFormat>::deserialize(deserializer)?;

        if numeric_formats.is_empty() {
            return Ok(NumericInputFormats::default());
        }

        if numeric_formats.contains(&NumericFormat::DecimalStr)
            && numeric_formats.contains(&NumericFormat::Hex)
        {
            return Err(D::Error::custom(
                "input format for numbers doesn't allow `hex` and `decimal_str` at the same time \
                 as the are ambiguous.",
            ));
        }

        Ok(NumericInputFormats(numeric_formats.into_iter().collect()))
    }
}

impl Default for NumericInputFormats {
    fn default() -> NumericInputFormats {
        Self(vec![NumericFormat::Decimal, NumericFormat::DecimalStr])
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub enum NumericFormat {
    #[default]
    Decimal,
    DecimalStr,
    Hex,
}

impl NumericFormat {
    pub fn as_str(&self) -> &str {
        match self {
            NumericFormat::Decimal => "decimal",
            NumericFormat::DecimalStr => "decimal_str",
            NumericFormat::Hex => "hex",
        }
    }

    pub fn parse_json_i64(
        &self,
        value: &serde_json::Value,
    ) -> Result<Option<TantivyValue>, String> {
        let number = match self {
            NumericFormat::Decimal if value.is_number() => {
                Some(value.as_i64().ok_or_else(|| {
                    format!("Expected i64, got inconvertible JSON number `{value}`.")
                })?)
            }
            NumericFormat::DecimalStr => value.as_str().and_then(|val| str::parse::<i64>(val).ok()),
            NumericFormat::Hex => value.as_str().and_then(|mut val| {
                if val.starts_with("0x") || val.starts_with("0X") {
                    val = &val[2..];
                }
                // we parse a u64 and cast it to i64 so 0xFF..F reads as -1, not a Positive
                // Overflow error
                u64::from_str_radix(val, 16).map(|val| val as i64).ok()
            }),
            NumericFormat::Decimal => None,
        };
        Ok(number.map(Into::into))
    }

    pub fn parse_json_u64(
        &self,
        value: &serde_json::Value,
    ) -> Result<Option<TantivyValue>, String> {
        let number = match self {
            NumericFormat::Decimal if value.is_number() => {
                Some(value.as_u64().ok_or_else(|| {
                    format!("Expected u64, got inconvertible JSON number `{value}`.")
                })?)
            }
            NumericFormat::DecimalStr => value.as_str().and_then(|val| str::parse::<u64>(val).ok()),
            NumericFormat::Hex => value.as_str().and_then(|mut val| {
                if val.starts_with("0x") || val.starts_with("0X") {
                    val = &val[2..];
                }
                u64::from_str_radix(val, 16).ok()
            }),
            NumericFormat::Decimal => None,
        };
        Ok(number.map(Into::into))
    }

    pub fn parse_json_f64(
        &self,
        value: &serde_json::Value,
    ) -> Result<Option<TantivyValue>, String> {
        let number = match self {
            NumericFormat::Decimal if value.is_number() => {
                Some(value.as_f64().ok_or_else(|| {
                    format!("Expected f64, got inconvertible JSON number `{value}`.")
                })?)
            }
            NumericFormat::DecimalStr => value.as_str().and_then(|val| str::parse::<f64>(val).ok()),
            NumericFormat::Hex => value.as_str().and_then(|mut val| {
                if val.starts_with("0x") || val.starts_with("0X") {
                    val = &val[2..];
                }
                // we accept the raw representation of floats, such that 0x7ff0000000000000==+Inf
                // TODO: should we really allow ±inf and NaN?
                u64::from_str_radix(val, 16).map(f64::from_bits).ok()
            }),
            NumericFormat::Decimal => None,
        };
        Ok(number.map(Into::into))
    }
}

impl FromStr for NumericFormat {
    type Err = String;

    fn from_str(numeric_format_str: &str) -> Result<Self, Self::Err> {
        let numeric_format = match numeric_format_str.to_lowercase().as_str() {
            "decimal" => NumericFormat::Decimal,
            "decimal_str" => NumericFormat::DecimalStr,
            "hex" => NumericFormat::Hex,
            _ => {
                return Err(format!("Unknown numeric format: `{numeric_format_str}`."));
            }
        };
        Ok(numeric_format)
    }
}

impl Serialize for NumericFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NumericFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let numeric_format_str: String = Deserialize::deserialize(deserializer)?;
        let numeric_format = numeric_format_str.parse().map_err(D::Error::custom)?;
        Ok(numeric_format)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitBytesOptions {
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

impl Default for QuickwitBytesOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            stored: true,
            fast: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitIpAddrOptions {
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

impl Default for QuickwitIpAddrOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            stored: true,
            fast: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub enum QuickwitTextTokenizer {
    #[serde(rename = "raw")]
    Raw,
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "en_stem")]
    StemEn,
    #[serde(rename = "chinese_compatible")]
    Chinese,
    #[serde(rename = "lowercase")]
    /// Does not tokenize, only lowercases the text.
    Lowercase,
}

impl QuickwitTextTokenizer {
    pub fn get_name(&self) -> &str {
        match self {
            QuickwitTextTokenizer::Raw => "raw",
            QuickwitTextTokenizer::Default => "default",
            QuickwitTextTokenizer::StemEn => "en_stem",
            QuickwitTextTokenizer::Chinese => "chinese_compatible",
            QuickwitTextTokenizer::Lowercase => "lowercase",
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitTextOptions {
    #[schema(value_type = String)]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "default_as_true")]
    pub indexed: bool,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<QuickwitTextTokenizer>,
    #[schema(value_type = IndexRecordOptionSchema)]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<IndexRecordOption>,
    #[serde(default)]
    pub fieldnorms: bool,
    #[serde(default = "default_as_true")]
    pub stored: bool,
    #[serde(default)]
    pub fast: FastFieldOptions,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FastFieldOptions {
    IsEnabled(bool),
    EnabledWithTokenizer { tokenizer: String },
}

impl Default for FastFieldOptions {
    fn default() -> Self {
        FastFieldOptions::IsEnabled(false)
    }
}

impl Default for QuickwitTextOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            tokenizer: None,
            record: None,
            fieldnorms: false,
            stored: true,
            fast: FastFieldOptions::default(),
        }
    }
}

impl From<QuickwitTextOptions> for TextOptions {
    fn from(quickwit_text_options: QuickwitTextOptions) -> Self {
        let mut text_options = TextOptions::default();
        if quickwit_text_options.stored {
            text_options = text_options.set_stored();
        }
        match &quickwit_text_options.fast {
            FastFieldOptions::IsEnabled(true) => {
                text_options = text_options.set_fast(None);
            }
            FastFieldOptions::EnabledWithTokenizer { tokenizer } => {
                text_options = text_options.set_fast(Some(tokenizer));
            }
            FastFieldOptions::IsEnabled(false) => {}
        }
        if quickwit_text_options.indexed {
            let index_record_option = quickwit_text_options
                .record
                .unwrap_or(IndexRecordOption::Basic);
            let tokenizer = quickwit_text_options
                .tokenizer
                .unwrap_or(QuickwitTextTokenizer::Default);
            let text_field_indexing = TextFieldIndexing::default()
                .set_index_option(index_record_option)
                .set_fieldnorms(quickwit_text_options.fieldnorms)
                .set_tokenizer(tokenizer.get_name());

            text_options = text_options.set_indexing_options(text_field_indexing);
        }
        text_options
    }
}

#[allow(unused)]
#[derive(utoipa::ToSchema)]
pub enum IndexRecordOptionSchema {
    /// records only the `DocId`s
    #[schema(rename = "basic")]
    Basic,
    /// records the document ids as well as the term frequency.
    /// The term frequency can help giving better scoring of the documents.
    #[schema(rename = "freq")]
    WithFreqs,
    /// records the document id, the term frequency and the positions of
    /// the occurrences in the document.
    #[schema(rename = "position")]
    WithFreqsAndPositions,
}

/// Options associated to a json field.
///
/// `QuickwitJsonOptions` is also used to configure
/// the dynamic mapping.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
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
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<QuickwitTextTokenizer>,
    /// Sets how much information should be added in the index
    /// with each token.
    ///
    /// Setting `record` is only allowed if indexed == true.
    #[schema(value_type = IndexRecordOptionSchema)]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<IndexRecordOption>,
    /// If true, the field will be stored in the doc store.
    #[serde(default = "default_as_true")]
    pub stored: bool,
    /// If true, the '.' in json keys will be expanded.
    #[serde(default = "default_as_true")]
    pub expand_dots: bool,
    /// If true, the json object will be stored in columnar format.
    #[serde(default)]
    pub fast: bool,
}

impl Default for QuickwitJsonOptions {
    fn default() -> Self {
        QuickwitJsonOptions {
            description: None,
            indexed: true,
            tokenizer: None,
            record: None,
            stored: true,
            expand_dots: true,
            fast: false,
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
            let index_record_option = quickwit_json_options
                .record
                .unwrap_or(IndexRecordOption::Basic);
            let tokenizer = quickwit_json_options
                .tokenizer
                .unwrap_or(QuickwitTextTokenizer::Default);
            let text_field_indexing = TextFieldIndexing::default()
                .set_tokenizer(tokenizer.get_name())
                .set_index_option(index_record_option);
            json_options = json_options.set_indexing_options(text_field_indexing);
        }
        if quickwit_json_options.expand_dots {
            json_options = json_options.set_expand_dots_enabled();
        }
        if quickwit_json_options.fast {
            json_options = json_options.set_fast();
        }
        json_options
    }
}

fn deserialize_mapping_type(
    quickwit_field_type: QuickwitFieldType,
    json: JsonValue,
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
                    || text_options.record.is_some()
                    || text_options.fieldnorms
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
        Type::IpAddr => {
            let ip_addr_options: QuickwitIpAddrOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::IpAddr(ip_addr_options, cardinality))
        }
        Type::Date => {
            let date_time_options = serde_json::from_value::<QuickwitDateTimeOptions>(json)?;
            Ok(FieldMappingType::DateTime(date_time_options, cardinality))
        }
        Type::Facet => unimplemented!("Facet are not supported in quickwit yet."),
        Type::Bytes => {
            let numeric_options: QuickwitBytesOptions = serde_json::from_value(json)?;
            if numeric_options.fast && cardinality == Cardinality::MultiValues {
                bail!("fast field is not allowed for array<bytes>.");
            }
            Ok(FieldMappingType::Bytes(numeric_options, cardinality))
        }
        Type::Json => {
            let json_options: QuickwitJsonOptions = serde_json::from_value(json)?;
            #[allow(clippy::collapsible_if)]
            if !json_options.indexed {
                if json_options.tokenizer.is_some() || json_options.record.is_some() {
                    bail!(
                        "`record` and `tokenizer` parameters are allowed only if indexed is true."
                    );
                }
            }
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
            JsonValue::Object(value.field_mapping_json),
        )
        .map_err(|err| format!("Error while parsing field `{}`: {}", value.name, err))?;
        Ok(FieldMappingEntry {
            name: value.name,
            mapping_type,
        })
    }
}

/// Serialize object into a `Map` of json values.
fn serialize_to_map<S: Serialize>(val: &S) -> Option<serde_json::Map<String, JsonValue>> {
    let json_val = serde_json::to_value(val).ok()?;
    if let JsonValue::Object(map) = json_val {
        Some(map)
    } else {
        None
    }
}

fn typed_mapping_to_json_params(
    field_mapping_type: FieldMappingType,
) -> serde_json::Map<String, JsonValue> {
    match field_mapping_type {
        FieldMappingType::Text(text_options, _) => serialize_to_map(&text_options),
        FieldMappingType::U64(options, _)
        | FieldMappingType::I64(options, _)
        | FieldMappingType::F64(options, _)
        | FieldMappingType::Bool(options, _) => serialize_to_map(&options),
        FieldMappingType::Bytes(options, _) => serialize_to_map(&options),
        FieldMappingType::IpAddr(options, _) => serialize_to_map(&options),
        FieldMappingType::DateTime(date_time_options, _) => serialize_to_map(&date_time_options),
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
    use tantivy::schema::{IndexRecordOption, JsonObjectOptions, TextOptions};

    use super::{FieldMappingEntry, NumericFormat};
    use crate::default_doc_mapper::field_mapping_entry::{
        QuickwitJsonOptions, QuickwitTextOptions, QuickwitTextTokenizer,
    };
    use crate::default_doc_mapper::FieldMappingType;
    use crate::Cardinality;

    #[test]
    fn test_quickwit_json_options_default() {
        let serde_default_json_options: QuickwitJsonOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(serde_default_json_options, QuickwitJsonOptions::default())
    }

    #[test]
    fn test_tantivy_text_options_from_quickwit_text_options() {
        let tantivy_text_option = TextOptions::from(QuickwitTextOptions::default());

        assert_eq!(tantivy_text_option.is_stored(), true);
        assert_eq!(tantivy_text_option.is_fast(), false);

        match tantivy_text_option.get_indexing_options() {
            Some(text_field_indexing) => {
                assert_eq!(text_field_indexing.index_option(), IndexRecordOption::Basic);
                assert_eq!(text_field_indexing.fieldnorms(), false);
                assert_eq!(text_field_indexing.tokenizer(), "default");
            }
            _ => panic!("text field indexing is None"),
        }
    }

    #[test]
    fn test_tantivy_json_options_from_quickwit_json_options() {
        let tantivy_json_option = JsonObjectOptions::from(QuickwitJsonOptions::default());

        assert_eq!(tantivy_json_option.is_stored(), true);

        match tantivy_json_option.get_text_indexing_options() {
            Some(text_field_indexing) => {
                assert_eq!(text_field_indexing.index_option(), IndexRecordOption::Basic);
                assert_eq!(text_field_indexing.tokenizer(), "default");
            }
            _ => panic!("text field indexing is None"),
        }
    }

    #[test]
    fn test_deserialize_text_mapping_entry_not_indexed() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "data_binary",
                "type": "text",
                "indexed": false,
                "stored": true
            }"#,
        )?;
        assert_eq!(mapping_entry.name, "data_binary");
        match mapping_entry.mapping_type {
            FieldMappingType::Text(options, _) => {
                assert_eq!(options.stored, true);
                assert_eq!(options.indexed, false);
                assert_eq!(options.record.is_some(), false);
            }
            _ => panic!("wrong property type"),
        }
        Ok(())
    }

    #[test]
    fn test_deserialize_text_mapping_entry_not_indexed_invalid() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "data_binary",
                "type": "text",
                "indexed": false,
                "record": "basic"
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error while parsing field `data_binary`: `record`, `tokenizer`, and `fieldnorms` \
             parameters are allowed only if indexed is true."
        );
    }

    #[test]
    fn test_deserialize_json_mapping_entry_not_indexed() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "data_binary",
                "type": "json",
                "indexed": false,
                "stored": true
            }"#,
        )?;
        assert_eq!(mapping_entry.name, "data_binary");
        match mapping_entry.mapping_type {
            FieldMappingType::Json(options, _) => {
                assert_eq!(options.stored, true);
                assert_eq!(options.indexed, false);
                assert_eq!(options.record.is_some(), false);
            }
            _ => panic!("wrong property type"),
        }
        Ok(())
    }

    #[test]
    fn test_deserialize_json_mapping_entry_not_indexed_invalid() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "data_binary",
                "type": "json",
                "indexed": false,
                "record": "basic"
            }
            "#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error while parsing field `data_binary`: `record` and `tokenizer` parameters are \
             allowed only if indexed is true."
        );
    }

    #[test]
    fn test_deserialize_invalid_text_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "text",
                "stored": true,
                "record": "basic",
                "tokenizer": "notexist"
            }
            "#,
        );
        assert!(mapping_entry.is_err());
        assert_eq!(
            mapping_entry.unwrap_err().to_string(),
            "Error while parsing field `my_field_name`: unknown variant `notexist`, expected one \
             of `raw`, `default`, `en_stem`, `chinese_compatible`, `lowercase`"
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_invalid_json_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
        {
            "name": "my_field_name",
            "type": "json",
            "blub": true
        }
    "#,
        );
        assert!(mapping_entry.is_err());
        assert!(mapping_entry
            .unwrap_err()
            .to_string()
            .contains("Error while parsing field `my_field_name`: unknown field `blub`"));
        Ok(())
    }

    #[test]
    fn test_deserialize_text_mapping_entry() -> anyhow::Result<()> {
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
        {
            "name": "my_field_name",
            "type": "text",
            "stored": true,
            "record": "basic",
            "tokenizer": "en_stem"
        }
        "#,
        )?;
        assert_eq!(mapping_entry.name, "my_field_name");
        match mapping_entry.mapping_type {
            FieldMappingType::Text(options, _) => {
                assert_eq!(options.stored, true);
                assert_eq!(options.indexed, true);
                assert_eq!(options.tokenizer.unwrap().get_name(), "en_stem");
                assert_eq!(options.record.unwrap(), IndexRecordOption::Basic);
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
        let mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
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
            "#,
        )
        .unwrap();
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
             of `description`, `stored`, `indexed`, `fast`, `input_formats`, `output_format`"
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
    fn test_deserialize_i64_mapping_singlevalued() -> anyhow::Result<()> {
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
                "indexed": true,
                "input_formats": ["decimal", "decimal_str"],
                "output_format": "decimal",
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
             of `description`, `stored`, `indexed`, `fast`, `input_formats`, `output_format`"
        );
    }

    #[test]
    fn test_deserialize_u64_mapping_with_wrong_input_format() {
        assert_eq!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "my_field_name",
                "type": "u64",
                "input_formats": ["not_decimal"]
            }"#
            )
            .unwrap_err()
            .to_string(),
            "Error while parsing field `my_field_name`: Unknown numeric format: `not_decimal`."
        );
    }

    #[test]
    fn test_deserialize_u64_mapping_with_ambiguous_input_format() {
        assert_eq!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "my_field_name",
                "type": "u64",
                "input_formats": ["decimal_str", "hex"]
            }"#
            )
            .unwrap_err()
            .to_string(),
            "Error while parsing field `my_field_name`: input format for numbers doesn't allow \
             `hex` and `decimal_str` at the same time as the are ambiguous."
        );
    }

    #[test]
    fn test_deserialize_u64_mapping_with_input_format() {
        let result = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "u64",
                "input_formats": ["decimal", "hex"]
            }"#,
        )
        .unwrap();
        if let FieldMappingType::U64(options, _) = result.mapping_type {
            assert_eq!(
                options.input_formats.0,
                vec![NumericFormat::Decimal, NumericFormat::Hex]
            );
        } else {
            panic!("Wrong type");
        }
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
                "indexed": true,
                "input_formats": ["decimal", "decimal_str"],
                "output_format": "decimal"
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
                "indexed": true,
                "input_formats": ["decimal", "decimal_str"],
                "output_format": "decimal",
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
                "indexed": true,
                "input_formats": ["decimal", "decimal_str"],
                "output_format": "decimal",
            })
        );
    }

    #[test]
    fn test_parse_ip_addr_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "ip_address",
                "description": "Client IP address",
                "type": "ip"
            }
            "#,
        )
        .unwrap();
        let entry_str = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_str,
            serde_json::json!({
                "name": "ip_address",
                "description": "Client IP address",
                "type": "ip",
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
            })
        );
    }

    #[test]
    fn test_parse_text_fast_field_tokenizer() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "text",
                "fast": {"tokenizer": "lowercase"}
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
                "fast": {"tokenizer": "lowercase"},
                "stored": true,
                "indexed": true,
                "fieldnorms": false,
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
            })
        );
    }

    #[test]
    fn test_parse_date_mapping() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "datetime"
            }
            "#,
        )
        .unwrap();
        let entry_deserser = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_deserser,
            json!({
                "name": "my_field_name",
                "type": "datetime",
                "input_formats": ["rfc3339", "unix_timestamp"],
                "output_format": "rfc3339",
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
                "type": "array<datetime>",
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
                "type": "array<datetime>",
                "input_formats": ["rfc3339", "unix_timestamp"],
                "output_format": "rfc3339",
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
            tokenizer: None,
            record: None,
            stored: true,
            fast: false,
            expand_dots: true,
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
        assert_eq!(quickwit_json_options.tokenizer, None);
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
                "stored": false,
                "fast": false
            }
            "#,
        )
        .unwrap();
        let expected_json_options = QuickwitJsonOptions {
            description: None,
            indexed: true,
            tokenizer: Some(QuickwitTextTokenizer::Raw),
            record: None,
            stored: false,
            expand_dots: true,
            fast: false,
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
                "indexed": true,
                "input_formats": ["decimal", "decimal_str"],
                "output_format": "decimal"
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
            })
        );
    }
    #[test]
    fn test_serialize_json_with_description_field() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "description": "If you see this description, your test failed",
                "type": "json"
            }"#,
        )
        .unwrap();

        let entry_str = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_str,
            serde_json::json!({
                "name": "my_field_name",
                "description": "If you see this description, your test failed",
                "type": "json",
                "stored": true,
                "indexed": true,
                "fast": false,
                "expand_dots": true,
            })
        );
    }
}
