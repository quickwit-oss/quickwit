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

use std::borrow::Cow;
use std::convert::TryFrom;

use anyhow::bail;
use base64::prelude::{BASE64_STANDARD, Engine};
use once_cell::sync::Lazy;
use quickwit_common::true_fn;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tantivy::schema::{
    IndexRecordOption, JsonObjectOptions, OwnedValue as TantivyValue, TextFieldIndexing,
    TextOptions, Type,
};

use super::FieldMappingType;
use super::date_time_type::QuickwitDateTimeOptions;
use crate::doc_mapper::field_mapping_type::QuickwitFieldType;
use crate::{Cardinality, QW_RESERVED_FIELD_NAMES};

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
    /// Property parameters which define the type and the way the value must be indexed.
    pub mapping_type: FieldMappingType,
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
    #[serde(default = "true_fn")]
    pub stored: bool,
    #[serde(default = "true_fn")]
    pub indexed: bool,
    #[serde(default)]
    pub fast: bool,
    #[serde(default = "true_fn")]
    pub coerce: bool,
    #[serde(default)]
    pub output_format: NumericOutputFormat,
}

impl Default for QuickwitNumericOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            stored: true,
            fast: false,
            coerce: true,
            output_format: NumericOutputFormat::default(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitBoolOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "true_fn")]
    pub stored: bool,
    #[serde(default = "true_fn")]
    pub indexed: bool,
    #[serde(default)]
    pub fast: bool,
}

impl Default for QuickwitBoolOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            stored: true,
            fast: false,
        }
    }
}

/// Options associated to a bytes field.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitBytesOptions {
    /// Optional description of the bytes field.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// If true, the field will be stored in the doc store.
    #[serde(default = "true_fn")]
    pub stored: bool,
    /// If true, the field will be indexed.
    #[serde(default = "true_fn")]
    pub indexed: bool,
    /// If true, the field will be stored in columnar format.
    #[serde(default)]
    pub fast: bool,
    /// Input format of the bytes field.
    #[serde(default)]
    pub input_format: BinaryFormat,
    /// Output format of the bytes field.
    #[serde(default)]
    pub output_format: BinaryFormat,
}

impl Default for QuickwitBytesOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexed: true,
            stored: true,
            fast: false,
            input_format: BinaryFormat::default(),
            output_format: BinaryFormat::default(),
        }
    }
}

/// Available binary formats.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryFormat {
    /// Base64 format.
    #[default]
    Base64,
    /// Hexadecimal format.
    Hex,
}

impl BinaryFormat {
    /// Returns the string representation of the format.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Base64 => "base64",
            Self::Hex => "hex",
        }
    }

    /// Returns representation of the format in `serde_json::Value`.
    pub fn format_to_json(&self, value: &[u8]) -> JsonValue {
        match self {
            Self::Base64 => BASE64_STANDARD.encode(value).into(),
            Self::Hex => hex::encode(value).into(),
        }
    }

    /// Parses the `serde_json::Value` into `tantivy::schema::Value`.
    pub fn parse_str(&self, byte_str: &str) -> Result<Vec<u8>, String> {
        let payload = match self {
            Self::Base64 => BASE64_STANDARD
                .decode(byte_str)
                .map_err(|base64_decode_err| {
                    format!("expected base64 string, got `{byte_str}`: {base64_decode_err}")
                })?,
            Self::Hex => hex::decode(byte_str).map_err(|hex_decode_err| {
                format!("expected hex string, got `{byte_str}`: {hex_decode_err}")
            })?,
        };
        Ok(payload)
    }

    /// Parses the `serde_json::Value` into `tantivy::schema::Value`.
    pub fn parse_json(&self, json_val: &JsonValue) -> Result<TantivyValue, String> {
        let byte_str = if let JsonValue::String(byte_str) = json_val {
            byte_str
        } else {
            return Err(format!(
                "expected {} string, got `{json_val}`",
                self.as_str()
            ));
        };
        let payload = self.parse_str(byte_str)?;
        Ok(TantivyValue::Bytes(payload))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumericOutputFormat {
    #[default]
    Number,
    String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitIpAddrOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "true_fn")]
    pub stored: bool,
    #[serde(default = "true_fn")]
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

#[derive(Clone, PartialEq, Debug, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct QuickwitTextTokenizer(Cow<'static, str>);

pub(crate) const DEFAULT_TOKENIZER_NAME: &str = "default";

pub(crate) const RAW_TOKENIZER_NAME: &str = "raw";

impl Default for QuickwitTextTokenizer {
    fn default() -> Self {
        Self::from_static(DEFAULT_TOKENIZER_NAME)
    }
}

impl QuickwitTextTokenizer {
    pub const fn from_static(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }
    pub(crate) fn name(&self) -> &str {
        &self.0
    }
    pub fn raw() -> Self {
        Self::from_static(RAW_TOKENIZER_NAME)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuickwitTextNormalizer {
    Raw,
    Lowercase,
}

impl QuickwitTextNormalizer {
    pub fn get_name(&self) -> &str {
        match self {
            QuickwitTextNormalizer::Raw => "raw",
            QuickwitTextNormalizer::Lowercase => "lowercase",
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TextIndexingOptions {
    pub tokenizer: QuickwitTextTokenizer,
    pub record: IndexRecordOption,
    pub fieldnorms: bool,
}

impl TextIndexingOptions {
    fn from_parts_text(
        indexed: bool,
        tokenizer: Option<QuickwitTextTokenizer>,
        record: Option<IndexRecordOption>,
        fieldnorms: bool,
    ) -> anyhow::Result<Option<Self>> {
        if indexed {
            Ok(Some(TextIndexingOptions {
                tokenizer: tokenizer.unwrap_or_default(),
                record: record.unwrap_or(IndexRecordOption::Basic),
                fieldnorms,
            }))
        } else {
            if tokenizer.is_some() || record.is_some() || fieldnorms {
                bail!(
                    "`record`, `tokenizer`, and `fieldnorms` parameters are allowed only if \
                     indexed is true"
                )
            }
            Ok(None)
        }
    }

    fn from_parts_json(
        indexed: bool,
        tokenizer: Option<QuickwitTextTokenizer>,
        record: Option<IndexRecordOption>,
    ) -> anyhow::Result<Option<Self>> {
        if indexed {
            Ok(Some(TextIndexingOptions {
                tokenizer: tokenizer.unwrap_or_else(QuickwitTextTokenizer::raw),
                record: record.unwrap_or(IndexRecordOption::Basic),
                fieldnorms: false,
            }))
        } else {
            if tokenizer.is_some() || record.is_some() {
                bail!("`record` and `tokenizer` parameters are allowed only if indexed is true")
            }
            Ok(None)
        }
    }

    fn from_parts_concatenate(
        tokenizer: Option<QuickwitTextTokenizer>,
        record: Option<IndexRecordOption>,
    ) -> anyhow::Result<Self> {
        let text_index_options_opt = Self::from_parts_text(true, tokenizer, record, false)?;
        let text_index_options = text_index_options_opt.expect("concatenate field must be indexed");
        Ok(text_index_options)
    }

    fn to_parts_text(
        this: Option<Self>,
    ) -> (
        bool, // indexed
        Option<QuickwitTextTokenizer>,
        Option<IndexRecordOption>,
        bool, // fieldnorms
    ) {
        match this {
            Some(this) => (
                true,
                Some(this.tokenizer),
                Some(this.record),
                this.fieldnorms,
            ),
            None => (false, None, None, false),
        }
    }

    fn to_parts_json(
        this: Option<Self>,
    ) -> (
        bool, // indexed
        Option<QuickwitTextTokenizer>,
        Option<IndexRecordOption>,
    ) {
        let (indexed, tokenizer, record, _fieldorm) = TextIndexingOptions::to_parts_text(this);
        (indexed, tokenizer, record)
    }

    fn to_parts_concatenate(
        this: Self,
    ) -> (Option<QuickwitTextTokenizer>, Option<IndexRecordOption>) {
        let (_indexed, tokenizer, record, _fieldorm) =
            TextIndexingOptions::to_parts_text(Some(this));
        (tokenizer, record)
    }

    fn default_json() -> Self {
        TextIndexingOptions {
            tokenizer: QuickwitTextTokenizer::raw(),
            record: IndexRecordOption::Basic,
            fieldnorms: false,
        }
    }
}

impl Default for TextIndexingOptions {
    fn default() -> Self {
        TextIndexingOptions {
            tokenizer: QuickwitTextTokenizer::default(),
            record: IndexRecordOption::Basic,
            fieldnorms: false,
        }
    }
}

#[quickwit_macros::serde_multikey]
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitTextOptions {
    #[schema(value_type = String)]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde_multikey(
        deserializer = TextIndexingOptions::from_parts_text,
        serializer = TextIndexingOptions::to_parts_text,
        fields = (
            #[serde(default = "true_fn")]
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
        ),
    )]
    pub indexing_options: Option<TextIndexingOptions>,
    #[serde(default = "true_fn")]
    pub stored: bool,
    #[serde(default)]
    pub fast: FastFieldOptions,
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(
    into = "FastFieldOptionsForSerialization",
    from = "FastFieldOptionsForSerialization"
)]
pub enum FastFieldOptions {
    #[default]
    Disabled,
    EnabledWithNormalizer {
        normalizer: QuickwitTextNormalizer,
    },
}

impl FastFieldOptions {
    pub fn default_enabled() -> Self {
        FastFieldOptions::EnabledWithNormalizer {
            normalizer: QuickwitTextNormalizer::Raw,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum FastFieldOptionsForSerialization {
    IsEnabled(bool),
    EnabledWithNormalizer { normalizer: QuickwitTextNormalizer },
}

impl From<FastFieldOptionsForSerialization> for FastFieldOptions {
    fn from(fast_field_options: FastFieldOptionsForSerialization) -> Self {
        match fast_field_options {
            FastFieldOptionsForSerialization::IsEnabled(is_enabled) => {
                if is_enabled {
                    FastFieldOptions::default_enabled()
                } else {
                    FastFieldOptions::Disabled
                }
            }
            FastFieldOptionsForSerialization::EnabledWithNormalizer { normalizer } => {
                FastFieldOptions::EnabledWithNormalizer { normalizer }
            }
        }
    }
}

impl From<FastFieldOptions> for FastFieldOptionsForSerialization {
    fn from(fast_field_options: FastFieldOptions) -> Self {
        match fast_field_options {
            FastFieldOptions::Disabled => FastFieldOptionsForSerialization::IsEnabled(false),
            FastFieldOptions::EnabledWithNormalizer { normalizer } => {
                FastFieldOptionsForSerialization::EnabledWithNormalizer { normalizer }
            }
        }
    }
}

impl Default for QuickwitTextOptions {
    fn default() -> Self {
        Self {
            description: None,
            indexing_options: Some(TextIndexingOptions::default()),
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
            FastFieldOptions::EnabledWithNormalizer { normalizer } => {
                text_options = text_options.set_fast(Some(normalizer.get_name()));
            }
            FastFieldOptions::Disabled => {}
        }
        if let Some(indexing_options) = quickwit_text_options.indexing_options {
            let text_field_indexing = TextFieldIndexing::default()
                .set_index_option(indexing_options.record)
                .set_fieldnorms(indexing_options.fieldnorms)
                .set_tokenizer(indexing_options.tokenizer.name());

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
#[quickwit_macros::serde_multikey]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitJsonOptions {
    /// Optional description of JSON object.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde_multikey(
        deserializer = TextIndexingOptions::from_parts_json,
        serializer = TextIndexingOptions::to_parts_json,
        fields = (
            /// If true, all of the element in the json object will be indexed.
            #[serde(default = "true_fn")]
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
        ),
    )]
    /// Options for indexing text in a Json field.
    pub indexing_options: Option<TextIndexingOptions>,
    /// If true, the field will be stored in the doc store.
    #[serde(default = "true_fn")]
    pub stored: bool,
    /// If true, the '.' in json keys will be expanded.
    #[serde(default = "true_fn")]
    pub expand_dots: bool,
    /// If true, the json object will be stored in columnar format.
    #[serde(default)]
    pub fast: FastFieldOptions,
}

impl QuickwitJsonOptions {
    /// Build a default QuickwitJsonOptions for dynamic fields.
    pub fn default_dynamic() -> Self {
        QuickwitJsonOptions {
            fast: FastFieldOptions::default_enabled(),
            ..Default::default()
        }
    }
}

impl Default for QuickwitJsonOptions {
    fn default() -> Self {
        QuickwitJsonOptions {
            description: None,
            indexing_options: Some(TextIndexingOptions::default_json()),
            stored: true,
            expand_dots: true,
            fast: FastFieldOptions::default(),
        }
    }
}

impl From<QuickwitJsonOptions> for JsonObjectOptions {
    fn from(quickwit_json_options: QuickwitJsonOptions) -> Self {
        let mut json_options = JsonObjectOptions::default();
        if quickwit_json_options.stored {
            json_options = json_options.set_stored();
        }
        if let Some(indexing_options) = quickwit_json_options.indexing_options {
            let text_field_indexing = TextFieldIndexing::default()
                .set_tokenizer(indexing_options.tokenizer.name())
                .set_index_option(indexing_options.record);
            json_options = json_options.set_indexing_options(text_field_indexing);
        }
        if quickwit_json_options.expand_dots {
            json_options = json_options.set_expand_dots_enabled();
        }
        match &quickwit_json_options.fast {
            FastFieldOptions::EnabledWithNormalizer { normalizer } => {
                json_options = json_options.set_fast(Some(normalizer.get_name()));
            }
            FastFieldOptions::Disabled => {}
        }
        json_options
    }
}

/// Options associated to a concatenate field.
#[quickwit_macros::serde_multikey]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QuickwitConcatenateOptions {
    /// Optional description of JSON object.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Fields to concatenate
    #[serde(default)]
    pub concatenate_fields: Vec<String>,
    #[serde(default)]
    pub include_dynamic_fields: bool,
    #[serde_multikey(
        deserializer = TextIndexingOptions::from_parts_concatenate,
        serializer = TextIndexingOptions::to_parts_concatenate,
        fields = (
            /// Sets the tokenize that should be used with the text fields in the
            /// concatenate field.
            #[serde(default)]
            #[serde(skip_serializing_if = "Option::is_none")]
            pub tokenizer: Option<QuickwitTextTokenizer>,
            /// Sets how much information should be added in the index
            /// with each token.
            #[schema(value_type = IndexRecordOptionSchema)]
            #[serde(default)]
            #[serde(skip_serializing_if = "Option::is_none")]
            pub record: Option<IndexRecordOption>,
        ),
    )]
    /// Options for indexing text in a concatenate field.
    pub indexing_options: TextIndexingOptions,
}

impl Default for QuickwitConcatenateOptions {
    fn default() -> Self {
        QuickwitConcatenateOptions {
            description: None,
            concatenate_fields: Vec::new(),
            include_dynamic_fields: false,
            indexing_options: TextIndexingOptions {
                tokenizer: QuickwitTextTokenizer::default(),
                record: IndexRecordOption::Basic,
                fieldnorms: false,
            },
        }
    }
}

impl From<QuickwitConcatenateOptions> for JsonObjectOptions {
    fn from(quickwit_text_options: QuickwitConcatenateOptions) -> Self {
        let mut text_options = JsonObjectOptions::default();
        let text_field_indexing = TextFieldIndexing::default()
            .set_index_option(quickwit_text_options.indexing_options.record)
            .set_fieldnorms(quickwit_text_options.indexing_options.fieldnorms)
            .set_tokenizer(quickwit_text_options.indexing_options.tokenizer.name());

        text_options = text_options.set_indexing_options(text_field_indexing);
        text_options
    }
}

fn deserialize_mapping_type(
    quickwit_field_type: QuickwitFieldType,
    json: JsonValue,
) -> anyhow::Result<FieldMappingType> {
    let (typ, cardinality) = match quickwit_field_type {
        QuickwitFieldType::Simple(typ) => (typ, Cardinality::SingleValued),
        QuickwitFieldType::Array(typ) => (typ, Cardinality::MultiValued),
        QuickwitFieldType::Object => {
            let object_options: QuickwitObjectOptions = serde_json::from_value(json)?;
            if object_options.field_mappings.is_empty() {
                anyhow::bail!("object type must have at least one field mapping");
            }
            return Ok(FieldMappingType::Object(object_options));
        }
        QuickwitFieldType::Concatenate => {
            let concatenate_options: QuickwitConcatenateOptions = serde_json::from_value(json)?;
            if concatenate_options.concatenate_fields.is_empty()
                && !concatenate_options.include_dynamic_fields
            {
                anyhow::bail!("concatenate type must have at least one sub-field");
            }
            return Ok(FieldMappingType::Concatenate(concatenate_options));
        }
    };
    match typ {
        Type::Str => {
            let text_options: QuickwitTextOptions = serde_json::from_value(json)?;
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
            let bool_options: QuickwitBoolOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::Bool(bool_options, cardinality))
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
            if numeric_options.fast && cardinality == Cardinality::MultiValued {
                bail!("fast field is not allowed for array<bytes>");
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
                    "field `{}` has an unknown type: `{}`",
                    &value.name, &value.type_id
                )
            })?;
        let mapping_type = deserialize_mapping_type(
            quickwit_field_type,
            JsonValue::Object(value.field_mapping_json),
        )
        .map_err(|err| format!("error while parsing field `{}`: {}", value.name, err))?;
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
        | FieldMappingType::F64(options, _) => serialize_to_map(&options),
        FieldMappingType::Bool(options, _) => serialize_to_map(&options),
        FieldMappingType::Bytes(options, _) => serialize_to_map(&options),
        FieldMappingType::IpAddr(options, _) => serialize_to_map(&options),
        FieldMappingType::DateTime(date_time_options, _) => serialize_to_map(&date_time_options),
        FieldMappingType::Json(json_options, _) => serialize_to_map(&json_options),
        FieldMappingType::Object(object_options) => serialize_to_map(&object_options),
        FieldMappingType::Concatenate(concatenate_options) => {
            serialize_to_map(&concatenate_options)
        }
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

/// Regular expression validating a field mapping name.
pub const FIELD_MAPPING_NAME_PATTERN: &str = r"^[@$_\-a-zA-Z][@$_/\.\-a-zA-Z0-9]{0,254}$";

/// Validates a field mapping name.
/// Returns `Ok(())` if the name can be used for a field mapping.
///
/// A field mapping name:
/// - can only contain uppercase and lowercase ASCII letters `[a-zA-Z]`, digits `[0-9]`, `.`,
///   hyphens `-`, underscores `_`, at `@` and dollar `$` signs;
/// - must not start with a dot or a digit;
/// - must be different from Quickwit's reserved field mapping names `_source`, `_dynamic`,
///   `_field_presence`;
/// - must not be longer than 255 characters.
pub fn validate_field_mapping_name(field_mapping_name: &str) -> anyhow::Result<()> {
    static FIELD_MAPPING_NAME_PTN: Lazy<Regex> =
        Lazy::new(|| Regex::new(FIELD_MAPPING_NAME_PATTERN).unwrap());

    if QW_RESERVED_FIELD_NAMES.contains(&field_mapping_name) {
        bail!(
            "field name `{field_mapping_name}` is reserved. the following fields are reserved for \
             Quickwit internal usage: {}",
            QW_RESERVED_FIELD_NAMES.join(", "),
        );
    }
    if FIELD_MAPPING_NAME_PTN.is_match(field_mapping_name) {
        return Ok(());
    }
    if field_mapping_name.is_empty() {
        bail!("field name is empty");
    }
    if field_mapping_name.starts_with('.') {
        bail!(
            "field name `{}` must not start with a dot `.`",
            field_mapping_name
        );
    }
    if field_mapping_name.len() > 255 {
        bail!(
            "field name `{}` is too long. field names must not be longer than 255 characters",
            field_mapping_name
        )
    }
    let first_char = field_mapping_name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() {
        bail!(
            "field name `{}` is invalid. field names must start with an uppercase or lowercase \
             ASCII letter, or an underscore `_`",
            field_mapping_name
        )
    }
    bail!(
        "field name `{}` contains illegal characters. field names must only contain uppercase and \
         lowercase ASCII letters, digits, hyphens `-`, periods `.`, and underscores `_`",
        field_mapping_name
    );
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use matches::matches;
    use serde_json::json;
    use tantivy::schema::{IndexRecordOption, JsonObjectOptions, TextOptions};

    use super::*;
    use crate::Cardinality;
    use crate::doc_mapper::{FastFieldOptions, FieldMappingType};

    #[test]
    fn test_validate_field_mapping_name() {
        assert!(
            validate_field_mapping_name("")
                .unwrap_err()
                .to_string()
                .contains("is empty")
        );
        assert!(
            validate_field_mapping_name(&"a".repeat(256))
                .unwrap_err()
                .to_string()
                .contains("is too long")
        );
        assert!(
            validate_field_mapping_name("0")
                .unwrap_err()
                .to_string()
                .contains("must start with")
        );
        assert!(
            validate_field_mapping_name(".my-field")
                .unwrap_err()
                .to_string()
                .contains("must not start with")
        );
        assert!(
            validate_field_mapping_name("_source")
                .unwrap_err()
                .to_string()
                .contains("are reserved for Quickwit")
        );
        assert!(
            validate_field_mapping_name("_dynamic")
                .unwrap_err()
                .to_string()
                .contains("are reserved for Quickwit")
        );
        assert!(
            validate_field_mapping_name("my-field!")
                .unwrap_err()
                .to_string()
                .contains("illegal characters")
        );
        assert!(validate_field_mapping_name("_my_field").is_ok());
        assert!(validate_field_mapping_name("-my-field").is_ok());
        assert!(validate_field_mapping_name("my-field").is_ok());
        assert!(validate_field_mapping_name("my.field").is_ok());
        assert!(validate_field_mapping_name("my_field").is_ok());
        assert!(validate_field_mapping_name("$my_field@").is_ok());
        assert!(validate_field_mapping_name("my/field").is_ok());
        assert!(validate_field_mapping_name(&"a".repeat(255)).is_ok());
    }

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
                assert_eq!(text_field_indexing.tokenizer(), "raw");
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
                assert!(options.indexing_options.is_none());
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
            "error while parsing field `data_binary`: `record`, `tokenizer`, and `fieldnorms` \
             parameters are allowed only if indexed is true"
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
                assert!(options.indexing_options.is_none());
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
            "error while parsing field `data_binary`: `record` and `tokenizer` parameters are \
             allowed only if indexed is true"
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
                "record": "notexist"
            }
            "#,
        );
        assert!(mapping_entry.is_err());
        assert_eq!(
            mapping_entry.unwrap_err().to_string(),
            "error while parsing field `my_field_name`: unknown variant `notexist`, expected one \
             of `basic`, `freq`, `position`"
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
        assert!(
            mapping_entry
                .unwrap_err()
                .to_string()
                .contains("error while parsing field `my_field_name`: unknown field `blub`")
        );
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
                let indexing_options = options.indexing_options.unwrap();
                assert_eq!(indexing_options.tokenizer.name(), "en_stem");
                assert_eq!(indexing_options.record, IndexRecordOption::Basic);
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
                let indexing_options = options.indexing_options.unwrap();
                assert_eq!(indexing_options.fieldnorms, true);
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
            "error while parsing field `my_field_name`: `record`, `tokenizer`, and `fieldnorms` \
             parameters are allowed only if indexed is true"
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
            "error while parsing field `my_field_name`: object type must have at least one field \
             mapping"
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
            "field `my_field_name` has an unknown type: `my custom type`"
        );
    }

    #[test]
    fn test_deserialize_i64_mapping_with_invalid_name() {
        assert!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "this is not ok",
                "type": "i64"
            }
            "#,
            )
            .unwrap_err()
            .to_string()
            .contains("illegal characters")
        );
    }

    #[test]
    fn test_deserialize_i64_parsing_error_with_text_options() {
        let error = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "i64",
                "tokenizer": "basic"
            }
            "#,
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "error while parsing field `my_field_name`: unknown field `tokenizer`, expected one \
             of `description`, `stored`, `indexed`, `fast`, `coerce`, `output_format`"
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
                assert_eq!(cardinality, Cardinality::MultiValued);
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
                assert_eq!(cardinality, Cardinality::SingleValued);
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
                "coerce": true,
                "output_format": "number"
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
            "error while parsing field `my_field_name`: unknown field `tokenizer`, expected one \
             of `description`, `stored`, `indexed`, `fast`, `coerce`, `output_format`"
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
            assert_eq!(cardinality, Cardinality::MultiValued);
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
            assert_eq!(cardinality, Cardinality::SingleValued);
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
                "coerce": true,
                "output_format": "number"
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
                "coerce": true,
                "output_format": "number"
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
                "record": "basic",
                "tokenizer": "default",
                "fieldnorms": false,
            })
        );
    }

    #[test]
    fn test_parse_text_fast_field_normalizer() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "my_field_name",
                "type": "text",
                "fast": {"normalizer": "lowercase"}
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
                "fast": {"normalizer": "lowercase"},
                "stored": true,
                "indexed": true,
                "record": "basic",
                "tokenizer": "default",
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
                "record": "basic",
                "tokenizer": "default",
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
                "fast_precision": "seconds",
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
                "fast_precision": "milliseconds"
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
                "fast_precision": "milliseconds",
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
                "type": "bytes",
                "input_format": "hex",
                "output_format": "base64"
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
                "input_format": "hex",
                "output_format": "base64"
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
                "input_format": "base64",
                "output_format": "base64"
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
            "error while parsing field `my_field_name`: fast field is not allowed for array<bytes>",
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
            indexing_options: Some(TextIndexingOptions::default_json()),
            stored: true,
            fast: FastFieldOptions::Disabled,
            expand_dots: true,
        };
        assert_eq!(&field_mapping_entry.name, "my_json_field");
        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::Json(json_config,
            Cardinality::SingleValued) if json_config == expected_json_options)
        );
    }

    #[test]
    fn test_quickwit_json_options_default_tokenizer_is_raw() {
        let quickwit_json_options = QuickwitJsonOptions::default();
        assert_eq!(
            quickwit_json_options
                .indexing_options
                .unwrap()
                .tokenizer
                .name(),
            "raw"
        );
    }

    #[test]
    fn test_quickwit_json_options_default_fast_is_false() {
        let quickwit_json_options = QuickwitJsonOptions::default();
        assert_eq!(quickwit_json_options.fast, FastFieldOptions::Disabled);
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
            indexing_options: Some(TextIndexingOptions::default_json()),
            stored: false,
            expand_dots: true,
            fast: FastFieldOptions::Disabled,
        };
        assert_eq!(&field_mapping_entry.name, "my_json_field_multi");
        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::Json(json_config,
    Cardinality::MultiValued) if json_config == expected_json_options)
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
                "coerce": true,
                "output_format": "number"
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
                "record": "basic",
                "tokenizer": "default",
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
                "tokenizer": "raw",
                "record": "basic",
                "fast": false,
                "expand_dots": true,
            })
        );
    }
}
