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

use std::fmt::Debug;

use dyn_clone::{clone_trait_object, DynClone};
use quickwit_proto::SearchRequest;
use tantivy::query::Query;
use tantivy::schema::{Field, Schema, Value};
use tantivy::{Document, Order};

use crate::{DocParsingError, QueryParserError, TAGS_FIELD_NAME};

/// Separator used to format tags into `{field_name}:{value}`
pub const TAG_FIELD_VALUE_SEPARATOR: &str = ":";

/// Wilcard value use to collapse too many tag values into one.
pub const MANY_TAG_VALUES: &str = "*";

/// Character use to escape tag value when there is collision with the wilcard
/// tag values.
pub const TAGS_VALUE_ESCAPE: &str = "\\";

/// Sorted order (either Ascending or Descending).
/// To get a regular top-K results search, use `SortOrder::Desc`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    /// Descending. This is the default to get Top-K results.
    Desc,
    /// Ascending order.
    Asc,
}

impl Default for SortOrder {
    fn default() -> Self {
        Self::Desc
    }
}

use crate::{DocParsingError, QueryParserError, SortBy, TAGS_FIELD_NAME};

/// Converts a field (name, value) into a tag string `name:value`.
pub fn convert_tag_to_string(field_name: &str, field_value: &Value) -> String {
    let field_value_str = tantivy_value_to_string(field_value);
    if field_value_str == MANY_TAG_VALUES {
        return format!(
            "{}{}{}{}",
            field_name, TAG_FIELD_VALUE_SEPARATOR, TAGS_VALUE_ESCAPE, field_value_str
        );
    }
    format!(
        "{}{}{}",
        field_name, TAG_FIELD_VALUE_SEPARATOR, field_value_str
    )
}

/// Returns true if tag_string is of form `{field_name}:any_value`.
pub fn match_tag_field_name(field_name: &str, tag_string: &str) -> bool {
    tag_string.starts_with(&format!("{}{}", field_name, TAG_FIELD_VALUE_SEPARATOR))
}

/// Extracts the field name from a tag value of the form `{field_name}:any_value`.
pub fn extract_field_name_from_tag_value(tag_value: &str) -> Option<String> {
    tag_value
        .split_once(TAG_FIELD_VALUE_SEPARATOR)
        .map(|(field_name, _)| field_name.to_string())
}

/// Escapes the tag value by prefixing the value part of this format `{field_name}:any_value`
/// by `\` if `any_value` is the wildcard value `*`.
pub fn escape_tag_value(tag_value: &str) -> String {
    match tag_value.split_once(TAG_FIELD_VALUE_SEPARATOR) {
        Some((field_name, value)) if value == MANY_TAG_VALUES => format!(
            "{}{}{}{}",
            field_name, TAG_FIELD_VALUE_SEPARATOR, TAGS_VALUE_ESCAPE, value
        ),
        _ => tag_value.to_string(),
    }
}

/// Creates the wildcard tag value for a field name: `{field_name}:*`.
pub fn make_too_many_tag_value(field_name: &str) -> String {
    format!(
        "{}{}{}",
        field_name, TAG_FIELD_VALUE_SEPARATOR, MANY_TAG_VALUES
    )
}

/// Converts a [`tantivy::Value`] to it's [`String`] value.
fn tantivy_value_to_string(field_value: &Value) -> String {
    match field_value {
        Value::Str(text) => text.clone(),
        Value::PreTokStr(data) => data.text.clone(),
        Value::U64(num) => num.to_string(),
        Value::I64(num) => num.to_string(),
        Value::F64(num) => num.to_string(),
        Value::Date(date) => date.to_rfc3339(),
        Value::Facet(facet) => facet.to_string(),
        Value::Bytes(data) => base64::encode(data),
    }
}

/// The `IndexConfig` trait defines the way of defining how a (json) document,
/// and the fields it contains, are stored and indexed.
///
/// The `IndexConfig` trait is in charge of implementing :
///
/// - a way to build a tantivy::Document from a json payload
/// - a way to build a tantivy::Query from a SearchRequest
/// - a way to build a tantivy:Schema
#[typetag::serde(tag = "type")]
pub trait IndexConfig: Send + Sync + Debug + DynClone + 'static {
    /// Returns the document built from an owned JSON string.
    fn doc_from_json(&self, doc_json: String) -> Result<Document, DocParsingError>;

    /// Returns the schema.
    ///
    /// Considering schema evolution, splits within an index can have different schema
    /// over time. The schema returned here represents the most up-to-date schema of the index.
    fn schema(&self) -> Schema;

    /// Returns the query.
    ///
    /// Considering schema evolution, splits within an index can have different schema
    /// over time. So `split_schema` is the schema of the split the query is targeting.
    fn query(
        &self,
        split_schema: Schema,
        request: &SearchRequest,
    ) -> Result<Box<dyn Query>, QueryParserError>;

    /// Returns the default sort
    fn sort_by(&self) -> SortBy {
        SortBy::DocId
    }

    /// Returns the timestamp field.
    /// Considering schema evolution, splits within an index can have different schema
    /// over time. So `split_schema` is the schema of the split being operated on.
    fn timestamp_field(&self, split_schema: &Schema) -> Option<Field> {
        self.timestamp_field_name()
            .and_then(|field_name| split_schema.get_field(&field_name))
    }

    /// Returns the timestamp field name.
    fn timestamp_field_name(&self) -> Option<String> {
        None
    }

    /// Returns the tag field names
    fn tag_field_names(&self) -> Vec<String> {
        vec![]
    }

    /// Returns the special tags field if any.
    fn tags_field(&self, split_schema: &Schema) -> Field {
        split_schema
            .get_field(TAGS_FIELD_NAME)
            .expect("Tags field must exist in the schema.")
    }

    /// Returns the demux field name.
    fn demux_field_name(&self) -> Option<String> {
        None
    }
}

clone_trait_object!(IndexConfig);

#[cfg(test)]
mod tests {
    use crate::{DefaultIndexConfigBuilder, IndexConfig};

    const JSON_DEFAULT_INDEX_CONFIG: &str = r#"
        {
            "type": "default",
            "default_search_fields": [],
            "tag_fields": [],
            "field_mappings": []
        }"#;

    #[test]
    fn test_deserialize_index_config() -> anyhow::Result<()> {
        let deserialized_default_config =
            serde_json::from_str::<Box<dyn IndexConfig>>(JSON_DEFAULT_INDEX_CONFIG)?;
        let expected_default_config = DefaultIndexConfigBuilder::new().build()?;
        assert_eq!(
            format!("{:?}", deserialized_default_config),
            format!("{:?}", expected_default_config),
        );
        Ok(())
    }

    #[test]
    fn test_sedeserialize_index_config() -> anyhow::Result<()> {
        let deserialized_default_config =
            serde_json::from_str::<Box<dyn IndexConfig>>(JSON_DEFAULT_INDEX_CONFIG)?;
        let expected_default_config = DefaultIndexConfigBuilder::new().build()?;
        assert_eq!(
            format!("{:?}", deserialized_default_config),
            format!("{:?}", expected_default_config),
        );

        let serialized_config = serde_json::to_string(&deserialized_default_config)?;
        let deserialized_default_config =
            serde_json::from_str::<Box<dyn IndexConfig>>(&serialized_config)?;
        let serialized_config_2 = serde_json::to_string(&deserialized_default_config)?;

        assert_eq!(serialized_config, serialized_config_2);

        Ok(())
    }
}
