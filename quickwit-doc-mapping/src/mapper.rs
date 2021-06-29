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

use crate::DocParsingError;
use dyn_clone::clone_trait_object;
use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use quickwit_proto::SearchRequest;
use regex::Regex;
use std::fmt::Debug;
use tantivy::query::Query;
use tantivy::schema::{Field, Schema};
use tantivy::Document;

use crate::QueryParserError;

/// Sorted order (either Ascending or Descending).
/// To get a regular top-K results search, use `SortOrder::Desc`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortOrder {
    /// Descending. This is the default to get Top-K results.
    Desc,
    /// Ascending order.
    Asc,
}

/// Defines the way documents should be sorted.
/// In case of a tie, the documents are ordered according to descending `(split_id, segment_ord, doc_id)`.
#[derive(Clone, Debug)]
pub enum SortBy {
    /// Sort by a specific field.
    SortByFastField {
        /// Field to sort by.
        field_name: String,
        /// Order to sort by. A usual top-K search implies a Descending order.
        order: SortOrder,
    },
    /// Sort by DocId
    DocId,
}

/// The `DocMapper` trait defines the way of defining how a (json) document,
/// and the fields it contains, are stored and indexed.
///
/// The `DocMapper` trait is in charge of implementing :
///
/// - a way to build a tantivy::Document from a json payload
/// - a way to build a tantivy::Query from a SearchRequest
/// - a way to build a tantivy:Schema
///
#[typetag::serde(tag = "type")]
pub trait DocMapper: Send + Sync + Debug + DynClone + 'static {
    /// Returns the document built from a json string.
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError>;
    /// Returns the schema.
    fn schema(&self) -> Schema;
    /// Returns the query.
    fn query(&self, request: &SearchRequest) -> Result<Box<dyn Query>, QueryParserError>;
    /// Returns the default sort
    fn default_sort_by(&self) -> SortBy {
        SortBy::DocId
    }
    /// Returns the timestamp field.
    fn timestamp_field(&self) -> Option<Field> {
        self.timestamp_field_name()
            .and_then(|field_name| self.schema().get_field(&field_name))
    }
    /// Returns the timestamp field name.
    fn timestamp_field_name(&self) -> Option<String> {
        None
    }
}

clone_trait_object!(DocMapper);

/// Regular expression representing the restriction on a valid field name.
pub const FIELD_MAPPING_NAME_PATTERN: &str = r#"^[a-zA-Z_][a-zA-Z0-9_\.\-]*$"#;

/// Validator for a potential `field_mapping_name`.
/// Returns true if the name can be use for a field mapping name.
///
/// A field mapping name must start by a letter `[a-zA-Z]`.
/// The other characters can be any alphanumic character `[a-ZA-Z0-9]` or `_` or `.`.
/// Finally keyword `__dot__` is forbidden.
pub fn is_valid_field_mapping_name(field_mapping_name: &str) -> bool {
    static FIELD_MAPPING_NAME_PTN: Lazy<Regex> =
        Lazy::new(|| Regex::new(FIELD_MAPPING_NAME_PATTERN).unwrap());
    FIELD_MAPPING_NAME_PTN.is_match(field_mapping_name) && !field_mapping_name.contains("__dot__")
}

/// String used to replace a `.` in a field name so that the field is a valid tantivy field name.
pub const TANTIVY_DOT_SYMBOL: &str = "__dot__";

#[cfg(test)]
mod tests {
    use crate::{DefaultDocMapperBuilder, DocMapper};

    const JSON_ALL_FLATTEN_DOC_MAPPER: &str = r#"
        {
            "type": "all_flatten"
        }"#;

    const JSON_DEFAULT_DOC_MAPPER: &str = r#"
        {
            "type": "default",
            "default_search_fields": [],
            "field_mappings": []
        }"#;

    #[test]
    fn test_deserialize_doc_mapper() -> anyhow::Result<()> {
        let all_flatten_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_ALL_FLATTEN_DOC_MAPPER)?;
        assert_eq!(
            format!("{:?}", all_flatten_mapper),
            "AllFlattenDocMapper".to_string()
        );

        let deserialized_default_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_DEFAULT_DOC_MAPPER)?;
        let expected_default_mapper = DefaultDocMapperBuilder::new().build()?;
        assert_eq!(
            format!("{:?}", deserialized_default_mapper),
            format!("{:?}", expected_default_mapper),
        );
        Ok(())
    }
}
