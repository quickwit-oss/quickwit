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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::ops::Bound;

use anyhow::Context;
use dyn_clone::{clone_trait_object, DynClone};
use quickwit_query::query_ast::QueryAst;
use serde_json::Value as JsonValue;
use tantivy::query::Query;
use tantivy::schema::{Field, FieldType, Schema, Value};
use tantivy::tokenizer::TokenizerManager;
use tantivy::{Document, Term};

pub type Partition = u64;

/// An alias for serde_json's object type.
pub type JsonObject = serde_json::Map<String, JsonValue>;

use crate::{DocParsingError, QueryParserError};

/// The `DocMapper` trait defines the way of defining how a (json) document,
/// and the fields it contains, are stored and indexed.
///
/// The `DocMapper` trait is in charge of :
/// - building a tantivy [`Document`] from a JSON payload
/// - building a tantivy [`Query`] from a [`QueryAst`]
/// - supplying a tantivy [`Schema`]
#[typetag::serde(tag = "type")]
pub trait DocMapper: Send + Sync + Debug + DynClone + 'static {
    /// Transforms a JSON object into a tantivy [`Document`] according to the rules
    /// defined for the `DocMapper`.
    fn doc_from_json_obj(
        &self,
        json_obj: JsonObject,
    ) -> Result<(Partition, Document), DocParsingError>;

    /// Parses a JSON byte slice into a tantivy [`Document`].
    fn doc_from_json_bytes(
        &self,
        json_doc: &[u8],
    ) -> Result<(Partition, Document), DocParsingError> {
        let json_obj: JsonObject = serde_json::from_slice(json_doc).map_err(|_| {
            let json_doc_sample: String = std::str::from_utf8(json_doc)
                .map(|doc_str| doc_str.chars().take(20).chain("...".chars()).collect())
                .unwrap_or_else(|_| "document contains some invalid UTF-8 characters".to_string());
            DocParsingError::NotJsonObject(json_doc_sample)
        })?;
        self.doc_from_json_obj(json_obj)
    }

    /// Parses a JSON string into a tantivy [`Document`].
    fn doc_from_json_str(&self, json_doc: &str) -> Result<(Partition, Document), DocParsingError> {
        let json_obj: JsonObject = serde_json::from_str(json_doc).map_err(|_| {
            let json_doc_sample: String = json_doc.chars().take(20).chain("...".chars()).collect();
            DocParsingError::NotJsonObject(json_doc_sample)
        })?;
        self.doc_from_json_obj(json_obj)
    }

    /// Converts a tantivy named Document to the json format.
    ///
    /// Tantivy does not have any notion of cardinality nor object.
    /// It is therefore up to the `DocMapper` to pick a tantivy named document
    /// and convert it into a final quickwit document.
    ///
    /// Because this operation is dependent on the `DocMapper`, this
    /// method is meant to be called on the root node using the most recent
    /// `DocMapper`. This ensures that the different hits are formatted according
    /// to the same schema.
    fn doc_to_json(
        &self,
        named_doc: BTreeMap<String, Vec<Value>>,
    ) -> anyhow::Result<serde_json::Map<String, JsonValue>>;

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
        query_ast: &QueryAst,
        with_validation: bool,
    ) -> Result<(Box<dyn Query>, WarmupInfo), QueryParserError>;

    /// Returns the timestamp field name.
    fn timestamp_field_name(&self) -> Option<&str> {
        None
    }

    /// Returns the list of search fields to search into, when no field is specified.
    /// (See `UserInputQuery`).
    fn default_search_fields(&self) -> &[String];

    /// Returns the tag field names
    fn tag_field_names(&self) -> BTreeSet<String> {
        Default::default()
    }

    /// Returns the tag `NameField`s on the current schema.
    /// Returns an error if a tag field is not found in this schema.
    fn tag_named_fields(&self) -> anyhow::Result<Vec<NamedField>> {
        let index_schema = self.schema();
        self.tag_field_names()
            .iter()
            .map(|field_name| {
                index_schema
                    .get_field(field_name)
                    .context(format!("field `{field_name}` must exist in the schema"))
                    .map(|field| NamedField {
                        name: field_name.clone(),
                        field,
                        field_type: index_schema.get_field_entry(field).field_type().clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Returns the maximum number of partitions.
    fn max_num_partitions(&self) -> NonZeroU32;

    /// Returns the tokenizer manager.
    fn tokenizer_manager(&self) -> &TokenizerManager;
}

/// A struct to wrap a tantivy field with its name.
#[derive(Clone, Debug)]
pub struct NamedField {
    /// Name of the field.
    pub name: String,
    /// Tantivy schema field.
    pub field: Field,
    /// Tantivy schema field type.
    pub field_type: FieldType,
}

clone_trait_object!(DocMapper);

/// Bounds for a range of terms, with an optional max count of terms being matched.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TermRange {
    /// Start of the range
    pub start: Bound<Term>,
    /// End of the range
    pub end: Bound<Term>,
    /// Max number of matched terms
    pub limit: Option<u64>,
}

/// Information about what a DocMapper think should be warmed up before
/// running the query.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WarmupInfo {
    /// Name of fields from the term dictionary which needs to be entirely
    /// loaded
    pub term_dict_field_names: HashSet<String>,
    /// Name of fields from the posting lists which needs to be entirely
    /// loaded
    pub posting_field_names: HashSet<String>,
    /// Name of fast fields which needs to be loaded
    pub fast_field_names: HashSet<String>,
    /// Whether to warmup field norms. Used mostly for scoring.
    pub field_norms: bool,
    /// Terms to warmup, and whether their position is needed too.
    pub terms_grouped_by_field: HashMap<Field, HashMap<Term, bool>>,
    /// Term ranges to warmup, and whether their position is needed too.
    pub term_ranges_grouped_by_field: HashMap<Field, HashMap<TermRange, bool>>,
}

impl WarmupInfo {
    /// Merge other WarmupInfo into self.
    pub fn merge(&mut self, other: WarmupInfo) {
        self.term_dict_field_names
            .extend(other.term_dict_field_names);
        self.posting_field_names.extend(other.posting_field_names);
        self.fast_field_names.extend(other.fast_field_names);
        self.field_norms |= other.field_norms;

        for (field, term_and_pos) in other.terms_grouped_by_field.into_iter() {
            let sub_map = self.terms_grouped_by_field.entry(field).or_default();

            for (term, include_position) in term_and_pos.into_iter() {
                *sub_map.entry(term).or_default() |= include_position;
            }
        }

        // this merge is suboptimal in case of overlapping range with no limit.
        for (field, term_range_and_pos) in other.term_ranges_grouped_by_field.into_iter() {
            let sub_map = self.term_ranges_grouped_by_field.entry(field).or_default();

            for (term_range, include_position) in term_range_and_pos.into_iter() {
                *sub_map.entry(term_range).or_default() |= include_position;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::ops::Bound;

    use quickwit_query::query_ast::{query_ast_from_user_text, UserInputQuery};
    use quickwit_query::BooleanOperand;
    use tantivy::schema::{Field, FieldType, Term};

    use crate::default_doc_mapper::{FieldMappingType, QuickwitJsonOptions};
    use crate::{
        Cardinality, DefaultDocMapper, DefaultDocMapperBuilder, DocMapper, DocParsingError,
        FieldMappingEntry, Mode, TermRange, WarmupInfo, DYNAMIC_FIELD_NAME,
    };

    const JSON_DEFAULT_DOC_MAPPER: &str = r#"
        {
            "type": "default",
            "default_search_fields": [],
            "tag_fields": [],
            "field_mappings": []
        }"#;

    #[test]
    fn test_doc_from_json_bytes() {
        let doc_mapper = DefaultDocMapperBuilder::default().try_build().unwrap();
        let json_doc = br#"{"title": "hello", "body": "world"}"#;
        doc_mapper.doc_from_json_bytes(json_doc).unwrap();

        let DocParsingError::NotJsonObject(json_doc_sample) = doc_mapper
            .doc_from_json_bytes(br#"Not a JSON object"#)
            .unwrap_err()
        else {
            panic!("Expected `DocParsingError::NotJsonObject` error");
        };
        assert_eq!(json_doc_sample, "Not a JSON object...");
    }

    #[test]
    fn test_doc_from_json_str() {
        let doc_mapper = DefaultDocMapperBuilder::default().try_build().unwrap();
        let json_doc = r#"{"title": "hello", "body": "world"}"#;
        doc_mapper.doc_from_json_str(json_doc).unwrap();

        let DocParsingError::NotJsonObject(json_doc_sample) = doc_mapper
            .doc_from_json_str(r#"Not a JSON object"#)
            .unwrap_err()
        else {
            panic!("Expected `DocParsingError::NotJsonObject` error");
        };
        assert_eq!(json_doc_sample, "Not a JSON object...");
    }

    #[test]
    fn test_deserialize_doc_mapper() -> anyhow::Result<()> {
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_DEFAULT_DOC_MAPPER)?;
        let expected_default_doc_mapper = DefaultDocMapperBuilder::default().try_build()?;
        assert_eq!(
            format!("{deserialized_default_doc_mapper:?}"),
            format!("{expected_default_doc_mapper:?}"),
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_minimal_doc_mapper() -> anyhow::Result<()> {
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(r#"{"type": "default"}"#)?;
        let expected_default_doc_mapper = DefaultDocMapperBuilder::default().try_build()?;
        assert_eq!(
            format!("{deserialized_default_doc_mapper:?}"),
            format!("{expected_default_doc_mapper:?}"),
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_doc_mapper_default_dynamic_tokenizer() {
        let doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(r#"{"type": "default", "mode": "dynamic"}"#)
                .unwrap();
        let tantivy_schema = doc_mapper.schema();
        let dynamic_field = tantivy_schema.get_field(DYNAMIC_FIELD_NAME).unwrap();
        if let FieldType::JsonObject(json_options) =
            tantivy_schema.get_field_entry(dynamic_field).field_type()
        {
            let text_opt = json_options.get_text_indexing_options().unwrap();
            assert_eq!(text_opt.tokenizer(), "raw");
        } else {
            panic!("dynamic field should be of JSON type");
        }
    }

    #[test]
    fn test_serdeserialize_doc_mapper() -> anyhow::Result<()> {
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_DEFAULT_DOC_MAPPER)?;
        let expected_default_doc_mapper = DefaultDocMapperBuilder::default().try_build()?;
        assert_eq!(
            format!("{deserialized_default_doc_mapper:?}"),
            format!("{expected_default_doc_mapper:?}"),
        );

        let serialized_doc_mapper = serde_json::to_string(&deserialized_default_doc_mapper)?;
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(&serialized_doc_mapper)?;
        let serialized_doc_mapper_2 = serde_json::to_string(&deserialized_default_doc_mapper)?;

        assert_eq!(serialized_doc_mapper, serialized_doc_mapper_2);

        Ok(())
    }

    #[test]
    fn test_doc_mapper_query_with_json_field() {
        let mut doc_mapper_builder = DefaultDocMapperBuilder::default();
        doc_mapper_builder.field_mappings.push(FieldMappingEntry {
            name: "json_field".to_string(),
            mapping_type: FieldMappingType::Json(
                QuickwitJsonOptions::default(),
                Cardinality::SingleValue,
            ),
        });
        let doc_mapper = doc_mapper_builder.try_build().unwrap();
        let schema = doc_mapper.schema();
        let query_ast = UserInputQuery {
            user_text: "json_field.toto.titi:hello".to_string(),
            default_fields: None,
            default_operator: BooleanOperand::And,
        }
        .parse_user_query(&[])
        .unwrap();
        let (query, _) = doc_mapper.query(schema, &query_ast, true).unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"TermQuery(Term(field=2, type=Json, path=toto.titi, type=Str, "hello"))"#
        );
    }

    #[test]
    fn test_doc_mapper_query_with_json_field_default_search_fields() {
        let doc_mapper: DefaultDocMapper = DefaultDocMapperBuilder {
            mode: Mode::default(),
            ..Default::default()
        }
        .try_build()
        .unwrap();
        let schema = doc_mapper.schema();
        let query_ast = query_ast_from_user_text("toto.titi:hello", None)
            .parse_user_query(doc_mapper.default_search_fields())
            .unwrap();
        let (query, _) = doc_mapper.query(schema, &query_ast, true).unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"TermQuery(Term(field=1, type=Json, path=toto.titi, type=Str, "hello"))"#
        );
    }

    #[test]
    fn test_doc_mapper_query_with_json_field_ambiguous_term() {
        let doc_mapper: DefaultDocMapper = DefaultDocMapperBuilder {
            mode: Mode::default(),
            ..Default::default()
        }
        .try_build()
        .unwrap();
        let schema = doc_mapper.schema();
        let query_ast = query_ast_from_user_text("toto:5", None)
            .parse_user_query(&[])
            .unwrap();
        let (query, _) = doc_mapper.query(schema, &query_ast, true).unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"BooleanQuery { subqueries: [(Should, TermQuery(Term(field=1, type=Json, path=toto, type=I64, 5))), (Should, TermQuery(Term(field=1, type=Json, path=toto, type=Str, "5")))] }"#
        );
    }

    fn hashset(elements: &[&str]) -> HashSet<String> {
        elements.iter().map(|elem| elem.to_string()).collect()
    }

    fn hashmap(elements: &[(u32, &str, bool)]) -> HashMap<Field, HashMap<Term, bool>> {
        let mut result: HashMap<Field, HashMap<Term, bool>> = HashMap::new();
        for (field, term, pos) in elements {
            let field = Field::from_field_id(*field);
            *result
                .entry(field)
                .or_default()
                .entry(Term::from_field_text(field, term))
                .or_default() |= pos;
        }

        result
    }

    fn hashmap_ranges(elements: &[(u32, &str, bool)]) -> HashMap<Field, HashMap<TermRange, bool>> {
        let mut result: HashMap<Field, HashMap<TermRange, bool>> = HashMap::new();
        for (field, term, pos) in elements {
            let field = Field::from_field_id(*field);
            let term = Term::from_field_text(field, term);
            // this is a 1 element bound, but it's enought for testing.
            let range = TermRange {
                start: Bound::Included(term.clone()),
                end: Bound::Included(term),
                limit: None,
            };
            *result.entry(field).or_default().entry(range).or_default() |= pos;
        }

        result
    }

    #[test]
    fn test_warmup_info_merge() {
        let wi_base = WarmupInfo {
            term_dict_field_names: hashset(&["termdict1", "termdict2"]),
            posting_field_names: hashset(&["posting1", "posting2"]),
            fast_field_names: hashset(&["fast1", "fast2"]),
            field_norms: false,
            terms_grouped_by_field: hashmap(&[(1, "term1", false), (1, "term2", false)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (2, "term1", false),
                (2, "term2", false),
            ]),
        };

        // merging with default has no impact
        let mut wi_cloned = wi_base.clone();
        wi_cloned.merge(WarmupInfo::default());
        assert_eq!(wi_cloned, wi_base);

        let mut wi_base = wi_base;
        let wi_2 = WarmupInfo {
            term_dict_field_names: hashset(&["termdict2", "termdict3"]),
            posting_field_names: hashset(&["posting2", "posting3"]),
            fast_field_names: hashset(&["fast2", "fast3"]),
            field_norms: true,
            terms_grouped_by_field: hashmap(&[(2, "term1", false), (1, "term2", true)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (3, "term1", false),
                (2, "term2", true),
            ]),
        };
        wi_base.merge(wi_2.clone());

        assert_eq!(
            wi_base.term_dict_field_names,
            hashset(&["termdict1", "termdict2", "termdict3"])
        );
        assert_eq!(
            wi_base.posting_field_names,
            hashset(&["posting1", "posting2", "posting3"])
        );
        assert_eq!(
            wi_base.fast_field_names,
            hashset(&["fast1", "fast2", "fast3"])
        );
        assert!(wi_base.field_norms);

        let expected_terms = [(1, "term1", false), (1, "term2", true), (2, "term1", false)];
        for (field, term, pos) in expected_terms {
            let field = Field::from_field_id(field);
            let term = Term::from_field_text(field, term);

            assert_eq!(
                *wi_base
                    .terms_grouped_by_field
                    .get(&field)
                    .unwrap()
                    .get(&term)
                    .unwrap(),
                pos
            );
        }

        let expected_ranges = [(2, "term1", false), (2, "term2", true), (3, "term1", false)];
        for (field, term, pos) in expected_ranges {
            let field = Field::from_field_id(field);
            let term = Term::from_field_text(field, term);
            let range = TermRange {
                start: Bound::Included(term.clone()),
                end: Bound::Included(term),
                limit: None,
            };

            assert_eq!(
                *wi_base
                    .term_ranges_grouped_by_field
                    .get(&field)
                    .unwrap()
                    .get(&range)
                    .unwrap(),
                pos
            );
        }

        // merge is idempotent
        let mut wi_cloned = wi_base.clone();
        wi_cloned.merge(wi_2);
        assert_eq!(wi_cloned, wi_base);
    }

    #[test]
    #[cfg(feature = "testsuite")]
    fn test_doc_mapper_query_with_multilang_field() {
        use quickwit_query::query_ast::TermQuery;
        use tantivy::schema::IndexRecordOption;

        use crate::default_doc_mapper::{
            QuickwitTextOptions, QuickwitTextTokenizer, TextIndexingOptions, TokenizerType,
        };
        use crate::{TokenizerConfig, TokenizerEntry};
        let mut doc_mapper_builder = DefaultDocMapperBuilder::default();
        doc_mapper_builder.field_mappings.push(FieldMappingEntry {
            name: "multilang".to_string(),
            mapping_type: FieldMappingType::Text(
                QuickwitTextOptions {
                    indexing_options: Some(TextIndexingOptions {
                        tokenizer: QuickwitTextTokenizer::from_static("multilang"),
                        record: IndexRecordOption::Basic,
                        fieldnorms: false,
                    }),
                    ..Default::default()
                },
                Cardinality::SingleValue,
            ),
        });
        doc_mapper_builder.tokenizers.push(TokenizerEntry {
            name: "multilang".to_string(),
            config: TokenizerConfig {
                tokenizer_type: TokenizerType::Multilang,
                filters: vec![],
            },
        });
        let doc_mapper = doc_mapper_builder.try_build().unwrap();
        let schema = doc_mapper.schema();
        let query_ast = quickwit_query::query_ast::QueryAst::Term(TermQuery {
            field: "multilang".to_string(),
            value: "JPN:す".to_string(),
        });
        let (query, _) = doc_mapper.query(schema, &query_ast, false).unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"TermQuery(Term(field=2, type=Str, "JPN:す"))"#
        );
    }
}
