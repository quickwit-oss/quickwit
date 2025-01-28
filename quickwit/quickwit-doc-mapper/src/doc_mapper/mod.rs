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

mod date_time_type;
mod deser_num;
mod doc_mapper_builder;
mod doc_mapper_impl;
mod field_mapping_entry;
mod field_mapping_type;
mod field_presence;
mod mapping_tree;
mod tantivy_val_to_json;
mod tokenizer_entry;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Bound;

pub use doc_mapper_builder::DocMapperBuilder;
pub use doc_mapper_impl::DocMapper;
#[cfg(all(test, feature = "multilang"))]
pub(crate) use field_mapping_entry::TextIndexingOptions;
pub use field_mapping_entry::{
    BinaryFormat, FastFieldOptions, FieldMappingEntry, QuickwitBytesOptions, QuickwitJsonOptions,
    QuickwitTextNormalizer,
};
pub(crate) use field_mapping_entry::{
    FieldMappingEntryForSerialization, IndexRecordOptionSchema, QuickwitTextTokenizer,
};
#[cfg(test)]
pub(crate) use field_mapping_entry::{QuickwitNumericOptions, QuickwitTextOptions};
pub use field_mapping_type::FieldMappingType;
use serde_json::Value as JsonValue;
use tantivy::schema::{Field, FieldType};
use tantivy::Term;
pub use tokenizer_entry::{analyze_text, TokenizerConfig, TokenizerEntry};
pub(crate) use tokenizer_entry::{
    NgramTokenizerOption, RegexTokenizerOption, TokenFilterType, TokenizerType,
};

/// Function used with serde to initialize boolean value at true if there is no value in json.
fn default_as_true() -> bool {
    true
}

pub type Partition = u64;

/// An alias for serde_json's object type.
pub type JsonObject = serde_json::Map<String, JsonValue>;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Supported automaton types to warmup
pub enum Automaton {
    /// A regex in it's str representation as tantivy_fst::Regex isn't PartialEq, and the path if
    /// inside a json field
    Regex(Option<Vec<u8>>, String),
    // we could add termset query here, instead of downloading the whole dictionary
}

/// Description of how a fast field should be warmed up
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FastFieldWarmupInfo {
    /// Name of the fast field
    pub name: String,
    /// Whether subfields should also be loaded for warmup
    pub with_subfields: bool,
}

/// Information about what a DocMapper think should be warmed up before
/// running the query.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WarmupInfo {
    /// Name of fields from the term dictionary and posting list which needs to
    /// be entirely loaded
    pub term_dict_fields: HashSet<Field>,
    /// Fast fields which needs to be loaded
    pub fast_fields: HashSet<FastFieldWarmupInfo>,
    /// Whether to warmup field norms. Used mostly for scoring.
    pub field_norms: bool,
    /// Terms to warmup, and whether their position is needed too.
    pub terms_grouped_by_field: HashMap<Field, HashMap<Term, bool>>,
    /// Term ranges to warmup, and whether their position is needed too.
    pub term_ranges_grouped_by_field: HashMap<Field, HashMap<TermRange, bool>>,
    /// Automatons to warmup
    pub automatons_grouped_by_field: HashMap<Field, HashSet<Automaton>>,
}

impl WarmupInfo {
    /// Merge other WarmupInfo into self.
    pub fn merge(&mut self, other: WarmupInfo) {
        self.term_dict_fields.extend(other.term_dict_fields);
        self.field_norms |= other.field_norms;

        for fast_field_warmup_info in other.fast_fields.into_iter() {
            // avoid overwriting with a less demanding warmup
            if !self.fast_fields.contains(&FastFieldWarmupInfo {
                name: fast_field_warmup_info.name.clone(),
                with_subfields: true,
            }) {
                self.fast_fields.insert(fast_field_warmup_info);
            }
        }

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

        for (field, automatons) in other.automatons_grouped_by_field.into_iter() {
            let sub_map = self.automatons_grouped_by_field.entry(field).or_default();
            sub_map.extend(automatons);
        }
    }

    /// Simplify a WarmupInfo, removing some redundant tasks
    pub fn simplify(&mut self) {
        self.terms_grouped_by_field.retain(|field, terms| {
            if self.term_dict_fields.contains(field) {
                // we are already about to full-load this dictionary. We only care about terms
                // which needs additional position
                terms.retain(|_term, include_position| *include_position);
            }
            // if no term is left, remove the entry from the hashmap
            !terms.is_empty()
        });
        self.term_ranges_grouped_by_field.retain(|field, terms| {
            if self.term_dict_fields.contains(field) {
                terms.retain(|_term, include_position| *include_position);
            }
            !terms.is_empty()
        });
        // TODO we could remove from terms_grouped_by_field for ranges with no `limit` in
        // term_ranges_grouped_by_field
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::ops::Bound;

    use quickwit_query::query_ast::{query_ast_from_user_text, UserInputQuery};
    use quickwit_query::BooleanOperand;
    use tantivy::schema::{Field, FieldType, Term};

    use super::*;
    use crate::{
        Cardinality, DocMapper, DocMapperBuilder, DocParsingError, FieldMappingEntry, TermRange,
        WarmupInfo, DYNAMIC_FIELD_NAME,
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
        let doc_mapper = DocMapperBuilder::default().try_build().unwrap();
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
        let doc_mapper = DocMapperBuilder::default().try_build().unwrap();
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
            serde_json::from_str::<Box<DocMapper>>(JSON_DEFAULT_DOC_MAPPER)?;
        let expected_default_doc_mapper = DocMapperBuilder::default().try_build()?;
        assert_eq!(
            format!("{deserialized_default_doc_mapper:?}"),
            format!("{expected_default_doc_mapper:?}"),
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_minimal_doc_mapper() -> anyhow::Result<()> {
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<DocMapper>>(r#"{"type": "default"}"#)?;
        let expected_default_doc_mapper = DocMapperBuilder::default().try_build()?;
        assert_eq!(
            format!("{deserialized_default_doc_mapper:?}"),
            format!("{expected_default_doc_mapper:?}"),
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_doc_mapper_default_dynamic_tokenizer() {
        let doc_mapper =
            serde_json::from_str::<Box<DocMapper>>(r#"{"type": "default", "mode": "dynamic"}"#)
                .unwrap();
        let tantivy_schema = doc_mapper.schema();
        let dynamic_field = tantivy_schema.get_field(DYNAMIC_FIELD_NAME).unwrap();
        if let FieldType::JsonObject(json_options) =
            tantivy_schema.get_field_entry(dynamic_field).field_type()
        {
            let text_opt = json_options.get_text_indexing_options().unwrap();
            assert_eq!(text_opt.tokenizer(), "default");
        } else {
            panic!("dynamic field should be of JSON type");
        }
    }

    #[test]
    fn test_doc_mapper_query_with_json_field() {
        let mut doc_mapper_builder = DocMapperBuilder::default();
        doc_mapper_builder
            .doc_mapping
            .field_mappings
            .push(FieldMappingEntry {
                name: "json_field".to_string(),
                mapping_type: FieldMappingType::Json(
                    QuickwitJsonOptions::default(),
                    Cardinality::SingleValued,
                ),
            });
        let doc_mapper = doc_mapper_builder.try_build().unwrap();
        let schema = doc_mapper.schema();
        let query_ast = UserInputQuery {
            user_text: "json_field.toto.titi:hello".to_string(),
            default_fields: None,
            default_operator: BooleanOperand::And,
            lenient: false,
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
        let doc_mapper = DocMapperBuilder::default().try_build().unwrap();
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
        let doc_mapper = DocMapperBuilder::default().try_build().unwrap();
        let schema = doc_mapper.schema();
        let query_ast = query_ast_from_user_text("toto:5", None)
            .parse_user_query(&[])
            .unwrap();
        let (query, _) = doc_mapper.query(schema, &query_ast, true).unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"BooleanQuery { subqueries: [(Should, TermQuery(Term(field=1, type=Json, path=toto, type=I64, 5))), (Should, TermQuery(Term(field=1, type=Json, path=toto, type=Str, "5")))], minimum_number_should_match: 1 }"#
        );
    }

    #[track_caller]
    fn test_validate_doc_aux(
        doc_mapper: &DocMapper,
        doc_json: &str,
    ) -> Result<(), DocParsingError> {
        let json_val: serde_json_borrow::Value = serde_json::from_str(doc_json).unwrap();
        let json_obj = json_val.as_object().unwrap();
        doc_mapper.validate_json_obj(json_obj)
    }

    #[test]
    fn test_validate_doc() {
        const JSON_CONFIG_VALUE: &str = r#"{
            "timestamp_field": "timestamp",
            "field_mappings": [
            {
                "name": "timestamp",
                "type": "datetime",
                "fast": true
            },
            {
                "name": "body",
                "type": "text"
            },
            {
                "name": "response_date",
                "type": "datetime",
                "input_formats": ["rfc3339", "unix_timestamp"]
            },
            {
                "name": "response_time",
                "type": "f64"
            },
            {
                "name": "response_time_no_coercion",
                "type": "f64",
                "coerce": false
            },
            {
                "name": "response_payload",
                "type": "bytes"
            },
            {
                "name": "is_important",
                "type": "bool"
            },
            {
                "name": "properties",
                "type": "json"
            },
            {
                "name": "attributes",
                "type": "object",
                "field_mappings": [
                    {
                        "name": "numbers",
                        "type": "array<i64>"
                    }
                ]
            }]
        }"#;
        let doc_mapper = serde_json::from_str::<DocMapper>(JSON_CONFIG_VALUE).unwrap();
        {
            assert!(test_validate_doc_aux(
                &doc_mapper,
                r#"{ "body": "toto", "timestamp": "2024-01-01T01:01:01Z"}"#
            )
            .is_ok());
        }
        {
            assert!(matches!(
                test_validate_doc_aux(
                    &doc_mapper,
                    r#"{ "response_time": "toto", "timestamp": "2024-01-01T01:01:01Z"}"#
                )
                .unwrap_err(),
                DocParsingError::ValueError(_, _)
            ));
        }
        {
            assert!(test_validate_doc_aux(
                &doc_mapper,
                r#"{ "response_time": "2.3", "timestamp": "2024-01-01T01:01:01Z"}"#
            )
            .is_ok(),);
        }
        {
            // coercion disabled
            assert!(matches!(
                test_validate_doc_aux(
                    &doc_mapper,
                    r#"{"response_time_no_coercion": "2.3", "timestamp": "2024-01-01T01:01:01Z"}"#
                )
                .unwrap_err(),
                DocParsingError::ValueError(_, _)
            ));
        }
        {
            assert!(matches!(
                test_validate_doc_aux(
                    &doc_mapper,
                    r#"{"response_time": [2.3], "timestamp": "2024-01-01T01:01:01Z"}"#
                )
                .unwrap_err(),
                DocParsingError::MultiValuesNotSupported(_)
            ));
        }
        {
            assert!(test_validate_doc_aux(
                &doc_mapper,
                r#"{"attributes": {"numbers": [-2]}, "timestamp": "2024-01-01T01:01:01Z"}"#
            )
            .is_ok());
        }
    }

    #[test]
    fn test_validate_doc_timestamp() {
        const JSON_CONFIG_TS_AT_ROOT: &str = r#"{
            "timestamp_field": "timestamp",
            "field_mappings": [
            {
                "name": "timestamp",
                "type": "datetime",
                "fast": true
            },
            {
                "name": "body",
                "type": "text"
            }
            ]
        }"#;
        const JSON_CONFIG_TS_WITH_DOT: &str = r#"{
            "timestamp_field": "timestamp\\.now",
            "field_mappings": [
            {
                "name": "timestamp.now",
                "type": "datetime",
                "fast": true
            },
            {
                "name": "body",
                "type": "text"
            }
            ]
        }"#;
        const JSON_CONFIG_TS_NESTED: &str = r#"{
            "timestamp_field": "doc.timestamp",
            "field_mappings": [
            {
                "name": "doc",
                "type": "object",
                "field_mappings": [
                    {
                        "name": "timestamp",
                        "type": "datetime",
                        "fast": true
                    }
                ]
            },
            {
                "name": "body",
                "type": "text"
            }
            ]
        }"#;
        let doc_mapper = serde_json::from_str::<DocMapper>(JSON_CONFIG_TS_AT_ROOT).unwrap();
        {
            assert!(test_validate_doc_aux(
                &doc_mapper,
                r#"{ "body": "toto", "timestamp": "2024-01-01T01:01:01Z"}"#
            )
            .is_ok());
        }
        {
            assert!(matches!(
                test_validate_doc_aux(
                    &doc_mapper,
                    r#"{ "body": "toto", "timestamp": "invalid timestamp"}"#
                )
                .unwrap_err(),
                DocParsingError::ValueError(_, _),
            ));
        }
        {
            assert!(matches!(
                test_validate_doc_aux(&doc_mapper, r#"{ "body": "toto", "timestamp": null}"#)
                    .unwrap_err(),
                DocParsingError::RequiredField(_),
            ));
        }
        {
            assert!(matches!(
                test_validate_doc_aux(&doc_mapper, r#"{ "body": "toto"}"#).unwrap_err(),
                DocParsingError::RequiredField(_),
            ));
        }

        let doc_mapper = serde_json::from_str::<DocMapper>(JSON_CONFIG_TS_WITH_DOT).unwrap();
        {
            assert!(test_validate_doc_aux(
                &doc_mapper,
                r#"{ "body": "toto", "timestamp.now": "2024-01-01T01:01:01Z"}"#
            )
            .is_ok());
        }
        {
            assert!(matches!(
                test_validate_doc_aux(
                    &doc_mapper,
                    r#"{ "body": "toto", "timestamp.now": "invalid timestamp"}"#
                )
                .unwrap_err(),
                DocParsingError::ValueError(_, _),
            ));
        }
        {
            assert!(matches!(
                test_validate_doc_aux(
                    &doc_mapper,
                    r#"{ "body": "toto", "timestamp": {"now": "2024-01-01T01:01:01Z"}}"#
                )
                .unwrap_err(),
                DocParsingError::RequiredField(_),
            ));
        }

        let doc_mapper = serde_json::from_str::<DocMapper>(JSON_CONFIG_TS_NESTED).unwrap();
        {
            assert!(test_validate_doc_aux(
                &doc_mapper,
                r#"{ "body": "toto", "doc":{"timestamp": "2024-01-01T01:01:01Z"}}"#
            )
            .is_ok());
        }
        {
            assert!(matches!(
                test_validate_doc_aux(
                    &doc_mapper,
                    r#"{ "body": "toto", "doc.timestamp": "2024-01-01T01:01:01Z"}"#
                )
                .unwrap_err(),
                DocParsingError::RequiredField(_),
            ));
        }
    }

    #[test]
    fn test_validate_doc_mode() {
        const DOC: &str = r#"{ "whatever": "blop" }"#;
        {
            const JSON_CONFIG_VALUE: &str = r#"{ "mode": "strict", "field_mappings": [] }"#;
            let doc_mapper = serde_json::from_str::<DocMapper>(JSON_CONFIG_VALUE).unwrap();
            assert!(matches!(
                test_validate_doc_aux(&doc_mapper, DOC).unwrap_err(),
                DocParsingError::NoSuchFieldInSchema(_)
            ));
        }
        {
            const JSON_CONFIG_VALUE: &str = r#"{ "mode": "lenient", "field_mappings": [] }"#;
            let doc_mapper = serde_json::from_str::<DocMapper>(JSON_CONFIG_VALUE).unwrap();
            assert!(test_validate_doc_aux(&doc_mapper, DOC).is_ok());
        }
        {
            const JSON_CONFIG_VALUE: &str = r#"{ "mode": "dynamic", "field_mappings": [] }"#;
            let doc_mapper = serde_json::from_str::<DocMapper>(JSON_CONFIG_VALUE).unwrap();
            assert!(test_validate_doc_aux(&doc_mapper, DOC).is_ok());
        }
    }

    fn hashset_fast(elements: &[&str]) -> HashSet<FastFieldWarmupInfo> {
        elements
            .iter()
            .map(|elem| FastFieldWarmupInfo {
                name: elem.to_string(),
                with_subfields: false,
            })
            .collect()
    }

    fn automaton_hashset(elements: &[&str]) -> HashSet<Automaton> {
        elements
            .iter()
            .map(|elem| Automaton::Regex(None, elem.to_string()))
            .collect()
    }

    fn hashset_field(elements: &[u32]) -> HashSet<Field> {
        elements
            .iter()
            .map(|elem| Field::from_field_id(*elem))
            .collect()
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
            // this is a 1 element bound, but it's enough for testing.
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
            term_dict_fields: hashset_field(&[1, 2]),
            fast_fields: hashset_fast(&["fast1", "fast2"]),
            field_norms: false,
            terms_grouped_by_field: hashmap(&[(1, "term1", false), (1, "term2", false)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (2, "term1", false),
                (2, "term2", false),
            ]),
            automatons_grouped_by_field: [(
                Field::from_field_id(1),
                automaton_hashset(&["my_reg.*ex"]),
            )]
            .into_iter()
            .collect(),
        };

        // merging with default has no impact
        let mut wi_cloned = wi_base.clone();
        wi_cloned.merge(WarmupInfo::default());
        assert_eq!(wi_cloned, wi_base);

        let mut wi_base = wi_base;
        let wi_2 = WarmupInfo {
            term_dict_fields: hashset_field(&[2, 3]),
            fast_fields: hashset_fast(&["fast2", "fast3"]),
            field_norms: true,
            terms_grouped_by_field: hashmap(&[(2, "term1", false), (1, "term2", true)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (3, "term1", false),
                (2, "term2", true),
            ]),
            automatons_grouped_by_field: [
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(2), automaton_hashset(&["my_reg.*ex"])),
            ]
            .into_iter()
            .collect(),
        };
        wi_base.merge(wi_2.clone());

        assert_eq!(wi_base.term_dict_fields, hashset_field(&[1, 2, 3]));
        assert_eq!(
            wi_base.fast_fields,
            hashset_fast(&["fast1", "fast2", "fast3"])
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

        let expected_automatons = [(1, "my_reg.*ex"), (1, "other-re.ex"), (2, "my_reg.*ex")];
        for (field, regex) in expected_automatons {
            let field = Field::from_field_id(field);
            let automaton = Automaton::Regex(None, regex.to_string());
            assert!(wi_base
                .automatons_grouped_by_field
                .get(&field)
                .unwrap()
                .contains(&automaton));
        }

        // merge is idempotent
        let mut wi_cloned = wi_base.clone();
        wi_cloned.merge(wi_2);
        assert_eq!(wi_cloned, wi_base);
    }

    #[test]
    fn test_warmup_info_simplify() {
        let mut warmup_info = WarmupInfo {
            term_dict_fields: hashset_field(&[1]),
            fast_fields: hashset_fast(&["fast1", "fast2"]),
            field_norms: false,
            terms_grouped_by_field: hashmap(&[
                (1, "term1", false),
                (1, "term2", true),
                (2, "term3", false),
            ]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (1, "term1", false),
                (1, "term2", true),
                (2, "term3", false),
            ]),
            automatons_grouped_by_field: [
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(2), automaton_hashset(&["my_reg.ex"])),
            ]
            .into_iter()
            .collect(),
        };
        let expected = WarmupInfo {
            term_dict_fields: hashset_field(&[1]),
            fast_fields: hashset_fast(&["fast1", "fast2"]),
            field_norms: false,
            terms_grouped_by_field: hashmap(&[(1, "term2", true), (2, "term3", false)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (1, "term2", true),
                (2, "term3", false),
            ]),
            automatons_grouped_by_field: [
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(2), automaton_hashset(&["my_reg.ex"])),
            ]
            .into_iter()
            .collect(),
        };

        warmup_info.simplify();
        assert_eq!(warmup_info, expected);
    }

    #[test]
    #[cfg(feature = "multilang")]
    fn test_doc_mapper_query_with_multilang_field() {
        use quickwit_query::query_ast::TermQuery;
        use tantivy::schema::IndexRecordOption;

        use crate::doc_mapper::{
            QuickwitTextOptions, QuickwitTextTokenizer, TextIndexingOptions, TokenizerType,
        };
        use crate::{TokenizerConfig, TokenizerEntry};
        let mut doc_mapper_builder = DocMapperBuilder::default();
        doc_mapper_builder
            .doc_mapping
            .field_mappings
            .push(FieldMappingEntry {
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
                    Cardinality::SingleValued,
                ),
            });
        doc_mapper_builder
            .doc_mapping
            .tokenizers
            .push(TokenizerEntry {
                name: "multilang".to_string(),
                config: TokenizerConfig {
                    tokenizer_type: TokenizerType::Multilang,
                    filters: Vec::new(),
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
