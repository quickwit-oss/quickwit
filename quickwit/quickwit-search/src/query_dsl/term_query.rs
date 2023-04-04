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

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;

use anyhow::{bail, Context};
use quickwit_doc_mapper::DocMapper;
use serde::{Deserialize, Serialize};
use tantivy::query::TermQuery as TantivyTermQuery;
use tantivy::schema::{Field, FieldEntry, FieldType, IndexRecordOption, IntoIpv6Addr, Type};
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::OffsetDateTime;
use tantivy::{DateTime, Term};

use crate::query_dsl::build_tantivy_query::BuildTantivyQuery;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
#[serde(
    into = "HashMap<String, TermQueryValue>",
    try_from = "HashMap<String, TermQueryValue>"
)]
pub struct TermQuery {
    pub field: String,
    pub value: String,
}

impl TermQuery {
    #[cfg(test)]
    pub fn from_field_value(field: impl ToString, value: impl ToString) -> Self {
        Self {
            field: field.to_string(),
            value: value.to_string(),
        }
    }
}

fn compute_term_query(
    field: Field,
    field_entry: &FieldEntry,
    json_path: &str,
    text: &str,
) -> anyhow::Result<TantivyTermQuery> {
    let field_type = field_entry.field_type();
    let field_name = field_entry.name();
    if !field_type.is_indexed() {
        bail!("Field `{field_name}` is not indexed.");
    }
    if field_type.value_type() != Type::Json && !json_path.is_empty() {
        bail!("Field `{field_name}.{json_path}` does not exist.");
    }
    match *field_type {
        FieldType::U64(_) => {
            let val: u64 = u64::from_str(text)?;
            let term = Term::from_field_u64(field, val);
            Ok(TantivyTermQuery::new(term, IndexRecordOption::Basic))
        }
        FieldType::I64(_) => {
            let val: i64 = i64::from_str(text)?;
            let term = Term::from_field_i64(field, val);
            Ok(TantivyTermQuery::new(term, IndexRecordOption::Basic))
        }
        FieldType::F64(_) => {
            let val: f64 = f64::from_str(text)?;
            let term = Term::from_field_f64(field, val);
            Ok(TantivyTermQuery::new(term, IndexRecordOption::Basic))
        }
        FieldType::Bool(_) => {
            let val: bool = bool::from_str(text)?;
            let term = Term::from_field_bool(field, val);
            Ok(TantivyTermQuery::new(term, IndexRecordOption::Basic))
        }
        FieldType::Date(_) => {
            // TODO handle input format.
            let dt = OffsetDateTime::parse(text, &Rfc3339)?;
            let term = Term::from_field_date(field, DateTime::from_utc(dt));
            Ok(TantivyTermQuery::new(term, IndexRecordOption::Basic))
        }
        FieldType::Str(ref str_options) => {
            let option = str_options
                .get_indexing_options()
                .context("Text field is not indexed")?; //< It should never happen.
            let index_record_option = option.index_option();
            let term = Term::from_field_text(field, text);
            Ok(TantivyTermQuery::new(term, index_record_option))
        }
        FieldType::IpAddr(_) => {
            let ip_v6 = IpAddr::from_str(text)?.into_ipv6_addr();
            let term = Term::from_field_ip_addr(field, ip_v6);
            Ok(TantivyTermQuery::new(term, IndexRecordOption::Basic))
        }
        FieldType::JsonObject(ref _json_options) => {
            // Waiting for us to point to tantivy main.
            todo!();
        }
        FieldType::Facet(_) => {
            todo!();
        }
        FieldType::Bytes(_) => {
            todo!()
        }
    }
}

impl BuildTantivyQuery for TermQuery {
    fn build_tantivy_query(
        &self,
        doc_mapper: &dyn DocMapper,
    ) -> anyhow::Result<Box<dyn tantivy::query::Query>> {
        let schema = doc_mapper.schema();
        let Some((field, path)) = schema.find_field(&self.field)
        else {
            bail!("Failed to find field `{}` in schema.", self.field);
        };
        let field_entry = schema.get_field_entry(field);
        let term_query: TantivyTermQuery =
            compute_term_query(field, field_entry, path, &self.value)?;
        Ok(Box::new(term_query))
    }
}

// Private struct used for serialization.
// It represents the value of a term query. in the json form : `{field: <TermQueryValue>}`.
#[derive(Serialize, Deserialize)]
struct TermQueryValue {
    value: String,
}

impl From<TermQuery> for (String, TermQueryValue) {
    fn from(term_query: TermQuery) -> Self {
        (
            term_query.field,
            TermQueryValue {
                value: term_query.value,
            },
        )
    }
}

impl From<(String, TermQueryValue)> for TermQuery {
    fn from((field, term_query_value): (String, TermQueryValue)) -> Self {
        Self {
            field,
            value: term_query_value.value,
        }
    }
}

impl TryFrom<HashMap<String, TermQueryValue>> for TermQuery {
    type Error = &'static str;

    fn try_from(map: HashMap<String, TermQueryValue>) -> Result<Self, Self::Error> {
        if map.len() != 1 {
            return Err("TermQuery must have exactly one entry");
        }
        Ok(TermQuery::from(map.into_iter().next().expect(
            "There should be exactly one element in the map, as checked by the if statement above.",
        ))) // unwrap justified by the if statement
            // above.
    }
}

impl From<TermQuery> for HashMap<String, TermQueryValue> {
    fn from(term_query: TermQuery) -> HashMap<String, TermQueryValue> {
        let (field, term_query_value) = term_query.into();
        let mut map = HashMap::with_capacity(1);
        map.insert(field, term_query_value);
        map
    }
}

#[cfg(test)]
mod tests {
    use super::TermQuery;

    #[test]
    fn test_term_query_simple() {
        let term_query_json = r#"{ "product_id": { "value": "61809" } }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(
            &term_query,
            &TermQuery::from_field_value("product_id", "61809")
        );
    }
}
