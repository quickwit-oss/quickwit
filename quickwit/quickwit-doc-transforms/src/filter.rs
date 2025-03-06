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

use std::str::FromStr;

use serde::Deserialize;
use vrl::datadog_filter::{build_matcher, Filter, Matcher, Resolver, Run};
use vrl::datadog_search_syntax::{Comparison, QueryNode};

use crate::error::PipelineError;
use crate::ProcessedLog;

/// Parses a query using the VRL parser.
///
/// The VRL parser implements the Datadog search syntax. It is used to match
/// logs based on their attributes and tags.
pub fn build_vrl_matcher(query: &str) -> Result<Box<dyn Matcher<ProcessedLog>>, PipelineError> {
    let node = QueryNode::from_str(query).map_err(|e| PipelineError::QueryParse {
        message: e.to_string(),
    })?;

    Ok(build_matcher(&node, &FilterResolver)?)
}

#[derive(Debug, Clone, Deserialize)]
struct FilterResolver;

/// Uses the default `Resolver`, to build a `Vec<Field>`.
///
/// TODO: Implement the `Resolver` trait for `FilterResolver` instead of the default.
impl Resolver for FilterResolver {}

use vrl::datadog_search_syntax::Field;
use vrl::datadog_search_syntax::Field::{Attribute as Custom, Reserved as CoreAttribute};

/// Note: All reserved fields are of type String except for `timestamp` and `tags`, which are
/// unhandled below currently
impl Filter<ProcessedLog> for FilterResolver {
    fn exists(
        &self,
        field: Field,
    ) -> Result<Box<dyn vrl::datadog_filter::Matcher<ProcessedLog>>, vrl::path::PathParseError>
    {
        match field {
            Field::Default(_) => todo!(),
            CoreAttribute(_attr) => Ok(Box::new(true)), // They always exist
            Custom(_custom_path) => todo!(),
            Field::Tag(tag_str) => Ok(Run::boxed(move |log: &ProcessedLog| {
                log.tag.contains_key(&tag_str)
            })),
        }
    }

    fn equals(
        &self,
        field: Field,
        to_match: &str,
    ) -> Result<Box<dyn vrl::datadog_filter::Matcher<ProcessedLog>>, vrl::path::PathParseError>
    {
        let to_match = to_match.to_string();
        match field {
            Field::Default(_) => todo!(),
            CoreAttribute(attr) => Ok(Run::boxed(move |log: &ProcessedLog| {
                log.get_core_string_field_by_name(&attr)
                    .map(|v| v == to_match)
                    .unwrap_or(false)
            })),
            Custom(_custom_path) => todo!(),
            Field::Tag(tag_str) => Ok(Run::boxed(move |log: &ProcessedLog| {
                log.tag
                    .get(&tag_str)
                    .map(|v| v.contains(&to_match))
                    .unwrap_or(false)
            })),
        }
    }

    fn prefix(
        &self,
        _field: Field,
        _prefix: &str,
    ) -> Result<Box<dyn vrl::datadog_filter::Matcher<ProcessedLog>>, vrl::path::PathParseError>
    {
        todo!()
    }

    fn wildcard(
        &self,
        _field: Field,
        _wildcard: &str,
    ) -> Result<Box<dyn vrl::datadog_filter::Matcher<ProcessedLog>>, vrl::path::PathParseError>
    {
        todo!()
    }

    fn compare(
        &self,
        _field: Field,
        _comparator: Comparison,
        _comparison_value: vrl::datadog_search_syntax::ComparisonValue,
    ) -> Result<Box<dyn vrl::datadog_filter::Matcher<ProcessedLog>>, vrl::path::PathParseError>
    {
        todo!()
    }
}
