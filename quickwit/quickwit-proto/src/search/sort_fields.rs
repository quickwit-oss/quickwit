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

use super::SortOrder;

/// Parses field names with an optional direction prefix. Bare names and `+` use
/// `default_order`; `-` reverses it. Indexing uses ascending by default, while the
/// REST search API historically uses descending.
///
/// Callers handle list delimiters, whitespace, empty entries, and field validation.
/// Only one leading sign is consumed; the rest of each field name is preserved verbatim.
pub fn parse_sort_fields(
    fields: impl IntoIterator<Item = impl AsRef<str>>,
    default_order: SortOrder,
) -> Vec<(String, SortOrder)> {
    let reverse_order = match default_order {
        SortOrder::Asc => SortOrder::Desc,
        SortOrder::Desc => SortOrder::Asc,
    };
    fields
        .into_iter()
        .map(|expression| {
            let expression = expression.as_ref();
            let (field, order) = if let Some(field) = expression.strip_prefix('-') {
                (field, reverse_order)
            } else {
                (
                    expression.strip_prefix('+').unwrap_or(expression),
                    default_order,
                )
            };
            (field.to_string(), order)
        })
        .collect()
}
