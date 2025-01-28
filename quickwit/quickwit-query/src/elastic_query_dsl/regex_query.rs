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

use serde::Deserialize;

use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::ConvertibleToQueryAst;
use crate::query_ast::{QueryAst, RegexQuery as AstRegexQuery};

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct RegexQueryParams {
    value: String,
    // we could probably add case_insensitive
}

pub type RegexQuery = OneFieldMap<RegexQueryParams>;

impl ConvertibleToQueryAst for RegexQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        Ok(AstRegexQuery {
            field: self.field,
            regex: self.value.value,
        }
        .into())
    }
}
