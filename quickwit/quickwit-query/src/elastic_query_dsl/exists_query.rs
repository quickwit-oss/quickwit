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

use crate::elastic_query_dsl::ConvertibleToQueryAst;
use crate::query_ast::{self, QueryAst};

#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct ExistsQuery {
    field: String,
}

impl ConvertibleToQueryAst for ExistsQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        Ok(QueryAst::FieldPresence(query_ast::FieldPresenceQuery {
            field: self.field,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::elastic_query_dsl::exists_query::ExistsQuery;

    #[test]
    fn test_dsl_exists_query_deserialize_simple() {
        let exists_query_json = r#"{
           "field": "privileged"
        }"#;
        let bool_query: ExistsQuery = serde_json::from_str(exists_query_json).unwrap();
        assert_eq!(
            &bool_query,
            &ExistsQuery {
                field: "privileged".to_string(),
            }
        );
    }
}
