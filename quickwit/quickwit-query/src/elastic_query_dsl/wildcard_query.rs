use serde::Deserialize;

use crate::elastic_query_dsl::ConvertibleToQueryAst;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::query_ast::{QueryAst, WildcardQuery as AstWildcardQuery};

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct WildcardQueryParams {
    value: String,
}

pub type WildcardQuery = OneFieldMap<WildcardQueryParams>;

impl ConvertibleToQueryAst for WildcardQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        Ok(AstWildcardQuery {
            field: self.field,
            value: self.value.value,
            lenient: true,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_query_convert_to_query_ast() {
        let wildcard_query_json = r#"{
            "user_name": {
                "value": "john*"
            }
        }"#;
        let wildcard_query: WildcardQuery = serde_json::from_str(wildcard_query_json).unwrap();
        let query_ast = wildcard_query.convert_to_query_ast().unwrap();

        if let QueryAst::Wildcard(wildcard) = query_ast {
            assert_eq!(wildcard.field, "user_name");
            assert_eq!(wildcard.value, "john*");
            assert!(wildcard.lenient);
        } else {
            panic!("Expected QueryAst::Wildcard");
        }
    }
}
