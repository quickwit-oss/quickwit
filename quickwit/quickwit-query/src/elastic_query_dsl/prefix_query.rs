use serde::Deserialize;

use crate::elastic_query_dsl::ConvertibleToQueryAst;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::query_ast::{QueryAst, WildcardQuery as AstWildcardQuery};

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct PrefixQueryParams {
    value: String,
}

pub type PrefixQuery = OneFieldMap<PrefixQueryParams>;

impl ConvertibleToQueryAst for PrefixQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let wildcard = format!(
            "{}*",
            self.value
                .value
                .replace("\\", "\\\\")
                .replace("*", "\\*")
                .replace("?", "\\?")
        );
        Ok(AstWildcardQuery {
            field: self.field,
            value: wildcard,
            lenient: true,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_query_convert_to_query_ast() {
        let prefix_query_json = r#"{
            "user_name": {
                "value": "john"
            }
        }"#;
        let prefix_query: PrefixQuery = serde_json::from_str(prefix_query_json).unwrap();
        let query_ast = prefix_query.convert_to_query_ast().unwrap();

        if let QueryAst::Wildcard(prefix) = query_ast {
            assert_eq!(prefix.field, "user_name");
            assert_eq!(prefix.value, "john*");
            assert!(prefix.lenient);
        } else {
            panic!("Expected QueryAst::Prefix, got {:?}", query_ast);
        }
    }

    #[test]
    fn test_prefix_query_convert_to_query_ast_special_chars() {
        let prefix_query_json = r#"{
            "user_name": {
                "value": "a\\dm?n*"
            }
        }"#;
        let prefix_query: PrefixQuery = serde_json::from_str(prefix_query_json).unwrap();
        let query_ast = prefix_query.convert_to_query_ast().unwrap();

        if let QueryAst::Wildcard(prefix) = query_ast {
            assert_eq!(prefix.field, "user_name");
            assert_eq!(prefix.value, "a\\\\dm\\?n\\**");
            assert!(prefix.lenient);
        } else {
            panic!("Expected QueryAst::Prefix, got {:?}", query_ast);
        }
    }
}
