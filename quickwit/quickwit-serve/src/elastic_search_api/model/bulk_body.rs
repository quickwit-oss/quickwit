use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum BulkAction {
    Index(BulkActionMeta),
    Create(BulkActionMeta),
}

impl BulkAction {
    pub fn into_index(self) -> String {
        match self {
            BulkAction::Index(meta) => meta.index_id,
            BulkAction::Create(meta) => meta.index_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct BulkActionMeta {
    #[serde(alias = "_index")]
    pub index_id: String,
    #[serde(alias = "_id")]
    #[serde(default)]
    pub doc_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::elastic_search_api::model::{BulkAction, BulkActionMeta};

    #[test]
    fn test_bulk_action_serde() {
        {
            let bulk_action_json = r#"{
                "create": {
                    "_index": "test",
                    "_id" : "2"
                }
            }"#;
            let bulk_action = serde_json::from_str::<BulkAction>(bulk_action_json).unwrap();
            assert_eq!(
                bulk_action,
                BulkAction::Create(BulkActionMeta {
                    index_id: "test".to_string(),
                    doc_id: Some("2".to_string()),
                })
            );
        }
        {
            let bulk_action_json = r#"{
                "create": {
                    "_index": "test"
                }
            }"#;
            let bulk_action = serde_json::from_str::<BulkAction>(bulk_action_json).unwrap();
            assert_eq!(
                bulk_action,
                BulkAction::Create(BulkActionMeta {
                    index_id: "test".to_string(),
                    doc_id: None,
                })
            );
        }
        {
            let bulk_action_json = r#"{
                "delete": {
                    "_index": "test",
                    "_id": "2"
                }
            }"#;
            serde_json::from_str::<BulkAction>(bulk_action_json).unwrap_err();
        }
    }
}
