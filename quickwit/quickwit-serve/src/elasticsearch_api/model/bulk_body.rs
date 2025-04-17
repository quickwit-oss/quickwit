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

use quickwit_proto::types::IndexId;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum BulkAction {
    Create(BulkActionMeta),
    Index(BulkActionMeta),
}

impl BulkAction {
    pub fn into_index_id(self) -> Option<IndexId> {
        match self {
            BulkAction::Index(meta) => meta.index_id,
            BulkAction::Create(meta) => meta.index_id,
        }
    }

    pub fn into_meta(self) -> BulkActionMeta {
        match self {
            BulkAction::Create(meta) => meta,
            BulkAction::Index(meta) => meta,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct BulkActionMeta {
    #[serde(alias = "_index")]
    #[serde(default)]
    pub index_id: Option<IndexId>,
    #[serde(alias = "_id")]
    #[serde(default)]
    pub es_doc_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::elasticsearch_api::model::BulkAction;
    use crate::elasticsearch_api::model::bulk_body::BulkActionMeta;

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
                    index_id: Some("test".to_string()),
                    es_doc_id: Some("2".to_string()),
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
                    index_id: Some("test".to_string()),
                    es_doc_id: None,
                })
            );
        }
        {
            let bulk_action_json = r#"{
                "create": {
                    "_id": "3"
                }
            }"#;
            let bulk_action = serde_json::from_str::<BulkAction>(bulk_action_json).unwrap();
            assert_eq!(
                bulk_action,
                BulkAction::Create(BulkActionMeta {
                    index_id: None,
                    es_doc_id: Some("3".to_string()),
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
