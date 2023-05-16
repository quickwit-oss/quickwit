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

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum BulkAction {
    Index(BulkActionMeta),
    Create(BulkActionMeta),
}

impl BulkAction {
    pub fn into_index(self) -> Option<String> {
        match self {
            BulkAction::Index(meta) => meta.index_id,
            BulkAction::Create(meta) => meta.index_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct BulkActionMeta {
    #[serde(alias = "_index")]
    #[serde(default)]
    pub index_id: Option<String>,
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
                    index_id: Some("test".to_string()),
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
                    index_id: Some("test".to_string()),
                    doc_id: None,
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
                    doc_id: Some("3".to_string()),
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
