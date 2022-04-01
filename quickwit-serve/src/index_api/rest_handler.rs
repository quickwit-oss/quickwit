// Copyright (C) 2021 Quickwit, Inc.
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

use std::convert::Infallible;
use std::sync::Arc;

use quickwit_config::IndexConfig;
use quickwit_core::IndexService;
use quickwit_search::SearchError;
use serde::de::DeserializeOwned;
use tracing::info;
use warp::{Filter, Rejection};

use crate::format::Format;

pub fn index_management_handlers(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    get_index_metadata_handler(index_service.clone())
        .or(get_indexes_metadatas_handler(index_service.clone()))
        .or(get_all_splits_handler(index_service.clone()))
        .or(create_index_handler(index_service.clone()))
        .or(delete_index_handler(index_service))
}

fn get_index_metadata_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::get())
        .and(warp::any().map(move || index_service.clone()))
        .and_then(get_index_metadata)
}

async fn get_index_metadata(
    index_id: String,
    index_service: Arc<IndexService>,
) -> Result<impl warp::Reply, Infallible> {
    info!(index_id = %index_id, "get-index");
    let index_metadata = index_service.get_index(&index_id).await;
    Ok(Format::default().make_rest_reply_non_serializable_error(index_metadata))
}

fn get_indexes_metadatas_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes")
        .and(warp::get())
        .and(warp::path::end().map(move || index_service.clone()))
        .and_then(get_indexes_metadatas)
}

async fn get_all_splits(
    index_id: String,
    index_service: Arc<IndexService>,
) -> Result<impl warp::Reply, Infallible> {
    info!(index_id = %index_id, "get-index");
    let splits = index_service.get_all_splits(&index_id).await;
    Ok(Format::default().make_rest_reply_non_serializable_error(splits))
}

fn get_all_splits_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String / "splits")
        .and(warp::get())
        .and(warp::path::end().map(move || index_service.clone()))
        .and_then(get_all_splits)
}

async fn get_indexes_metadatas(
    index_service: Arc<IndexService>,
) -> Result<impl warp::Reply, Infallible> {
    info!("get-indexes-metadatas");
    let index_metadata = index_service.get_indexes().await.map_err(SearchError::from);
    Ok(Format::default().make_rest_reply_non_serializable_error(index_metadata))
}

fn create_index_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes")
        .and(warp::post())
        .and(json_body())
        .and(warp::path::end().map(move || index_service.clone()))
        .and_then(create_index)
}

fn json_body<T: DeserializeOwned + Send>(
) -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 1024).and(warp::body::json())
}

async fn create_index(
    index_config: IndexConfig,
    index_service: Arc<IndexService>,
) -> Result<impl warp::Reply, Infallible> {
    info!(index_id = %index_config.index_id, "create-index");
    let index_metadata = index_service.create_index(index_config).await;
    Ok(Format::default().make_rest_reply_non_serializable_error(index_metadata))
}

fn delete_index_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::delete())
        .and(warp::path::end().map(move || index_service.clone()))
        .and_then(delete_index)
}

async fn delete_index(
    index_id: String,
    index_service: Arc<IndexService>,
) -> Result<impl warp::Reply, Infallible> {
    info!(index_id = %index_id, "delete-index");
    let file_entries_res = index_service.delete_index(&index_id, false).await;
    Ok(Format::default().make_rest_reply_non_serializable_error(file_entries_res))
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use assert_json_diff::assert_json_include;
    use quickwit_common::uri::Uri;
    use quickwit_indexing::mock_split;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_storage::StorageUriResolver;

    use super::*;
    use crate::recover_fn;

    #[tokio::test]
    async fn test_rest_get_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            });
        let index_service = IndexService::new(
            Arc::new(metastore),
            StorageUriResolver::for_test(),
            Uri::new("file:///default-index-uri".to_string()),
        );
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "index_id": "quickwit-demo-index",
            "index_uri": "file:///path/to/index/quickwit-demo-index",
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_get_all_splits() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_list_all_splits()
            .returning(|_index_id: &str| Ok(vec![mock_split("split_1")]));
        let index_service = IndexService::new(
            Arc::new(metastore),
            StorageUriResolver::for_test(),
            "file:///default-index-uri".to_string(),
        );
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!([{
            "create_timestamp": 0,
            "split_id": "split_1",
        }]);
        assert_json_include!(actual: resp_json, expected: expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_delete_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split_1")])
            },
        );
        metastore
            .expect_mark_splits_for_deletion()
            .returning(|_index_id: &str, _splits: &[&str]| Ok(()));
        metastore
            .expect_delete_splits()
            .returning(|_index_id: &str, _splits: &[&str]| Ok(()));
        metastore
            .expect_delete_index()
            .returning(|_index_id: &str| Ok(()));
        let index_service = IndexService::new(
            Arc::new(metastore),
            StorageUriResolver::for_test(),
            Uri::new("file:///default-index-uri".to_string()),
        );
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index")
            .method("DELETE")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!([{
            "file_name": "split_1.split",
            "file_size_in_bytes": 800,
        }]);
        assert_json_include!(actual: resp_json, expected: expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_get_list_indexes() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore.expect_list_indexes_metadatas().returning(|| {
            Ok(vec![IndexMetadata::for_test(
                "quickwit-demo-index",
                "file:///path/to/index/quickwit-demo-index",
            )])
        });
        let index_service = IndexService::new(
            Arc::new(metastore),
            StorageUriResolver::for_test(),
            Uri::new("file:///default-index-uri".to_string()),
        );
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!([{
            "index_id": "quickwit-demo-index",
            "index_uri": "file:///path/to/index/quickwit-demo-index",
        }]);
        assert_json_include!(actual: resp_json, expected: expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_create_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_create_index()
            .returning(|_index_metadata: IndexMetadata| Ok(()));
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "hdfs-log",
                    "file:///default-index-uri/hdfs-logs",
                ))
            });
        let index_service = IndexService::new(
            Arc::new(metastore),
            StorageUriResolver::for_test(),
            Uri::new("file:///default-index-uri".to_string()),
        );
        let index_management_handler = super::index_management_handlers(Arc::new(index_service));
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(r#"{"version": 0, "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "index_id": "hdfs-log",
            "index_uri": "file:///default-index-uri/hdfs-logs",
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_create_index_with_bad_config() -> anyhow::Result<()> {
        let metastore = MockMetastore::new();
        let index_service = IndexService::new(
            Arc::new(metastore),
            StorageUriResolver::for_test(),
            Uri::new("file:///default-index-uri".to_string()),
        );
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(r#"{"version": 0, "index_id": "hdfs-log", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "unknown", "fast": true, "indexed": true}]}}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 400);
        Ok(())
    }
}
