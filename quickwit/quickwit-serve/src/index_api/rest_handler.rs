// Copyright (C) 2022 Quickwit, Inc.
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

use std::sync::Arc;

use bytes::Bytes;
use quickwit_config::{ConfigFormat, QuickwitConfig, SourceConfig};
use quickwit_core::{IndexService, IndexServiceError};
use quickwit_janitor::FileEntry;
use quickwit_metastore::{IndexMetadata, Split};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::info;
use warp::{Filter, Rejection, Reply};

use crate::format::Format;
use crate::with_arg;

pub fn index_management_handlers(
    index_service: Arc<IndexService>,
    quickwit_config: Arc<QuickwitConfig>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    get_index_metadata_handler(index_service.clone())
        .or(get_indexes_metadatas_handler(index_service.clone()))
        .or(get_all_splits_handler(index_service.clone()))
        .or(create_index_handler(index_service.clone(), quickwit_config))
        .or(delete_index_handler(index_service.clone()))
        .or(create_source_handler(index_service.clone()))
        .or(delete_source_handler(index_service))
}

fn format_response<T: Serialize>(result: Result<T, IndexServiceError>) -> impl Reply {
    Format::default().make_rest_reply_non_serializable_error(result)
}

fn get_index_metadata_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::get())
        .and(with_arg(index_service))
        .then(get_index_metadata)
        .map(format_response)
}

async fn get_index_metadata(
    index_id: String,
    index_service: Arc<IndexService>,
) -> Result<IndexMetadata, IndexServiceError> {
    info!(index_id = %index_id, "get-index");
    index_service.get_index(&index_id).await
}

fn get_indexes_metadatas_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes")
        .and(warp::get())
        .and(with_arg(index_service))
        .then(get_indexes_metadatas)
        .map(format_response)
}

async fn get_all_splits(
    index_id: String,
    index_service: Arc<IndexService>,
) -> Result<Vec<Split>, IndexServiceError> {
    info!(index_id = %index_id, "get-all-splits");
    index_service.get_all_splits(&index_id).await
}

fn get_all_splits_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String / "splits")
        .and(warp::get())
        .and(with_arg(index_service))
        .then(get_all_splits)
        .map(format_response)
}

async fn get_indexes_metadatas(
    index_service: Arc<IndexService>,
) -> Result<Vec<IndexMetadata>, IndexServiceError> {
    info!("get-indexes-metadatas");
    index_service
        .list_indexes()
        .await
        .map_err(|err| IndexServiceError::InternalError(format!("{err:?}")))
}

fn create_index_handler(
    index_service: Arc<IndexService>,
    quickwit_config: Arc<QuickwitConfig>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes")
        // TODO: add a filter on the content type, we support only json.
        .and(warp::post())
        .and(warp::filters::body::bytes())
        .and(with_arg(index_service))
        .and(with_arg(quickwit_config))
        .then(create_index)
        .map(format_response)
}

fn json_body<T: DeserializeOwned + Send>(
) -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 1024).and(warp::body::json())
}

async fn create_index(
    index_config_bytes: Bytes,
    index_service: Arc<IndexService>,
    quickwit_config: Arc<QuickwitConfig>,
) -> Result<IndexMetadata, IndexServiceError> {
    let index_config = quickwit_config::load_index_config_from_user_config(
        ConfigFormat::Json,
        &index_config_bytes,
        &quickwit_config.default_index_root_uri,
    )
    .map_err(IndexServiceError::InvalidConfig)?;
    index_service.create_index(index_config, false).await
}

fn delete_index_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::delete())
        .and(with_arg(index_service))
        .then(delete_index)
        .map(format_response)
}

async fn delete_index(
    index_id: String,
    index_service: Arc<IndexService>,
) -> Result<Vec<FileEntry>, IndexServiceError> {
    info!(index_id = %index_id, "delete-index");
    index_service.delete_index(&index_id, false).await
}

fn create_source_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources")
        .and(warp::post())
        .and(json_body())
        .and(with_arg(index_service))
        .then(create_source)
        .map(format_response)
}

async fn create_source(
    index_id: String,
    source_config: SourceConfig,
    index_service: Arc<IndexService>,
) -> Result<SourceConfig, IndexServiceError> {
    info!(index_id = %index_id, source_id = %source_config.source_id, "create-source");
    index_service.create_source(&index_id, source_config).await
}

fn delete_source_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String)
        .and(warp::delete())
        .and(with_arg(index_service))
        .then(delete_source)
        .map(format_response)
}

async fn delete_source(
    index_id: String,
    source_id: String,
    index_service: Arc<IndexService>,
) -> Result<(), IndexServiceError> {
    info!(index_id = %index_id, source_id = %source_id, "delete-source");
    index_service.delete_source(&index_id, &source_id).await
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_include;
    use quickwit_common::uri::{Protocol, Uri};
    use quickwit_config::{FileSourceParams, SourceParams};
    use quickwit_indexing::mock_split;
    use quickwit_metastore::file_backed_metastore::FileBackedMetastoreFactory;
    use quickwit_metastore::{
        IndexMetadata, Metastore, MetastoreError, MetastoreUriResolver, MockMetastore,
    };
    use quickwit_storage::StorageUriResolver;
    use serde::__private::from_utf8_lossy;
    use serde_json::Value as JsonValue;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::recover_fn;

    async fn build_metastore_for_test() -> Arc<dyn Metastore> {
        let storage_resolver = StorageUriResolver::for_test();
        let metastore_uri_resolver = MetastoreUriResolver::builder()
            .register(
                Protocol::Ram,
                FileBackedMetastoreFactory::new(storage_resolver.clone()),
            )
            .build();
        metastore_uri_resolver
            .resolve(&Uri::from_well_formed("ram://quickwit-test-indexes"))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_rest_get_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/test-index")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "index_id": "test-index",
            "index_uri": "ram:///indexes/test-index",
        });
        assert_json_include!(
            actual: actual_response_json.get("index_config").unwrap(),
            expected: expected_response_json
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_get_non_existing_index() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore, StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/test-index")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_rest_get_all_splits() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_list_all_splits()
            .returning(|_index_id: &str| Ok(vec![mock_split("split_1")]));
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!([{
            "create_timestamp": 0,
            "split_id": "split_1",
        }]);
        assert_json_include!(
            actual: actual_response_json,
            expected: expected_response_json
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_get_list_indexes() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore.expect_list_indexes_metadatas().returning(|| {
            Ok(vec![IndexMetadata::for_test(
                "test-index",
                "ram:///indexes/test-index",
            )])
        });
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let actual_response_arr: &Vec<JsonValue> = actual_response_json.as_array().unwrap();
        assert_eq!(actual_response_arr.len(), 1);
        let actual_index_metadata_json: &JsonValue = &actual_response_arr[0];
        let expected_response_json = serde_json::json!({
            "index_id": "test-index",
            "index_uri": "ram:///indexes/test-index",
        });
        assert_json_include!(
            actual: actual_index_metadata_json.get("index_config").unwrap(),
            expected: expected_response_json
        );
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
        metastore
            .expect_list_splits()
            .returning(|_| Ok(vec![mock_split("split_1")]));
        metastore
            .expect_mark_splits_for_deletion()
            .returning(|_index_id: &str, _splits: &[&str]| Ok(()));
        metastore
            .expect_delete_splits()
            .returning(|_index_id: &str, _splits: &[&str]| Ok(()));
        metastore
            .expect_delete_index()
            .returning(|_index_id: &str| Ok(()));
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
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
    async fn test_rest_delete_on_non_existing_index() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore, StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index")
            .method("DELETE")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_rest_create_delete_index_and_source() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore.clone(), StorageUriResolver::for_test());
        let mut quickwit_config = QuickwitConfig::for_test();
        quickwit_config.default_index_root_uri =
            Uri::from_well_formed("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service), Arc::new(quickwit_config));
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(r#"{"version": "0.4", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        // Create source.
        let source_temp_file = NamedTempFile::new().unwrap();
        let source_temp_path_string = source_temp_file.path().to_string_lossy().to_string();
        let source_config_body = r#"{"version": "0.4", "source_id": "file-source", "source_type": "file", "params": {"filepath": "FILEPATH"}}"#
            .replace("FILEPATH", &source_temp_path_string);
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources")
            .method("POST")
            .json(&true)
            .body(&source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);

        // Check that the source has been added to index metadata.
        let index_metadata = metastore.index_metadata("hdfs-logs").await.unwrap();
        assert!(index_metadata.sources.contains_key("file-source"));
        let source_config = index_metadata.sources.get("file-source").unwrap();
        assert_eq!(source_config.source_type(), "file");
        assert_eq!(
            source_config.source_params,
            SourceParams::File(FileSourceParams {
                filepath: Some(source_temp_file.path().to_path_buf())
            })
        );

        // Check delete source.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources/file-source")
            .method("DELETE")
            .body(&source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let index_metadata = metastore.index_metadata("hdfs-logs").await.unwrap();
        assert!(!index_metadata.sources.contains_key("file-source"));

        // Check delete index.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs")
            .method("DELETE")
            .body(&source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let indexes = metastore.list_indexes_metadatas().await.unwrap();
        assert!(indexes.is_empty());
    }

    #[tokio::test]
    async fn test_rest_create_index_with_bad_config() -> anyhow::Result<()> {
        let metastore = MockMetastore::new();
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(
                r#"{"version": "0.4", "index_id": "hdfs-log", "doc_mapping":
    {"field_mappings":[{"name": "timestamp", "type": "unknown", "fast": true, "indexed":
    true}]}}"#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let body = from_utf8_lossy(resp.body());
        assert!(body.contains("Field `timestamp` has an unknown type"));
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_create_source_with_bad_config() -> anyhow::Result<()> {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore, StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/my-index/sources")
            .method("POST")
            .json(&true)
            .body(r#"{"version": 0.4, "source_id": "file-source""#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let body = from_utf8_lossy(resp.body());
        println!("{}", body);
        assert!(body.contains("invalid type: floating point `0.4`"));
        Ok(())
    }

    #[tokio::test]
    async fn test_rest_delete_non_existing_source() {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            });
        metastore
            .expect_delete_source()
            .returning(|index_id, source_id| {
                assert_eq!(index_id, "quickwit-demo-index");
                Err(MetastoreError::SourceDoesNotExist {
                    source_id: source_id.to_string(),
                })
            });
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/foo-source")
            .method("DELETE")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }
}
