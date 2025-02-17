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

use std::any::type_name;

use bytes::Bytes;
use quickwit_config::{ConfigFormat, IndexTemplate, IndexTemplateId, VersionedIndexTemplate};
use quickwit_proto::metastore::{
    serde_utils, CreateIndexTemplateRequest, DeleteIndexTemplatesRequest, GetIndexTemplateRequest,
    ListIndexTemplatesRequest, MetastoreError, MetastoreResult, MetastoreService,
    MetastoreServiceClient,
};
use serde_json::Value as JsonValue;
use warp::reject::Rejection;
use warp::{Filter, Reply};

use crate::format::{extract_config_format, extract_format_from_qs};
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;
use crate::with_arg;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        create_index_template,
        get_index_template,
        update_index_template,
        delete_index_template,
        list_index_templates,
    ),
    components(schemas(VersionedIndexTemplate))
)]
pub(crate) struct IndexTemplateApi;

pub(crate) fn index_template_api_handlers(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    create_index_template_handler(metastore.clone())
        .or(get_index_template_handler(metastore.clone()))
        .or(update_index_template_handler(metastore.clone()))
        .or(delete_index_template_handler(metastore.clone()))
        .or(list_index_templates_handler(metastore.clone()))
        .recover(recover_fn)
        .boxed()
}

fn create_index_template_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("templates")
        .and(warp::post())
        .and(warp::filters::body::bytes())
        .and(extract_config_format())
        .and(with_arg(metastore))
        .then(create_index_template)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    post,
    tag = "Templates",
    path = "/templates",
    request_body = VersionedIndexTemplate,
    responses(
        (status = 200, description = "The index template was successfully created.", body = VersionedIndexTemplate)
    ),
)]
/// Creates a new index template.
async fn create_index_template(
    body: Bytes,
    config_format: ConfigFormat,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<IndexTemplate> {
    let index_template: IndexTemplate =
        config_format
            .parse(&body)
            .map_err(|error| MetastoreError::JsonDeserializeError {
                struct_name: type_name::<IndexTemplate>().to_string(),
                message: error.to_string(),
            })?;
    index_template.validate().map_err(|error| {
        let message = format!("invalid index template: {error}");
        MetastoreError::InvalidArgument { message }
    })?;
    let index_template_json = serde_utils::to_json_str(&index_template)?;
    let create_index_template = CreateIndexTemplateRequest {
        index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template)
        .await?;
    Ok(index_template)
}

fn get_index_template_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("templates" / String)
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_index_template)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    get,
    tag = "Templates",
    path = "/templates/{template_id}",
    responses(
        (status = 200, description = "The index template was successfully retrieved.", body = VersionedIndexTemplate),
        (status = 404, description = "The index template was not found.")
    ),
)]
/// Retrieves the index template identified by `template_id`.
async fn get_index_template(
    template_id: IndexTemplateId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<IndexTemplate> {
    let get_index_template_request = GetIndexTemplateRequest { template_id };
    let get_index_template_response = metastore
        .get_index_template(get_index_template_request)
        .await?;
    let index_template: IndexTemplate =
        serde_utils::from_json_str(&get_index_template_response.index_template_json)?;
    Ok(index_template)
}

fn update_index_template_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("templates" / String)
        .and(warp::put())
        .and(warp::filters::body::bytes())
        .and(extract_config_format())
        .and(with_arg(metastore))
        .then(update_index_template)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    put,
    tag = "Templates",
    path = "/templates/{template_id}",
    request_body = VersionedIndexTemplate,
    responses(
        (status = 200, description = "The index template was successfully retrieved.", body = VersionedIndexTemplate),
        (status = 404, description = "The index template was not found.")
    ),
)]
/// Updates the index template identified by `template_id`.
async fn update_index_template(
    template_id: IndexTemplateId,
    body: Bytes,
    config_format: ConfigFormat,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<IndexTemplate> {
    let mut json_value: JsonValue =
        config_format
            .parse(&body)
            .map_err(|error| MetastoreError::JsonDeserializeError {
                struct_name: type_name::<IndexTemplate>().to_string(),
                message: error.to_string(),
            })?;
    json_value["template_id"] = JsonValue::String(template_id);

    if let Some(JsonValue::Number(number)) = json_value.get("version") {
        json_value["version"] = JsonValue::String(number.to_string());
    }
    let index_template: IndexTemplate = serde_utils::from_json_value(json_value)?;
    index_template.validate().map_err(|error| {
        let message = format!("invalid index template: {error}");
        MetastoreError::InvalidArgument { message }
    })?;
    let index_template_json = serde_utils::to_json_str(&index_template)?;
    let create_index_template = CreateIndexTemplateRequest {
        index_template_json,
        overwrite: true,
    };
    metastore
        .create_index_template(create_index_template)
        .await?;
    Ok(index_template)
}

fn delete_index_template_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("templates" / String)
        .and(warp::delete())
        .and(with_arg(metastore))
        .then(delete_index_template)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    delete,
    tag = "Templates",
    path = "/templates/{template_id}",
    responses(
        (status = 200, description = "The index template was successfully deleted."),
        (status = 404, description = "The index template was not found.")
    ),
)]
/// Deletes the index template identified by the provided `template_id`.
async fn delete_index_template(
    template_id: IndexTemplateId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<()> {
    let template_ids = vec![template_id];
    let delete_index_templates_request = DeleteIndexTemplatesRequest { template_ids };
    metastore
        .delete_index_templates(delete_index_templates_request)
        .await?;
    Ok(())
}

fn list_index_templates_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("templates")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(list_index_templates)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    get,
    tag = "Templates",
    path = "/templates",
    responses(
        (status = 200, description = "The index template was successfully retrieved.", body = [VersionedIndexTemplate]),
    ),
)]
/// Retrieves all the index templates stored in the metastore.
async fn list_index_templates(
    metastore: MetastoreServiceClient,
) -> MetastoreResult<Vec<IndexTemplate>> {
    let list_index_templates_request = ListIndexTemplatesRequest {};
    let list_index_templates_response = metastore
        .list_index_templates(list_index_templates_request)
        .await?;
    let index_templates: Vec<IndexTemplate> = list_index_templates_response
        .index_templates_json
        .into_iter()
        .map(|index_template_json| {
            serde_utils::from_json_str::<IndexTemplate>(&index_template_json)
        })
        .collect::<MetastoreResult<_>>()?;
    Ok(index_templates)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::metastore::{
        EmptyResponse, EntityKind, GetIndexTemplateResponse, ListIndexTemplatesResponse,
        MockMetastoreService,
    };
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn test_create_index_template() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_create_index_template()
            .return_once(|request| {
                assert!(!request.overwrite);

                let index_template: IndexTemplate =
                    serde_json::from_str(&request.index_template_json).unwrap();

                assert_eq!(index_template.template_id, "test-template-foo");
                assert_eq!(index_template.index_id_patterns, ["test-index-foo*"]);

                Ok(EmptyResponse {})
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let create_index_template_handler = create_index_template_handler(metastore);
        let response = warp::test::request()
            .path("/templates")
            .method("POST")
            .json(&json!({
                "version": "0.7",
                "template_id": "test-template-foo",
                "index_id_patterns": ["test-index-foo*"],
                "doc_mapping": {},
            }))
            .reply(&create_index_template_handler)
            .await;
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_get_index_template() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_get_index_template()
            .withf(|request| request.template_id == "test-template-foo")
            .return_once(|request| {
                assert_eq!(request.template_id, "test-template-foo");

                let error = MetastoreError::NotFound(EntityKind::IndexTemplate {
                    template_id: request.template_id,
                });
                Err(error)
            });
        mock_metastore
            .expect_get_index_template()
            .withf(|request| request.template_id == "test-template-bar")
            .return_once(|request| {
                assert_eq!(request.template_id, "test-template-bar");

                let index_template =
                    IndexTemplate::for_test("test-template-bar", &["test-index-bar*"], 100);
                let index_template_json = serde_utils::to_json_str(&index_template).unwrap();
                let response = GetIndexTemplateResponse {
                    index_template_json,
                };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let get_index_template_handler = get_index_template_handler(metastore);

        let response = warp::test::request()
            .path("/templates/test-template-foo")
            .reply(&get_index_template_handler)
            .await;
        assert_eq!(response.status(), 404);

        let response = warp::test::request()
            .path("/templates/test-template-bar")
            .reply(&get_index_template_handler)
            .await;
        assert_eq!(response.status(), 200);

        let index_template: IndexTemplate = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(index_template.template_id, "test-template-bar");
        assert_eq!(index_template.index_id_patterns, ["test-index-bar*"]);
        assert_eq!(index_template.priority, 100);
    }

    #[tokio::test]
    async fn test_update_index_template() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_create_index_template()
            .return_once(|request| {
                assert!(request.overwrite);

                let index_template: IndexTemplate =
                    serde_json::from_str(&request.index_template_json).unwrap();

                assert_eq!(index_template.template_id, "test-template-foo");
                assert_eq!(index_template.index_id_patterns, ["test-index-foo*"]);

                Ok(EmptyResponse {})
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let update_index_template_handler = update_index_template_handler(metastore);
        let response = warp::test::request()
            .path("/templates/test-template-foo")
            .method("PUT")
            .json(&json!({
                "version": "0.7",
                "template_id": "test-template-bar", // This `template_id` should be ignored and overridden by the path parameter.
                "index_id_patterns": ["test-index-foo*"],
                "doc_mapping": {},
            }))
            .reply(&update_index_template_handler)
            .await;
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_delete_index_template() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_delete_index_templates()
            .return_once(|request| {
                assert_eq!(request.template_ids, ["test-template-foo"]);
                Ok(EmptyResponse {})
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let delete_index_template_handler = delete_index_template_handler(metastore);
        let response = warp::test::request()
            .path("/templates/test-template-foo")
            .method("DELETE")
            .reply(&delete_index_template_handler)
            .await;
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_list_index_templates() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_index_templates()
            .return_once(|_request| {
                let index_template_foo =
                    IndexTemplate::for_test("test-template-foo", &["test-index-foo*"], 100);
                let index_template_foo_json = serde_json::to_string(&index_template_foo).unwrap();

                let index_template_bar =
                    IndexTemplate::for_test("test-template-bar", &["test-index-bar*"], 200);
                let index_template_bar_json = serde_json::to_string(&index_template_bar).unwrap();

                let response = ListIndexTemplatesResponse {
                    index_templates_json: vec![index_template_foo_json, index_template_bar_json],
                };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let list_index_templates_handler = list_index_templates_handler(metastore);
        let response = warp::test::request()
            .path("/templates")
            .method("GET")
            .reply(&list_index_templates_handler)
            .await;
        assert_eq!(response.status(), 200);

        let mut index_templates: Vec<IndexTemplate> =
            serde_json::from_slice(response.body()).unwrap();
        index_templates.sort_unstable_by(|left, right| left.template_id.cmp(&right.template_id));

        assert_eq!(index_templates.len(), 2);
        assert_eq!(index_templates[0].template_id, "test-template-bar");
        assert_eq!(index_templates[1].template_id, "test-template-foo");
    }
}
