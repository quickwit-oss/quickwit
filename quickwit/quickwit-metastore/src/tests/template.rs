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

use quickwit_common::rand::append_random_suffix;
use quickwit_config::IndexTemplate;
use quickwit_proto::metastore::{
    CreateIndexTemplateRequest, DeleteIndexTemplatesRequest, EntityKind,
    FindIndexTemplateMatchesRequest, GetIndexTemplateRequest, ListIndexTemplatesRequest,
    MetastoreError, MetastoreResult, MetastoreService, serde_utils,
};

use super::DefaultForTest;
use crate::MetastoreServiceExt;

async fn list_all_index_templates(
    metastore: &mut dyn MetastoreService,
) -> MetastoreResult<Vec<IndexTemplate>> {
    let list_index_templates_request = ListIndexTemplatesRequest {};
    let list_index_templates_response = metastore
        .list_index_templates(list_index_templates_request)
        .await?;
    list_index_templates_response
        .index_templates_json
        .into_iter()
        .map(|index_template_json| serde_utils::from_json_str(&index_template_json))
        .collect()
}

async fn cleanup_templates(metastore: &mut dyn MetastoreService) {
    let template_ids = list_all_index_templates(metastore)
        .await
        .unwrap()
        .into_iter()
        .map(|index_template| index_template.template_id)
        .collect::<Vec<_>>();

    let delete_templates_request = DeleteIndexTemplatesRequest { template_ids };
    metastore
        .delete_index_templates(delete_templates_request)
        .await
        .unwrap();
}

pub async fn test_metastore_create_index_template<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;
    cleanup_templates(&mut metastore).await;

    let template_id = append_random_suffix("test-create-template");
    let index_template = IndexTemplate::for_test(&template_id, &["test-template-*"], 100);
    let index_template_json = serde_json::to_string(&index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: index_template_json.clone(),
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let index_templates = list_all_index_templates(&mut metastore).await.unwrap();
    assert_eq!(index_templates.len(), 1);

    assert_eq!(index_templates[0].template_id, template_id);
    assert_eq!(index_templates[0].index_id_patterns, ["test-template-*"]);
    assert_eq!(index_templates[0].priority, 100);

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: index_template_json.clone(),
        overwrite: false,
    };
    let error = metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap_err();
    assert!(
        matches!(error, MetastoreError::AlreadyExists(EntityKind::IndexTemplate { template_id }) if template_id.starts_with("test-create-template"))
    );

    let index_template = IndexTemplate::for_test(&template_id, &["test-template-*"], 200);
    let index_template_json = serde_json::to_string(&index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: index_template_json.clone(),
        overwrite: true,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let index_templates = list_all_index_templates(&mut metastore).await.unwrap();
    assert_eq!(index_templates.len(), 1);

    let index_templates = list_all_index_templates(&mut metastore).await.unwrap();
    assert_eq!(index_templates.len(), 1);
    assert_eq!(index_templates[0].priority, 200);
}

pub async fn test_metastore_get_index_template<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;
    cleanup_templates(&mut metastore).await;

    let template_id = append_random_suffix("test-get-template");
    let index_template = IndexTemplate::for_test(&template_id, &["test-template"], 100);
    let index_template_json = serde_json::to_string(&index_template).unwrap();

    let get_index_template_request = GetIndexTemplateRequest {
        template_id: template_id.clone(),
    };
    let error = metastore
        .get_index_template(get_index_template_request.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(error, MetastoreError::NotFound(EntityKind::IndexTemplate { template_id }) if template_id.starts_with("test-get-template"))
    );

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let get_index_template_response = metastore
        .get_index_template(get_index_template_request.clone())
        .await
        .unwrap();
    let index_template: IndexTemplate =
        serde_utils::from_json_str(&get_index_template_response.index_template_json).unwrap();

    assert_eq!(index_template.template_id, template_id);
    assert_eq!(index_template.index_id_patterns, ["test-template"]);
    assert_eq!(index_template.priority, 100);
}

pub async fn test_metastore_find_index_template_matches<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;
    cleanup_templates(&mut metastore).await;

    let foo_template_id = append_random_suffix("test-template-foo");
    let foo_index_template = IndexTemplate::for_test(
        &foo_template_id,
        &["test-index-foo*", "-test-index-fool"],
        200,
    );
    let foo_index_template_json = serde_json::to_string(&foo_index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: foo_index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let foobar_template_id = append_random_suffix("test-template-foobar");
    let foobar_index_template =
        IndexTemplate::for_test(&foobar_template_id, &["test-index-foobar*"], 100);
    let foobar_index_template_json = serde_json::to_string(&foobar_index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: foobar_index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let bar_template_id = append_random_suffix("test-template-bar");
    let bar_index_template = IndexTemplate::for_test(&bar_template_id, &["test-index-bar*"], 100);
    let bar_index_template_json = serde_json::to_string(&bar_index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: bar_index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let find_index_template_matches = FindIndexTemplateMatchesRequest {
        index_ids: vec![
            "test-index-foo".to_string(),
            "test-index-fool".to_string(),
            "test-index-foobar".to_string(),
            "test-index-bar".to_string(),
            "test-index-qux".to_string(),
        ],
    };
    let find_index_template_matches_response = metastore
        .find_index_template_matches(find_index_template_matches)
        .await
        .unwrap();
    let mut matches = find_index_template_matches_response.matches;
    matches.sort_unstable_by(|left, right| left.index_id.cmp(&right.index_id));

    assert_eq!(matches.len(), 3);

    assert_eq!(matches[0].index_id, "test-index-bar");
    assert_eq!(matches[0].template_id, bar_template_id);

    assert_eq!(matches[1].index_id, "test-index-foo");
    assert_eq!(matches[1].template_id, foo_template_id);

    assert_eq!(matches[2].index_id, "test-index-foobar");
    assert_eq!(matches[2].template_id, foo_template_id);
}

pub async fn test_metastore_list_index_templates<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;
    cleanup_templates(&mut metastore).await;

    let list_index_templates_request = ListIndexTemplatesRequest {};
    let list_index_templates_response = metastore
        .list_index_templates(list_index_templates_request)
        .await
        .unwrap();
    assert_eq!(list_index_templates_response.index_templates_json.len(), 0);

    let template_id = append_random_suffix("test-list-template");
    let index_template = IndexTemplate::for_test(&template_id, &["test-template"], 100);
    let index_template_json = serde_json::to_string(&index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let list_index_templates_request = ListIndexTemplatesRequest {};
    let list_index_templates_response = metastore
        .list_index_templates(list_index_templates_request)
        .await
        .unwrap();
    assert_eq!(list_index_templates_response.index_templates_json.len(), 1);

    let index_template: IndexTemplate =
        serde_utils::from_json_str(&list_index_templates_response.index_templates_json[0]).unwrap();

    assert_eq!(index_template.template_id, template_id);
    assert_eq!(index_template.index_id_patterns, ["test-template"]);
    assert_eq!(
        index_template.index_root_uri.unwrap().as_str(),
        "ram:///indexes"
    );
    assert_eq!(index_template.priority, 100);
    assert_eq!(index_template.description.unwrap(), "Test description.");
    assert_eq!(index_template.doc_mapping.timestamp_field.unwrap(), "ts");
}

pub async fn test_metastore_delete_index_templates<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;
    cleanup_templates(&mut metastore).await;

    let foo_template_id = append_random_suffix("test-template-foo");
    let foo_index_template = IndexTemplate::for_test(&foo_template_id, &["test-index-foo*"], 100);
    let foo_index_template_json = serde_json::to_string(&foo_index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: foo_index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let bar_template_id = append_random_suffix("test-template-bar");
    let bar_index_template = IndexTemplate::for_test(&bar_template_id, &["test-index-bar*"], 100);
    let bar_index_template_json = serde_json::to_string(&bar_index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: bar_index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let qux_template_id = append_random_suffix("test-template-qux");
    let qux_index_template = IndexTemplate::for_test(&qux_template_id, &["test-index-qux*"], 100);
    let qux_index_template_json = serde_json::to_string(&qux_index_template).unwrap();

    let create_index_template_request = CreateIndexTemplateRequest {
        index_template_json: qux_index_template_json,
        overwrite: false,
    };
    metastore
        .create_index_template(create_index_template_request)
        .await
        .unwrap();

    let delete_index_templates_request = DeleteIndexTemplatesRequest {
        template_ids: vec![foo_template_id.clone(), bar_template_id.clone()],
    };
    metastore
        .delete_index_templates(delete_index_templates_request.clone())
        .await
        .unwrap();

    // Test idempotency.
    metastore
        .delete_index_templates(delete_index_templates_request.clone())
        .await
        .unwrap();

    let index_templates = list_all_index_templates(&mut metastore).await.unwrap();
    assert_eq!(index_templates.len(), 1);
    assert_eq!(index_templates[0].template_id, qux_template_id);
}
