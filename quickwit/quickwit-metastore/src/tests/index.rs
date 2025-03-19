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

// Index API tests
//
//  - create_index
//  - index_exists
//  - index_metadata
//  - list_indexes
//  - delete_index

use quickwit_common::rand::append_random_suffix;
use quickwit_config::merge_policy_config::{MergePolicyConfig, StableLogMergePolicyConfig};
use quickwit_config::{
    IndexConfig, IndexingSettings, RetentionPolicy, SearchSettings, SourceConfig, CLI_SOURCE_ID,
    INGEST_V2_SOURCE_ID,
};
use quickwit_doc_mapper::{Cardinality, FieldMappingEntry, FieldMappingType, QuickwitJsonOptions};
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteIndexRequest, EntityKind, IndexMetadataFailure,
    IndexMetadataFailureReason, IndexMetadataRequest, IndexMetadataSubrequest,
    IndexesMetadataRequest, ListIndexesMetadataRequest, MetastoreError, MetastoreService,
    StageSplitsRequest, UpdateIndexRequest,
};
use quickwit_proto::types::{DocMappingUid, IndexUid};

use super::DefaultForTest;
use crate::tests::cleanup_index;
use crate::{
    CreateIndexRequestExt, IndexMetadataResponseExt, IndexesMetadataResponseExt,
    ListIndexesMetadataResponseExt, MetastoreServiceExt, SplitMetadata, StageSplitsRequestExt,
    UpdateIndexRequestExt,
};

pub async fn test_metastore_create_index<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-create-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid = metastore
        .create_index(create_index_request.clone())
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    let error = metastore
        .create_index(create_index_request)
        .await
        .unwrap_err();
    assert!(matches!(error, MetastoreError::AlreadyExists { .. }));

    cleanup_index(&mut metastore, index_uid).await;
}

async fn setup_metastore_for_update<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() -> (MetastoreToTest, IndexUid, IndexConfig) {
    let metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-update-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid = metastore
        .create_index(create_index_request.clone())
        .await
        .unwrap()
        .index_uid()
        .clone();

    (metastore, index_uid, index_config)
}

pub async fn test_metastore_update_retention_policy<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let (mut metastore, index_uid, index_config) =
        setup_metastore_for_update::<MetastoreToTest>().await;
    let new_retention_policy_opt = Some(RetentionPolicy {
        retention_period: String::from("3 days"),
        evaluation_schedule: String::from("daily"),
        jitter_secs: None,
    });

    // set and unset retention policy multiple times
    for loop_retention_policy_opt in [
        None,
        new_retention_policy_opt.clone(),
        new_retention_policy_opt.clone(),
        None,
    ] {
        let index_update = UpdateIndexRequest::try_from_updates(
            index_uid.clone(),
            &index_config.search_settings,
            &loop_retention_policy_opt,
            &index_config.indexing_settings,
            &index_config.doc_mapping,
        )
        .unwrap();
        let response_metadata = metastore
            .update_index(index_update)
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(response_metadata.index_uid, index_uid);
        assert_eq!(
            response_metadata.index_config.retention_policy_opt,
            loop_retention_policy_opt
        );
        let updated_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(
                index_uid.index_id.to_string(),
            ))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(response_metadata, updated_metadata);
    }
    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_update_search_settings<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let (mut metastore, index_uid, index_config) =
        setup_metastore_for_update::<MetastoreToTest>().await;

    for loop_search_settings in [
        Vec::new(),
        vec!["body".to_string()],
        vec!["body".to_string()],
        vec!["body".to_string(), "owner".to_string()],
        Vec::new(),
    ] {
        let index_update = UpdateIndexRequest::try_from_updates(
            index_uid.clone(),
            &SearchSettings {
                default_search_fields: loop_search_settings.clone(),
            },
            &index_config.retention_policy_opt,
            &index_config.indexing_settings,
            &index_config.doc_mapping,
        )
        .unwrap();
        let response_metadata = metastore
            .update_index(index_update)
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(
            response_metadata
                .index_config
                .search_settings
                .default_search_fields,
            loop_search_settings
        );
        let updated_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(
                index_uid.index_id.to_string(),
            ))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(
            updated_metadata
                .index_config
                .search_settings
                .default_search_fields,
            loop_search_settings
        );
    }
    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_update_indexing_settings<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let (mut metastore, index_uid, index_config) =
        setup_metastore_for_update::<MetastoreToTest>().await;

    for loop_indexing_settings in [
        MergePolicyConfig::Nop,
        MergePolicyConfig::Nop,
        MergePolicyConfig::StableLog(StableLogMergePolicyConfig {
            merge_factor: 5,
            ..Default::default()
        }),
    ] {
        let index_update = UpdateIndexRequest::try_from_updates(
            index_uid.clone(),
            &index_config.search_settings,
            &index_config.retention_policy_opt,
            &IndexingSettings {
                merge_policy: loop_indexing_settings.clone(),
                ..Default::default()
            },
            &index_config.doc_mapping,
        )
        .unwrap();
        let resp_metadata = metastore
            .update_index(index_update)
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(
            resp_metadata.index_config.indexing_settings.merge_policy,
            loop_indexing_settings
        );
        let updated_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(
                index_uid.index_id.to_string(),
            ))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(
            updated_metadata.index_config.indexing_settings.merge_policy,
            loop_indexing_settings
        );
    }
    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_update_doc_mapping<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let (mut metastore, index_uid, index_config) =
        setup_metastore_for_update::<MetastoreToTest>().await;

    let json_options = QuickwitJsonOptions {
        description: None,
        stored: false,
        indexing_options: None,
        expand_dots: false,
        fast: Default::default(),
    };

    let initial = index_config.doc_mapping.clone();
    let mut new_field = initial.clone();
    new_field.field_mappings.push(FieldMappingEntry {
        name: "new_field".to_string(),
        mapping_type: FieldMappingType::Json(json_options.clone(), Cardinality::SingleValued),
    });
    new_field.doc_mapping_uid = DocMappingUid::random();
    let mut new_field_stored = initial.clone();
    new_field_stored.field_mappings.push(FieldMappingEntry {
        name: "new_field".to_string(),
        mapping_type: FieldMappingType::Json(
            QuickwitJsonOptions {
                stored: true,
                ..json_options
            },
            Cardinality::SingleValued,
        ),
    });
    new_field_stored.doc_mapping_uid = DocMappingUid::random();

    for loop_doc_mapping in [initial.clone(), new_field, new_field_stored, initial] {
        let index_update = UpdateIndexRequest::try_from_updates(
            index_uid.clone(),
            &index_config.search_settings,
            &index_config.retention_policy_opt,
            &index_config.indexing_settings,
            &loop_doc_mapping,
        )
        .unwrap();
        let resp_metadata = metastore
            .update_index(index_update)
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(resp_metadata.index_config.doc_mapping, loop_doc_mapping);
        let updated_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(
                index_uid.index_id.to_string(),
            ))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(updated_metadata.index_config.doc_mapping, loop_doc_mapping);
    }
    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_create_index_with_sources<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-create-index-with-sources");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let index_config_json = serde_json::to_string(&index_config).unwrap();

    let source_configs_json = vec![
        serde_json::to_string(&SourceConfig::cli()).unwrap(),
        serde_json::to_string(&SourceConfig::ingest_v2()).unwrap(),
    ];
    let create_index_request = CreateIndexRequest {
        index_config_json,
        source_configs_json,
    };
    let index_uid: IndexUid = metastore
        .create_index(create_index_request.clone())
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    assert_eq!(index_metadata.sources.len(), 2);
    assert!(index_metadata.sources.contains_key(CLI_SOURCE_ID));
    assert!(index_metadata.sources.contains_key(INGEST_V2_SOURCE_ID));

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_create_index_enforces_index_id_maximum_length<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix(format!("very-long-index-{}", "a".repeat(233)).as_str());
    assert_eq!(index_id.len(), 255);
    let index_uri = format!("ram:///indexes/{index_id}");

    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_index_exists<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-index-exists");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();

    assert!(!metastore.index_exists(&index_id).await.unwrap());

    let index_uid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_index_metadata<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-index-metadata");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let error = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_indexes_metadata<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id_0 = append_random_suffix("test-indexes-metadata-0");
    let index_uri_0 = format!("ram:///indexes/{index_id_0}");
    let index_config_0 = IndexConfig::for_test(&index_id_0, &index_uri_0);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_0).unwrap();
    let index_uid_0: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let index_id_1 = append_random_suffix("test-indexes-metadata-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_1).unwrap();
    let index_uid_1: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let indexes_metadata_request = IndexesMetadataRequest {
        subrequests: vec![
            IndexMetadataSubrequest {
                index_id: None,
                index_uid: None,
            },
            IndexMetadataSubrequest {
                index_id: Some(index_id_0.clone()),
                index_uid: None,
            },
            IndexMetadataSubrequest {
                index_id: Some("test-indexes-metadata-foo".to_string()),
                index_uid: None,
            },
            IndexMetadataSubrequest {
                index_id: None,
                index_uid: Some(index_uid_1.clone()),
            },
            IndexMetadataSubrequest {
                index_id: None,
                index_uid: Some(IndexUid::for_test("test-indexes-metadata-bar", 123)),
            },
        ],
    };
    let mut indexes_metadata_response = metastore
        .indexes_metadata(indexes_metadata_request)
        .await
        .unwrap();

    let failures = &mut indexes_metadata_response.failures;
    assert_eq!(failures.len(), 3);

    failures.sort_by(|left, right| left.index_id().cmp(right.index_id()));

    let expected_failure_0 = IndexMetadataFailure {
        index_id: None,
        index_uid: None,
        reason: IndexMetadataFailureReason::Internal as i32,
    };
    assert_eq!(failures[0], expected_failure_0);

    let expected_failure_1 = IndexMetadataFailure {
        index_id: None,
        index_uid: Some(IndexUid::for_test("test-indexes-metadata-bar", 123)),
        reason: IndexMetadataFailureReason::NotFound as i32,
    };
    assert_eq!(failures[1], expected_failure_1);

    let expected_failure_2 = IndexMetadataFailure {
        index_id: Some("test-indexes-metadata-foo".to_string()),
        index_uid: None,
        reason: IndexMetadataFailureReason::NotFound as i32,
    };
    assert_eq!(failures[2], expected_failure_2);

    let mut indexes_metadata = indexes_metadata_response
        .deserialize_indexes_metadata()
        .await
        .unwrap();
    assert_eq!(indexes_metadata.len(), 2);

    indexes_metadata.sort_by(|left, right| left.index_id().cmp(right.index_id()));
    assert_eq!(indexes_metadata[0].index_id(), index_id_0);
    assert_eq!(indexes_metadata[1].index_id(), index_id_1);

    cleanup_index(&mut metastore, index_uid_0).await;
    cleanup_index(&mut metastore, index_uid_1).await;
}

pub async fn test_metastore_list_all_indexes<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id_prefix = append_random_suffix("test-list-all-indexes");
    let index_id_1 = format!("{index_id_prefix}-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = format!("{index_id_prefix}-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .into_iter()
        .filter(|index| index.index_id().starts_with(&index_id_prefix))
        .count();
    assert_eq!(indexes_count, 0);

    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();

    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .into_iter()
        .filter(|index| index.index_id().starts_with(&index_id_prefix))
        .count();
    assert_eq!(indexes_count, 2);

    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
}

pub async fn test_metastore_list_indexes<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id_fragment = append_random_suffix("test-list-indexes");
    let index_id_1 = format!("prefix-1-{index_id_fragment}-suffix-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = format!("prefix-2-{index_id_fragment}-suffix-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);

    let index_id_3 = format!("prefix.3.{index_id_fragment}.3");
    let index_uri_3 = format!("ram:///indexes/{index_id_3}");
    let index_config_3 = IndexConfig::for_test(&index_id_3, &index_uri_3);

    let index_id_4 = format!("p-4-{index_id_fragment}-suffix-4");
    let index_uri_4 = format!("ram:///indexes/{index_id_4}");
    let index_config_4 = IndexConfig::for_test(&index_id_4, &index_uri_4);

    let index_id_patterns = vec![
        format!("prefix-*-{index_id_fragment}-suffix-*"),
        format!("prefix*{index_id_fragment}*suffix-*"),
    ];
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest { index_id_patterns })
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .len();
    assert_eq!(indexes_count, 0);

    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_3 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_3).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_4 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_4).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();

    let index_id_patterns = vec![format!("prefix-*-{index_id_fragment}-suffix-*")];
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest { index_id_patterns })
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .len();
    assert_eq!(indexes_count, 2);

    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
    cleanup_index(&mut metastore, index_uid_3).await;
    cleanup_index(&mut metastore, index_uid_4).await;
}

pub async fn test_metastore_delete_index<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-delete-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let index_uid_not_existing = IndexUid::new_with_random_ulid("index-not-found");
    let error = metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid_not_existing.clone()),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let error = metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid_not_existing),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid.clone().into(),
        })
        .await
        .unwrap();

    assert!(!metastore.index_exists(&index_id).await.unwrap());

    let split_id = format!("{index_id}--split");
    let split_metadata = SplitMetadata {
        split_id: split_id.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .unwrap();

    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    // TODO: We should not be able to delete an index that has remaining splits, at least not as
    // a default behavior. Let's implement the logic that allows this test to pass.
    // let error = metastore.delete_index(index_uid).await.unwrap_err();
    // assert!(matches!(error, MetastoreError::IndexNotEmpty { .. }));
    // let splits = metastore.list_all_splits(index_uid.clone()).await.unwrap();
    // assert_eq!(splits.len(), 1)

    cleanup_index(&mut metastore, index_uid).await;
}
