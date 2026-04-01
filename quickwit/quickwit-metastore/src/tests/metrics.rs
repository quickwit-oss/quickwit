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
use quickwit_config::IndexConfig;
use quickwit_parquet_engine::split::{
    MetricsSplitMetadata, MetricsSplitState, SplitId, TimeRange, TAG_ENV, TAG_HOST, TAG_SERVICE,
};
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteMetricsSplitsRequest, EntityKind, ListMetricsSplitsRequest,
    MarkMetricsSplitsForDeletionRequest, MetastoreError, PublishMetricsSplitsRequest,
    StageMetricsSplitsRequest,
};
use quickwit_proto::types::IndexUid;

use super::DefaultForTest;
use crate::tests::cleanup_index;
use crate::{
    CreateIndexRequestExt, ListMetricsSplitsQuery, ListMetricsSplitsRequestExt,
    ListMetricsSplitsResponseExt, MetastoreServiceExt, StageMetricsSplitsRequestExt,
};

/// Helper to create a test index and return the actual IndexUid assigned by the metastore.
async fn create_test_index(
    metastore: &mut dyn MetastoreServiceExt,
    index_id: &str,
) -> IndexUid {
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(index_id, &index_uri);
    let create_index_request =
        CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone()
}

/// Build a simple MetricsSplitMetadata for tests.
fn build_test_split(
    split_id: &str,
    index_uid: &IndexUid,
    time_range: TimeRange,
) -> MetricsSplitMetadata {
    MetricsSplitMetadata::builder()
        .split_id(SplitId::new(split_id))
        .index_uid(index_uid.to_string())
        .time_range(time_range)
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("cpu.usage")
        .parquet_file(format!("{split_id}.parquet"))
        .build()
}

pub async fn test_metastore_stage_metrics_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-stage-metrics-splits");

    // Stage on a non-existent index should fail.
    {
        let fake_uid = IndexUid::new_with_random_ulid("index-not-found");
        let split = build_test_split("split-1", &fake_uid, TimeRange::new(1000, 2000));
        let request =
            StageMetricsSplitsRequest::try_from_splits_metadata(fake_uid, &[split]).unwrap();
        let error = metastore.stage_metrics_splits(request).await.unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Index { .. })
        ));
    }

    let index_uid = create_test_index(&mut metastore, &index_id).await;

    // Stage two splits.
    let split_id_1 = format!("{index_id}--split-1");
    let split_id_2 = format!("{index_id}--split-2");
    let split_1 = build_test_split(&split_id_1, &index_uid, TimeRange::new(1000, 2000));
    let split_2 = build_test_split(&split_id_2, &index_uid, TimeRange::new(2000, 3000));

    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1, split_2],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Verify both splits are listed in Staged state.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Staged".to_string()]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 2);

    for split in &splits {
        assert_eq!(split.state, MetricsSplitState::Staged);
    }

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_stage_metrics_splits_upsert<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-stage-metrics-upsert");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    let split_id = format!("{index_id}--split-1");

    // Stage a split with 100 rows.
    let split_v1 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(&split_id))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("cpu.usage")
        .parquet_file(format!("{split_id}.parquet"))
        .build();

    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_v1],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Stage the same split_id again with 200 rows (upsert).
    let split_v2 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(&split_id))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(200)
        .size_bytes(8192)
        .add_metric_name("cpu.usage")
        .parquet_file(format!("{split_id}.parquet"))
        .build();

    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_v2],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Verify only one split exists and it has the updated num_rows.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Staged".to_string()]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].metadata.num_rows, 200);
    assert_eq!(splits[0].metadata.size_bytes, 8192);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_list_metrics_splits_by_state<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-list-metrics-by-state");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    // Stage two splits.
    let split_id_1 = format!("{index_id}--split-1");
    let split_id_2 = format!("{index_id}--split-2");
    let split_1 = build_test_split(&split_id_1, &index_uid, TimeRange::new(1000, 2000));
    let split_2 = build_test_split(&split_id_2, &index_uid, TimeRange::new(2000, 3000));
    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1, split_2],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Publish split_1 only.
    let publish_request = PublishMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_1.clone()],
        ..Default::default()
    };
    metastore
        .publish_metrics_splits(publish_request)
        .await
        .unwrap();

    // List only Published splits.
    {
        let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
            .with_split_states(vec!["Published".to_string()]);
        let list_request =
            ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
        let response = metastore.list_metrics_splits(list_request).await.unwrap();
        let splits = response.deserialize_splits().unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].metadata.split_id.as_str(), &split_id_1);
        assert_eq!(splits[0].state, MetricsSplitState::Published);
    }

    // List only Staged splits.
    {
        let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
            .with_split_states(vec!["Staged".to_string()]);
        let list_request =
            ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
        let response = metastore.list_metrics_splits(list_request).await.unwrap();
        let splits = response.deserialize_splits().unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].metadata.split_id.as_str(), &split_id_2);
        assert_eq!(splits[0].state, MetricsSplitState::Staged);
    }

    // List both states.
    {
        let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
            .with_split_states(vec!["Published".to_string(), "Staged".to_string()]);
        let list_request =
            ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
        let response = metastore.list_metrics_splits(list_request).await.unwrap();
        let splits = response.deserialize_splits().unwrap();
        assert_eq!(splits.len(), 2);
    }

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_list_metrics_splits_by_time_range<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-list-metrics-time-range");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    // Stage splits at different time ranges.
    let split_1 = build_test_split(
        &format!("{index_id}--split-1"),
        &index_uid,
        TimeRange::new(1000, 2000),
    );
    let split_2 = build_test_split(
        &format!("{index_id}--split-2"),
        &index_uid,
        TimeRange::new(3000, 4000),
    );
    let split_3 = build_test_split(
        &format!("{index_id}--split-3"),
        &index_uid,
        TimeRange::new(5000, 6000),
    );
    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1, split_2, split_3],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Query for time range that overlaps only the first two splits.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Staged".to_string()])
        .with_time_range(1500, 3500);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    // Should match splits whose time range overlaps [1500, 3500].
    // split_1: [1000,2000) overlaps [1500,3500] => yes
    // split_2: [3000,4000) overlaps [1500,3500] => yes
    // split_3: [5000,6000) overlaps [1500,3500] => no
    assert_eq!(splits.len(), 2);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_list_metrics_splits_by_metric_name<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-list-metrics-by-name");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    // Stage splits with different metric names.
    let split_1 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(format!("{index_id}--split-1")))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("cpu.usage")
        .parquet_file("split-1.parquet")
        .build();

    let split_2 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(format!("{index_id}--split-2")))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("memory.used")
        .parquet_file("split-2.parquet")
        .build();

    let split_3 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(format!("{index_id}--split-3")))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("cpu.usage")
        .add_metric_name("memory.used")
        .parquet_file("split-3.parquet")
        .build();

    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1, split_2, split_3],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Query for "cpu.usage" should return split_1 and split_3.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Staged".to_string()])
        .with_metric_names(vec!["cpu.usage".to_string()]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 2);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_list_metrics_splits_by_compaction_scope<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-list-metrics-compaction");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    // Stage splits with different compaction scopes.
    let split_1 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(format!("{index_id}--split-1")))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("cpu.usage")
        .parquet_file("split-1.parquet")
        .window_start_secs(1700000000)
        .window_duration_secs(3600)
        .sort_fields("metric_name|host|timestamp/V2")
        .build();

    let split_2 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(format!("{index_id}--split-2")))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("cpu.usage")
        .parquet_file("split-2.parquet")
        .window_start_secs(1700003600)
        .window_duration_secs(3600)
        .sort_fields("metric_name|host|timestamp/V2")
        .build();

    let split_3 = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(format!("{index_id}--split-3")))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 2000))
        .num_rows(100)
        .size_bytes(4096)
        .add_metric_name("cpu.usage")
        .parquet_file("split-3.parquet")
        .window_start_secs(1700000000)
        .window_duration_secs(3600)
        .sort_fields("different_schema/V1")
        .build();

    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1, split_2, split_3],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Query by compaction scope: window_start=1700000000, sort_fields matching split_1.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Staged".to_string()])
        .with_compaction_scope(1700000000, "metric_name|host|timestamp/V2");
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    // Only split_1 matches both window_start and sort_fields.
    assert_eq!(splits.len(), 1);
    assert_eq!(
        splits[0].metadata.split_id.as_str(),
        format!("{index_id}--split-1")
    );

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_publish_metrics_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-publish-metrics-splits");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    let split_id_1 = format!("{index_id}--split-1");
    let split_id_2 = format!("{index_id}--split-2");
    let split_1 = build_test_split(&split_id_1, &index_uid, TimeRange::new(1000, 2000));
    let split_2 = build_test_split(&split_id_2, &index_uid, TimeRange::new(2000, 3000));

    // Stage both.
    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1, split_2],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    // Publish both.
    let publish_request = PublishMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
        ..Default::default()
    };
    metastore
        .publish_metrics_splits(publish_request)
        .await
        .unwrap();

    // Verify they are now Published.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Published".to_string()]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 2);

    for split in &splits {
        assert_eq!(split.state, MetricsSplitState::Published);
    }

    // Verify no Staged splits remain.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Staged".to_string()]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 0);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_publish_metrics_splits_nonexistent<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-publish-metrics-nonexistent");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    // Publish a split_id that was never staged.
    let publish_request = PublishMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec!["nonexistent-split".to_string()],
        ..Default::default()
    };
    let error = metastore
        .publish_metrics_splits(publish_request)
        .await
        .unwrap_err();
    // File-backed: NotFound. Postgres: FailedPrecondition (count mismatch).
    assert!(
        matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
                | MetastoreError::FailedPrecondition { .. }
        ),
        "expected NotFound(Splits), got: {error:?}"
    );

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_mark_metrics_splits_for_deletion<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-mark-metrics-deletion");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    let split_id_1 = format!("{index_id}--split-1");
    let split_id_2 = format!("{index_id}--split-2");
    let split_1 = build_test_split(&split_id_1, &index_uid, TimeRange::new(1000, 2000));
    let split_2 = build_test_split(&split_id_2, &index_uid, TimeRange::new(2000, 3000));

    // Stage and publish.
    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1, split_2],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    let publish_request = PublishMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
        ..Default::default()
    };
    metastore
        .publish_metrics_splits(publish_request)
        .await
        .unwrap();

    // Mark split_1 for deletion.
    let mark_request = MarkMetricsSplitsForDeletionRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: vec![split_id_1.clone()],
    };
    metastore
        .mark_metrics_splits_for_deletion(mark_request)
        .await
        .unwrap();

    // Verify split_1 is MarkedForDeletion.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["MarkedForDeletion".to_string()]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].metadata.split_id.as_str(), &split_id_1);

    // Verify split_2 is still Published.
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Published".to_string()]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].metadata.split_id.as_str(), &split_id_2);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_delete_metrics_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-delete-metrics-splits");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    let split_id_1 = format!("{index_id}--split-1");
    let split_1 = build_test_split(&split_id_1, &index_uid, TimeRange::new(1000, 2000));

    // Stage, publish, mark for deletion.
    let request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split_1],
    )
    .unwrap();
    metastore.stage_metrics_splits(request).await.unwrap();

    let publish_request = PublishMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_1.clone()],
        ..Default::default()
    };
    metastore
        .publish_metrics_splits(publish_request)
        .await
        .unwrap();

    let mark_request = MarkMetricsSplitsForDeletionRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: vec![split_id_1.clone()],
    };
    metastore
        .mark_metrics_splits_for_deletion(mark_request)
        .await
        .unwrap();

    // Delete.
    let delete_request = DeleteMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: vec![split_id_1.clone()],
    };
    metastore
        .delete_metrics_splits(delete_request)
        .await
        .unwrap();

    // Verify it is gone (list all states).
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone()).with_split_states(vec![
        "Staged".to_string(),
        "Published".to_string(),
        "MarkedForDeletion".to_string(),
    ]);
    let list_request =
        ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let response = metastore.list_metrics_splits(list_request).await.unwrap();
    let splits = response.deserialize_splits().unwrap();
    assert_eq!(splits.len(), 0);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_delete_metrics_splits_nonexistent<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("test-delete-metrics-nonexistent");
    let index_uid = create_test_index(&mut metastore, &index_id).await;

    // Delete a split_id that doesn't exist.
    // File-backed: succeeds silently (idempotent).
    // Postgres: may return FailedPrecondition ("not marked for deletion").
    // Both behaviors are acceptable.
    let delete_request = DeleteMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: vec!["nonexistent-split".to_string()],
    };
    let result = metastore.delete_metrics_splits(delete_request).await;
    match &result {
        Ok(_) => {} // file-backed: idempotent success
        Err(MetastoreError::FailedPrecondition { .. }) => {} // postgres: not marked
        Err(other) => panic!("unexpected error: {other:?}"),
    }

    cleanup_index(&mut metastore, index_uid).await;
}
