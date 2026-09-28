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

use quickwit_common::split_file;
use quickwit_metastore::{
    ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SortFieldMetadata, SortValueType,
};
use quickwit_proto::metastore::{DeleteQuery, ListSplitsRequest};
use quickwit_proto::search::SortOrder;
use serde_json::json;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::{PhraseQuery, TermQuery};
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::{DocAddress, TantivyDocument, Term};

use super::*;
use crate::merge_policy::MergeOperation;
use crate::{TestSandbox, get_tantivy_directory_from_split_bundle};

const MAPPING: &str = r#"
field_mappings:
  - name: service
    type: text
    tokenizer: raw
    fast: true
  - name: id
    type: u64
    fast: true
  - name: body
    type: text
    record: position
tag_fields: [service]
"#;

fn sort_fields(order: SortOrder) -> Vec<SortFieldMetadata> {
    vec![SortFieldMetadata {
        field: "service".to_string(),
        order,
        field_type: SortValueType::Text,
    }]
}

#[test]
fn test_sort_fields_reject_incompatible_schemas() -> anyhow::Result<()> {
    let mut text_schema = tantivy::schema::Schema::builder();
    text_schema.add_text_field("service", tantivy::schema::FAST);
    let mut int_schema = tantivy::schema::Schema::builder();
    int_schema.add_i64_field("service", tantivy::schema::FAST);
    let settings = tantivy::IndexSettings {
        sort_by_field: Some(tantivy::IndexSortByField {
            field: "service".to_string(),
            order: tantivy::Order::Asc,
        }),
        ..Default::default()
    };
    let mut index_metas = Vec::new();
    for schema in [text_schema.build(), int_schema.build()] {
        let index = Index::builder()
            .schema(schema)
            .settings(settings.clone())
            .create_in_ram()?;
        index_metas.push(index.load_metas()?);
    }
    let error = combine_index_meta(index_metas).unwrap_err();
    assert!(error.to_string().contains("different physical sort fields"));
    Ok(())
}

/// Check doc-ID order and verify stored fields, fast fields, postings and positions
/// still refer to the same documents after the permutation.
fn check_index(index: &Index, order: SortOrder) -> anyhow::Result<Vec<u64>> {
    let reader = index.reader()?;
    let searcher = reader.searcher();
    assert_eq!(searcher.segment_readers().len(), 1);
    let segment = searcher.segment_reader(0);
    let service_field = index.schema().get_field("service")?;
    let id_field = index.schema().get_field("id")?;
    let body_field = index.schema().get_field("body")?;
    let service_column = segment.fast_fields().str("service")?.unwrap();
    let id_column = segment.fast_fields().u64("id")?;
    let mut keys = Vec::new();
    let mut ids = Vec::new();
    for doc_id in 0..segment.max_doc() {
        let document: TantivyDocument = searcher.doc(DocAddress::new(0, doc_id))?;
        let service = document
            .get_first(service_field)
            .and_then(|value| value.as_str());
        let id = document.get_first(id_field).unwrap().as_u64().unwrap();
        assert_eq!(id_column.first(doc_id), Some(id));
        let fast_service = match service_column.term_ords(doc_id).next() {
            Some(ordinal) => {
                let mut value = String::new();
                service_column.ord_to_str(ordinal, &mut value)?;
                Some(value)
            }
            None => None,
        };
        assert_eq!(fast_service.as_deref(), service);
        let expected_service = match id {
            0 => Some("zulu"),
            2 | 5 => Some("api"),
            3 => Some(""),
            4 => Some("équipe"),
            1 | 6 | 7 => None,
            _ => panic!("unexpected id {id}"),
        };
        assert_eq!(service, expected_service);
        let phrase = PhraseQuery::new(vec![
            Term::from_field_text(body_field, "payload"),
            Term::from_field_text(body_field, &format!("item{id}")),
        ]);
        let matches = searcher.search(&phrase, &DocSetCollector)?;
        assert_eq!(matches.len(), 1);
        assert!(matches.contains(&DocAddress::new(0, doc_id)));
        keys.push(fast_service);
        ids.push(id);
    }
    assert!(keys.windows(2).all(|pair| match order {
        SortOrder::Asc => pair[0] <= pair[1],
        SortOrder::Desc => pair[0] >= pair[1],
    }));
    let query = TermQuery::new(
        Term::from_field_text(service_field, "api"),
        IndexRecordOption::Basic,
    );
    assert_eq!(
        searcher.search(&query, &Count)?,
        ids.iter().filter(|&&id| id == 2 || id == 5).count()
    );
    Ok(ids)
}

async fn execute(
    sandbox: &TestSandbox,
    splits: Vec<SplitMetadata>,
    delete: bool,
) -> anyhow::Result<IndexedSplit> {
    let scratch = TempDirectory::for_test();
    let downloads = scratch.named_temp_child("downloads-")?;
    let mut directories = Vec::new();
    for split in &splits {
        let filename = split_file(split.split_id());
        let path = downloads.path().join(&filename);
        sandbox
            .storage()
            .copy_to_file(Path::new(&filename), &path)
            .await?;
        directories.push(get_tantivy_directory_from_split_bundle(&path)?);
    }
    for (directory, split) in directories.iter().zip(&splits) {
        let index = open_index(
            directory.clone(),
            sandbox.doc_mapper().tokenizer_manager().tantivy_manager(),
        )?;
        check_index(&index, split.sort_fields[0].order)?;
        assert_eq!(split.num_merge_ops, 0);
    }
    // A stale or missing declaration must not silently change merge semantics.
    let mut wrong_type = splits[0].sort_fields.clone();
    wrong_type[0].field_type = SortValueType::I64;
    for invalid_sort_fields in [Vec::new(), wrong_type] {
        let mut stale_metadata = splits.clone();
        stale_metadata[0].sort_fields = invalid_sort_fields;
        let error = open_split_directories(
            &directories,
            &stale_metadata,
            sandbox.doc_mapper().tokenizer_manager().tantivy_manager(),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("disagrees with metastore"));
    }

    let operation = if delete {
        MergeOperation::new_delete_and_merge_operation(splits[0].clone())
    } else {
        MergeOperation::new_merge_operation(splits)
    };
    let (mailbox, inbox) = sandbox.universe().create_test_mailbox();
    let executor = MergeExecutor::new(
        MergePipelineId {
            node_id: sandbox.node_id(),
            index_uid: sandbox.index_uid(),
            source_id: sandbox.source_id(),
        },
        sandbox.metastore(),
        sandbox.doc_mapper(),
        IoControls::default(),
        mailbox,
        None,
    );
    let (executor_mailbox, handle) = sandbox.universe().spawn_builder().spawn(executor);
    executor_mailbox
        .send_message(MergeScratch {
            merge_source: MergeSource::Operation(operation),
            tantivy_dirs: directories,
            merge_scratch_directory: scratch,
            downloaded_splits_directory: downloads,
        })
        .await?;
    handle.process_pending_and_observe().await;
    let mut batches = inbox.drain_for_test_typed::<IndexedSplitBatch>();
    assert_eq!(batches.len(), 1);
    Ok(batches.pop().unwrap().splits.pop().unwrap())
}

#[tokio::test]
async fn test_sort_fields_generation_zero_and_merge() -> anyhow::Result<()> {
    for order in [SortOrder::Asc, SortOrder::Desc] {
        let sort_field = match order {
            SortOrder::Asc => "service",
            SortOrder::Desc => "-service",
        };
        let settings = serde_json::to_string(&json!({"sort_fields": [sort_field]}))?;
        let sandbox = TestSandbox::create("service-sort", MAPPING, &settings, &["body"]).await?;
        for batch in [
            vec![
                (0, Some("zulu")),
                (1, None),
                (2, Some("api")),
                (3, Some("")),
            ],
            vec![(4, Some("équipe")), (5, Some("api")), (6, None)],
            vec![(7, None)],
        ] {
            sandbox
                .add_documents(batch.into_iter().map(|(id, service)| {
                    json!({
                        "id": id, "service": service, "body": format!("payload item{id}")
                    })
                }))
                .await?;
        }
        let splits = sandbox
            .metastore()
            .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid())?)
            .await?
            .collect_splits_metadata()
            .await?;
        assert_eq!(splits.len(), 3);
        assert!(
            splits
                .iter()
                .all(|split| split.sort_fields == sort_fields(order))
        );
        let merged = execute(&sandbox, splits, false).await?;
        assert_eq!(merged.split_attrs.sort_fields, sort_fields(order));
        assert_eq!(merged.split_attrs.num_docs, 8);
        let mut ids = check_index(&merged.index, order)?;
        ids.sort_unstable();
        assert_eq!(ids, (0..8).collect::<Vec<_>>());
        drop(merged);
        sandbox.assert_quit().await;
    }
    Ok(())
}

#[tokio::test]
async fn test_sort_fields_preserved_after_delete() -> anyhow::Result<()> {
    let sandbox = TestSandbox::create(
        "service-sort-delete",
        MAPPING,
        "sort_fields: [service]",
        &["body"],
    )
    .await?;
    sandbox
        .add_documents(vec![
            json!({"id": 0, "service": "zulu", "body": "payload item0"}),
            json!({"id": 1, "body": "payload item1"}),
            json!({"id": 2, "service": "api", "body": "payload item2"}),
        ])
        .await?;
    let splits = sandbox
        .metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid())?)
        .await?
        .collect_splits_metadata()
        .await?;
    sandbox
        .metastore()
        .create_delete_task(DeleteQuery {
            index_uid: Some(sandbox.index_uid()),
            start_timestamp: None,
            end_timestamp: None,
            query_ast: quickwit_query::query_ast::qast_json_helper("service:api", &["body"]),
        })
        .await?;
    let rewritten = execute(&sandbox, splits, true).await?;
    assert_eq!(
        rewritten.split_attrs.sort_fields,
        sort_fields(SortOrder::Asc)
    );
    assert_eq!(rewritten.split_attrs.num_docs, 2);
    assert_eq!(rewritten.split_attrs.delete_opstamp, 1);
    assert_eq!(check_index(&rewritten.index, SortOrder::Asc)?, vec![1, 0]);
    drop(rewritten);
    sandbox.assert_quit().await;
    Ok(())
}

#[test]
fn test_sort_fields_reject_incompatible_merge_inputs() {
    let mut first = SplitMetadata {
        sort_fields: sort_fields(SortOrder::Asc),
        ..Default::default()
    };
    let mut second = first.clone();
    second.sort_fields.clear();
    let pipeline_id = MergePipelineId {
        node_id: NodeId::from_str("test"),
        index_uid: first.index_uid.clone(),
        source_id: "test".to_string(),
    };
    assert!(
        merge_split_attrs(
            pipeline_id.clone(),
            SplitId::new(),
            &[first.clone(), second.clone()]
        )
        .is_err()
    );
    second.sort_fields = sort_fields(SortOrder::Desc);
    assert!(
        merge_split_attrs(
            pipeline_id.clone(),
            SplitId::new(),
            &[first.clone(), second.clone()]
        )
        .is_err()
    );
    second.sort_fields = first.sort_fields.clone();
    second.sort_fields[0].field_type = SortValueType::I64;
    assert!(
        merge_split_attrs(
            pipeline_id.clone(),
            SplitId::new(),
            &[first.clone(), second.clone()]
        )
        .is_err()
    );
    first.sort_fields = second.sort_fields.clone();
    assert!(merge_split_attrs(pipeline_id, SplitId::new(), &[first, second]).is_ok());
}
