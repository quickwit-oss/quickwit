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

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_directories::write_hotcache;
use quickwit_doc_mapper::NamedField;
use quickwit_doc_mapper::tag_pruning::append_to_tag_set;
use quickwit_proto::search::{
    ListFieldType, ListFields, ListFieldsEntryResponse, serialize_split_fields,
};
use tantivy::index::FieldMetadata;
use tantivy::schema::{FieldType, Type};
use tantivy::{InvertedIndexReader, ReloadPolicy, SegmentMeta};
use tokio::runtime::Handle;
use tracing::{debug, info, instrument, warn};

/// Maximum distinct values allowed for a tag field within a split.
const MAX_VALUES_PER_TAG_FIELD: usize = if cfg!(any(test, feature = "testsuite")) {
    6
} else {
    1000
};

use crate::actors::Uploader;
use crate::models::{
    EmptySplit, IndexedSplit, IndexedSplitBatch, PackagedSplit, PackagedSplitBatch,
};

/// The role of the packager is to get an index writer and
/// produce a split file.
///
/// This includes the following steps:
/// - commit: this step is CPU heavy
/// - identifying the list of tags for the splits, and labelling it accordingly
/// - creating a bundle file
/// - computing the hotcache
/// - appending it to the split file.
///
/// The split format is described in `internals/split-format.md`
#[derive(Clone)]
pub struct Packager {
    actor_name: &'static str,
    uploader_mailbox: Mailbox<Uploader>,
    /// List of tag fields ([`Vec<NamedField>`]) defined in the index config.
    tag_fields: Vec<NamedField>,
}

impl Packager {
    pub fn new(
        actor_name: &'static str,
        tag_fields: Vec<NamedField>,
        uploader_mailbox: Mailbox<Uploader>,
    ) -> Packager {
        Packager {
            actor_name,
            uploader_mailbox,
            tag_fields,
        }
    }

    pub async fn process_indexed_split(
        &self,
        split: IndexedSplit,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<PackagedSplit> {
        let segment_metas = split.index.searchable_segment_metas()?;
        assert_eq!(segment_metas.len(), 1);
        let packaged_split =
            create_packaged_split(&segment_metas[..], split, &self.tag_fields, ctx)?;
        Ok(packaged_split)
    }
}

#[async_trait]
impl Actor for Packager {
    type ObservableState = ();

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        ()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(1)
    }

    fn name(&self) -> String {
        self.actor_name.to_string()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }
}

#[async_trait]
impl Handler<IndexedSplitBatch> for Packager {
    type Reply = ();

    #[instrument(level = "info", name = "packager", parent=batch.batch_parent_span.id(), skip_all)]
    async fn handle(
        &mut self,
        batch: IndexedSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let split_ids: Vec<String> = batch
            .splits
            .iter()
            .map(|split| split.split_id().to_string())
            .collect_vec();
        debug!(
            split_ids=?split_ids,
            "start-packaging-splits"
        );
        fail_point!("packager:before");
        let mut packaged_splits = Vec::with_capacity(batch.splits.len());
        for split in batch.splits {
            if batch.publish_lock.is_dead() {
                // TODO: Remove the junk right away?
                info!(
                    split_ids=?split_ids,
                    "Splits' publish lock is dead."
                );
                return Ok(());
            }
            let packaged_split = self.process_indexed_split(split, ctx).await?;
            packaged_splits.push(packaged_split);
        }
        ctx.send_message(
            &self.uploader_mailbox,
            PackagedSplitBatch::new(
                packaged_splits,
                batch.checkpoint_delta_opt,
                batch.publish_lock,
                batch.publish_token_opt,
                batch.merge_task_opt,
                batch.batch_parent_span,
            ),
        )
        .await?;
        fail_point!("packager:after");
        Ok(())
    }
}

#[async_trait]
impl Handler<EmptySplit> for Packager {
    type Reply = ();

    #[instrument(
        name="package_empty_batch"
        parent=empty_split.batch_parent_span.id(),
        skip_all,
    )]
    async fn handle(
        &mut self,
        empty_split: EmptySplit,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        ctx.send_message(&self.uploader_mailbox, empty_split)
            .await?;
        Ok(())
    }
}

fn list_split_files(
    segment_metas: &[SegmentMeta],
    scratch_directory: &TempDirectory,
) -> io::Result<Vec<PathBuf>> {
    let mut index_files = vec![scratch_directory.path().join("meta.json")];

    // list the segment files
    for segment_meta in segment_metas {
        for relative_path in segment_meta.list_files() {
            let filepath = scratch_directory.path().join(relative_path);
            if filepath.try_exists()? {
                // If the file is missing, this is fine.
                // segment_meta.list_files() may actually returns files that
                // may not exist.
                index_files.push(filepath);
            }
        }
    }
    index_files.sort();
    Ok(index_files)
}

fn build_hotcache<W: io::Write>(split_path: &Path, out: &mut W) -> anyhow::Result<()> {
    let mmap_directory = tantivy::directory::MmapDirectory::open(split_path)?;
    write_hotcache(mmap_directory, out)?;
    Ok(())
}

/// Attempts to exhaustively extract the list of terms in a
/// field term dictionary.
///
/// returns None if:
/// - the number of terms exceed MAX_VALUES_PER_TAG_FIELD
/// - some of the terms are not value utf8.
/// - an error occurs.
///
/// Returns None may hurt split pruning and affects performance,
/// but it does not affect Quickwit's result validity.
fn try_extract_terms(
    named_field: &NamedField,
    inv_indexes: &[Arc<InvertedIndexReader>],
    max_terms: usize,
) -> anyhow::Result<Vec<String>> {
    let num_terms = inv_indexes
        .iter()
        .map(|inv_index| inv_index.terms().num_terms())
        .sum::<usize>();
    if num_terms > max_terms {
        bail!(
            "number of unique terms for tag field {} > {}",
            named_field.name,
            max_terms
        );
    }
    let mut terms = Vec::with_capacity(num_terms);
    for inv_index in inv_indexes {
        let mut terms_streamer = inv_index.terms().stream()?;
        while let Some((term_data, _)) = terms_streamer.next() {
            let term = match named_field.field_type {
                FieldType::U64(_) => u64_from_term_data(term_data)?.to_string(),
                FieldType::I64(_) => {
                    tantivy::u64_to_i64(u64_from_term_data(term_data)?).to_string()
                }
                FieldType::F64(_) => {
                    tantivy::u64_to_f64(u64_from_term_data(term_data)?).to_string()
                }
                FieldType::Bool(_) => match u64_from_term_data(term_data)? {
                    0 => false,
                    1 => true,
                    _ => bail!("invalid boolean value"),
                }
                .to_string(),
                FieldType::Bytes(_) => {
                    bail!("tags collection is not allowed on `bytes` fields")
                }
                _ => std::str::from_utf8(term_data)?.to_string(),
            };
            terms.push(term);
        }
    }
    Ok(terms)
}

fn create_packaged_split(
    segment_metas: &[SegmentMeta],
    split: IndexedSplit,
    tag_fields: &[NamedField],
    ctx: &ActorContext<Packager>,
) -> anyhow::Result<PackagedSplit> {
    debug!(split_id = split.split_id(), "create-packaged-split");
    let split_files = list_split_files(segment_metas, &split.split_scratch_directory)?;

    // Extracts tag values from inverted indexes only when a field cardinality is less
    // than `MAX_VALUES_PER_TAG_FIELD`.
    debug!(split_id = split.split_id(), tag_fields =? tag_fields, "extract-tags-values");
    let index_reader = split
        .index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;

    let fields_metadata = split.index.fields_metadata()?;

    let mut tags = BTreeSet::default();
    for named_field in tag_fields {
        let inverted_indexes = index_reader
            .searcher()
            .segment_readers()
            .iter()
            .map(|segment| segment.inverted_index(named_field.field))
            .collect::<Result<Vec<_>, _>>()?;

        match try_extract_terms(named_field, &inverted_indexes, MAX_VALUES_PER_TAG_FIELD) {
            Ok(terms) => {
                append_to_tag_set(&named_field.name, &terms, &mut tags);
            }
            Err(tag_extraction_error) => {
                warn!(err=?tag_extraction_error,  "no field values will be registered in the split metadata");
            }
        }
    }

    ctx.record_progress();

    debug!(split_id = split.split_id(), "build-hotcache");
    let mut hotcache_bytes = Vec::new();
    build_hotcache(split.split_scratch_directory.path(), &mut hotcache_bytes)?;
    ctx.record_progress();

    let serialized_split_fields = serialize_field_metadata(&fields_metadata);

    let packaged_split = PackagedSplit {
        serialized_split_fields,
        split_attrs: split.split_attrs,
        split_scratch_directory: split.split_scratch_directory,
        tags,
        split_files,
        hotcache_bytes,
    };
    Ok(packaged_split)
}

/// Serializes the Split fields.
///
/// `fields_metadata` has to be sorted.
fn serialize_field_metadata(fields_metadata: &[FieldMetadata]) -> Vec<u8> {
    let fields = fields_metadata
        .iter()
        .map(field_metadata_to_list_field_serialized)
        .collect::<Vec<_>>();

    serialize_split_fields(ListFields { fields })
}

fn tantivy_type_to_list_field_type(typ: Type) -> ListFieldType {
    match typ {
        Type::Str => ListFieldType::Str,
        Type::U64 => ListFieldType::U64,
        Type::I64 => ListFieldType::I64,
        Type::F64 => ListFieldType::F64,
        Type::Bool => ListFieldType::Bool,
        Type::Date => ListFieldType::Date,
        Type::Facet => ListFieldType::Facet,
        Type::Bytes => ListFieldType::Bytes,
        Type::Json => ListFieldType::Json,
        Type::IpAddr => ListFieldType::IpAddr,
    }
}

fn field_metadata_to_list_field_serialized(
    field_metadata: &FieldMetadata,
) -> ListFieldsEntryResponse {
    ListFieldsEntryResponse {
        field_name: field_metadata.field_name.to_string(),
        field_type: tantivy_type_to_list_field_type(field_metadata.typ) as i32,
        searchable: field_metadata.is_indexed(),
        aggregatable: field_metadata.is_fast(),
        index_ids: Vec::new(),
        non_searchable_index_ids: Vec::new(),
        non_aggregatable_index_ids: Vec::new(),
    }
}

/// Reads u64 from stored term data.
fn u64_from_term_data(data: &[u8]) -> anyhow::Result<u64> {
    let u64_bytes: [u8; 8] = data[0..8]
        .try_into()
        .context("could not interpret term bytes as u64")?;
    Ok(u64::from_be_bytes(u64_bytes))
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use quickwit_actors::{ObservationType, Universe};
    use quickwit_metastore::checkpoint::IndexCheckpointDelta;
    use quickwit_proto::search::{ListFieldsEntryResponse, deserialize_split_fields};
    use quickwit_proto::types::{DocMappingUid, IndexUid, NodeId};
    use tantivy::directory::MmapDirectory;
    use tantivy::schema::{FAST, NumericOptions, STRING, Schema, TEXT, Type};
    use tantivy::{DateTime, IndexBuilder, IndexSettings, doc};
    use tracing::Span;

    use super::*;
    use crate::models::{PublishLock, SplitAttrs};

    #[test]
    fn serialize_field_metadata_test() {
        let fields_metadata = vec![
            FieldMetadata {
                field_name: "test".to_string(),
                typ: Type::Str,
                stored: true,
                fast_size: Some(10u64.into()),
                term_dictionary_size: Some(10u64.into()),
                postings_size: Some(10u64.into()),
                positions_size: Some(10u64.into()),
            },
            FieldMetadata {
                field_name: "test2".to_string(),
                typ: Type::Str,
                stored: false,
                fast_size: None,
                term_dictionary_size: Some(10u64.into()),
                postings_size: Some(10u64.into()),
                positions_size: Some(10u64.into()),
            },
            FieldMetadata {
                field_name: "test3".to_string(),
                typ: Type::U64,
                stored: false,
                fast_size: Some(10u64.into()),
                term_dictionary_size: Some(10u64.into()),
                postings_size: Some(10u64.into()),
                positions_size: Some(10u64.into()),
            },
        ];

        let out = serialize_field_metadata(&fields_metadata);

        let deserialized: Vec<ListFieldsEntryResponse> =
            deserialize_split_fields(&mut &out[..]).unwrap().fields;

        assert_eq!(fields_metadata.len(), deserialized.len());
        assert_eq!(deserialized[0].field_name, "test");
        assert_eq!(deserialized[0].field_type, ListFieldType::Str as i32);
        assert!(deserialized[0].searchable);
        assert!(deserialized[0].aggregatable);

        assert_eq!(deserialized[1].field_name, "test2");
        assert_eq!(deserialized[1].field_type, ListFieldType::Str as i32);
        assert!(deserialized[1].searchable);
        assert!(!deserialized[1].aggregatable);

        assert_eq!(deserialized[2].field_name, "test3");
        assert_eq!(deserialized[2].field_type, ListFieldType::U64 as i32);
        assert!(deserialized[2].searchable);
        assert!(deserialized[2].aggregatable);
    }

    fn make_indexed_split_for_test(
        segment_timestamps: &[DateTime],
    ) -> anyhow::Result<IndexedSplit> {
        let split_scratch_directory = TempDirectory::for_test();
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let timestamp_field = schema_builder.add_u64_field("timestamp", FAST);
        let tag_str = schema_builder.add_text_field("tag_str", STRING);
        let tag_many = schema_builder.add_text_field("tag_many", STRING);
        let tag_u64 =
            schema_builder.add_u64_field("tag_u64", NumericOptions::default().set_indexed());
        let tag_i64 =
            schema_builder.add_i64_field("tag_i64", NumericOptions::default().set_indexed());
        let tag_f64 =
            schema_builder.add_f64_field("tag_f64", NumericOptions::default().set_indexed());
        let tag_bool =
            schema_builder.add_bool_field("tag_bool", NumericOptions::default().set_indexed());
        let schema = schema_builder.build();
        let index_builder = IndexBuilder::new()
            .settings(IndexSettings::default())
            .schema(schema)
            .tokenizers(
                quickwit_query::create_default_quickwit_tokenizer_manager()
                    .tantivy_manager()
                    .clone(),
            )
            .fast_field_tokenizers(
                quickwit_query::get_quickwit_fastfield_normalizer_manager()
                    .tantivy_manager()
                    .clone(),
            );
        let index_directory = MmapDirectory::open(split_scratch_directory.path())?;
        let mut index_writer =
            index_builder.single_segment_index_writer(index_directory, 100_000_000)?;
        let mut timerange_opt: Option<RangeInclusive<DateTime>> = None;
        let mut num_docs = 0;
        for &timestamp in segment_timestamps {
            for num in 1..10 {
                let doc = doc!(
                    text_field => format!("timestamp is {timestamp:?}"),
                    timestamp_field => timestamp,
                    tag_str => "value",
                    tag_many => format!("many-{num}"),
                    tag_u64 => 42u64,
                    tag_i64 => -42i64,
                    tag_f64 => -42.02f64,
                    tag_bool => true,
                );
                index_writer.add_document(doc)?;
                num_docs += 1;
                timerange_opt = Some(
                    timerange_opt
                        .map(|timestamp_range| {
                            let start = timestamp.min(*timestamp_range.start());
                            let end = timestamp.max(*timestamp_range.end());
                            RangeInclusive::new(start, end)
                        })
                        .unwrap_or_else(|| RangeInclusive::new(timestamp, timestamp)),
                )
            }
        }
        let index = index_writer.finalize()?;

        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();

        // TODO: In the future we would like that kind of segment flush to emit a new split,
        // but this will require work on tantivy.
        let indexed_split = IndexedSplit {
            split_attrs: SplitAttrs {
                node_id,
                index_uid,
                source_id,
                doc_mapping_uid: DocMappingUid::default(),
                split_id: "test-split".to_string(),
                partition_id: 17u64,
                num_docs,
                uncompressed_docs_size_in_bytes: num_docs * 15,
                time_range: timerange_opt,
                replaced_split_ids: Vec::new(),
                delete_opstamp: 0,
                num_merge_ops: 0,
            },
            index,
            split_scratch_directory,
            controlled_directory_opt: None,
        };
        Ok(indexed_split)
    }

    fn get_tag_fields(schema: Schema, field_names: &[&str]) -> Vec<NamedField> {
        field_names
            .iter()
            .map(|field_name| {
                let field = schema.get_field(field_name).unwrap();
                let field_type = schema.get_field_entry(field).field_type().clone();
                NamedField {
                    name: field_name.to_string(),
                    field,
                    field_type,
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn test_packager_simple() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let (mailbox, inbox) = universe.create_test_mailbox();
        let indexed_split = make_indexed_split_for_test(&[
            DateTime::from_timestamp_secs(1628203589),
            DateTime::from_timestamp_secs(1628203640),
        ])?;
        let tag_fields = get_tag_fields(
            indexed_split.index.schema(),
            &[
                "tag_str", "tag_many", "tag_u64", "tag_i64", "tag_f64", "tag_bool",
            ],
        );
        let packager = Packager::new("TestPackager", tag_fields, mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_builder().spawn(packager);
        packager_mailbox
            .send_message(IndexedSplitBatch {
                splits: vec![indexed_split],
                checkpoint_delta_opt: IndexCheckpointDelta::for_test("source_id", 10..20).into(),
                publish_lock: PublishLock::default(),
                publish_token_opt: None,
                merge_task_opt: None,
                batch_parent_span: Span::none(),
            })
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let packaged_splits = inbox.drain_for_test();
        assert_eq!(packaged_splits.len(), 1);
        let packaged_split = packaged_splits[0]
            .downcast_ref::<PackagedSplitBatch>()
            .unwrap();
        let split = &packaged_split.splits[0];
        assert_eq!(
            &split.tags.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
            &[
                "tag_bool!",
                "tag_bool:true",
                "tag_f64!",
                "tag_f64:-42.02",
                "tag_i64!",
                "tag_i64:-42",
                "tag_str!",
                "tag_str:value",
                "tag_u64!",
                "tag_u64:42"
            ]
        );
        assert_eq!(
            split.split_attrs.time_range,
            Some(
                DateTime::from_timestamp_secs(1628203589)
                    ..=DateTime::from_timestamp_secs(1628203640)
            )
        );
        universe.assert_quit().await;
        Ok(())
    }
}
