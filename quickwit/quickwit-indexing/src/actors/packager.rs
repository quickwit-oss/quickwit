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

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_directories::write_hotcache;
use quickwit_doc_mapper::tag_pruning::append_to_tag_set;
use quickwit_doc_mapper::NamedField;
use tantivy::schema::FieldType;
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
        info!(
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
                batch.merge_operation_opt,
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

/// returns true iff merge is required to reach a state where
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
    info!(split_id = split.split_id(), "create-packaged-split");
    let split_files = list_split_files(segment_metas, &split.split_scratch_directory)?;

    // Extracts tag values from inverted indexes only when a field cardinality is less
    // than `MAX_VALUES_PER_TAG_FIELD`.
    debug!(split_id = split.split_id(), tag_fields =? tag_fields, "extract-tags-values");
    let index_reader = split
        .index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
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
                warn!(err=?tag_extraction_error,  "No field values will be registered in the split metadata.");
            }
        }
    }

    ctx.record_progress();

    debug!(split_id = split.split_id(), "build-hotcache");
    let mut hotcache_bytes = Vec::new();
    build_hotcache(split.split_scratch_directory.path(), &mut hotcache_bytes)?;
    ctx.record_progress();

    let packaged_split = PackagedSplit {
        split_attrs: split.split_attrs,
        split_scratch_directory: split.split_scratch_directory,
        tags,
        split_files,
        hotcache_bytes,
    };
    Ok(packaged_split)
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
    use quickwit_proto::indexing::IndexingPipelineId;
    use quickwit_proto::types::IndexUid;
    use tantivy::directory::MmapDirectory;
    use tantivy::schema::{NumericOptions, Schema, FAST, STRING, TEXT};
    use tantivy::{doc, DateTime, IndexBuilder, IndexSettings};
    use tracing::Span;

    use super::*;
    use crate::models::{PublishLock, SplitAttrs};

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
            .tokenizers(quickwit_query::create_default_quickwit_tokenizer_manager())
            .fast_field_tokenizers(
                quickwit_query::get_quickwit_fastfield_normalizer_manager().clone(),
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
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new("test-index"),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };

        // TODO: In the future we would like that kind of segment flush to emit a new split,
        // but this will require work on tantivy.
        let indexed_split = IndexedSplit {
            split_attrs: SplitAttrs {
                split_id: "test-split".to_string(),
                partition_id: 17u64,
                pipeline_id,
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
                merge_operation_opt: None,
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
