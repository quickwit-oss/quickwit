/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::fmt;
use std::ops::RangeInclusive;
use std::path::Path;

use crate::indexing::manifest::Manifest;
use anyhow::{self, Context};
use quickwit_common::HOTCACHE_FILENAME;
use quickwit_directories::write_hotcache;
use quickwit_metastore::BundleAndSplitMetadata;
use quickwit_metastore::Metastore;
use quickwit_metastore::SplitMetadata;
use quickwit_storage::{PutPayload, Storage, StorageUriResolver};
use std::sync::Arc;
use std::time::Instant;
use std::{path::PathBuf, usize};
use tantivy::schema::Field;
use tantivy::Directory;
use tantivy::SegmentId;
use tantivy::SegmentMeta;
use tantivy::{directory::MmapDirectory, merge_policy::NoMergePolicy, schema::Schema, Document};
use tempfile::TempDir;
use tracing::{info, warn};
use uuid::Uuid;

use super::manifest::ManifestEntry;
use super::IndexDataParams;

pub const MAX_DOC_PER_SPLIT: usize = if cfg!(test) { 100 } else { 5_000_000 };

/// Struct that represents an instance of split
pub struct Split {
    /// Id of the split.
    pub id: Uuid,
    /// uri of the index this split belongs to.
    pub index_uri: String,
    /// id of the index relative to the metastore
    pub index_id: String,
    /// A combination of index_uri & split_id.
    pub split_uri: String,
    /// The split metadata.
    pub metadata: SplitMetadata,
    /// The local directory hosting this split artifacts.
    pub split_scratch_dir: Box<TempDir>,
    /// The tantivy index for this split.
    pub index: tantivy::Index,
    /// The configured index writer for this split.
    pub index_writer: Option<tantivy::IndexWriter>,
    /// The number of parsing errors occurred during this split construction
    pub num_parsing_errors: usize,
    /// The storage instance for this split.
    pub storage: Arc<dyn Storage>,
    /// The metastore instance.
    pub metastore: Arc<dyn Metastore>,
    /// The timestamp field
    pub timestamp_field: Option<Field>,
}

impl fmt::Debug for Split {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Split")
            .field("id", &self.id)
            .field("metadata", &self.metadata)
            .field("local_directory", &self.split_scratch_dir.path())
            .field("index_uri", &self.index_uri)
            .field("num_parsing_errors", &self.num_parsing_errors)
            .finish()
    }
}

/// returns true iff merge is required to reach a state where
/// we have zero, or a single segment with no deletes segment.
fn is_merge_required(segment_metas: &[SegmentMeta]) -> bool {
    match &segment_metas {
        // there are no segment to merge
        [] => false,
        // if there is only segment but it has deletes, it
        // still makes sense to merge it alone in order to remove deleted documents.
        [segment_meta] => segment_meta.has_deletes(),
        _ => true,
    }
}

impl Split {
    /// Create a new instance of an index split.
    pub async fn create(
        params: &IndexDataParams,
        storage_resolver: StorageUriResolver,
        metastore: Arc<dyn Metastore>,
        schema: Schema,
        timestamp_field: Option<Field>,
    ) -> anyhow::Result<Self> {
        let id = Uuid::new_v4();
        let split_scratch_dir = Box::new(tempfile::tempdir_in(params.temp_dir.path())?);
        let index = tantivy::Index::create_in_dir(split_scratch_dir.path(), schema)?;
        let index_writer =
            index.writer_with_num_threads(params.num_threads, params.heap_size as usize)?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let index_uri = metastore.index_metadata(&params.index_id).await?.index_uri;
        let mut metadata = SplitMetadata::new(id.to_string());
        // Metadata range is initialized with i64 MAX and MIN.
        // Note that this range will be saved if none of documents in this split have a timestamp
        // which is obviously a strange situation.
        if timestamp_field.is_some() {
            metadata.time_range = Some(RangeInclusive::new(i64::MAX, i64::MIN));
        }
        let split_uri = format!("{}/{}", index_uri, id);
        let storage = storage_resolver.resolve(&split_uri)?;
        Ok(Self {
            id,
            index_uri,
            index_id: params.index_id.clone(),
            split_uri,
            metadata,
            split_scratch_dir,
            index,
            index_writer: Some(index_writer),
            num_parsing_errors: 0,
            storage,
            metastore,
            timestamp_field,
        })
    }

    /// Add document to the index split.
    pub fn add_document(&mut self, doc: Document, doc_size: usize) -> anyhow::Result<()> {
        self.update_metadata(&doc, doc_size)?;
        self.index_writer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing index writer."))?
            .add_document(doc);
        Ok(())
    }

    /// Increment document parse errors.
    pub fn add_parse_error(&mut self) {
        self.num_parsing_errors += 1;
    }

    /// Update the split metadata (num_records, size_in_bytes, time_range) based on incomming document.
    fn update_metadata(&mut self, doc: &Document, doc_size: usize) -> anyhow::Result<()> {
        if let Some(timestamp_field) = self.timestamp_field {
            let split_time_range =
                self.metadata.time_range.as_mut().with_context(|| {
                    "Split time range must be set if timestamp field is present."
                })?;
            if let Some(timestamp) = doc
                .get_first(timestamp_field)
                .and_then(|field_value| field_value.i64_value())
            {
                *split_time_range = RangeInclusive::new(
                    timestamp.min(*split_time_range.start()),
                    timestamp.max(*split_time_range.end()),
                );
            }
        };

        self.metadata.size_in_bytes += doc_size as u64;
        self.metadata.num_records += 1;
        Ok(())
    }

    /// Checks to see if the split has enough documents.
    pub fn has_enough_docs(&self) -> bool {
        self.metadata.num_records >= MAX_DOC_PER_SPLIT
    }

    /// Commits the split.
    pub async fn commit(&mut self) -> anyhow::Result<u64> {
        let mut index_writer = self.index_writer.take().unwrap();
        let (moved_index_writer, commit_opstamp) = tokio::task::spawn_blocking(move || {
            let opstamp = index_writer.commit()?;
            anyhow::Result::<(tantivy::IndexWriter, u64)>::Ok((index_writer, opstamp))
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))??;

        self.index_writer = Some(moved_index_writer);
        Ok(commit_opstamp)
    }

    /// Merge all segments of the split into one.
    ///
    /// If there is only one segment and it has no delete, avoid doing anything them.
    pub async fn merge_all_segments(&mut self) -> anyhow::Result<()> {
        let segment_metas = self.index.searchable_segment_metas()?;
        if !is_merge_required(&segment_metas[..]) {
            return Ok(());
        }
        let segment_ids: Vec<SegmentId> = segment_metas
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect();
        self.index_writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Missing index writer."))?
            .merge(&segment_ids)
            .await?;
        Ok(())
    }

    /// Build the split hotcache file
    pub async fn build_hotcache(&mut self) -> anyhow::Result<()> {
        let split_scratch_dir = self.split_scratch_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let hotcache_path = split_scratch_dir.join(HOTCACHE_FILENAME);
            let mut hotcache_file = std::fs::File::create(&hotcache_path)?;
            let mmap_directory = MmapDirectory::open(split_scratch_dir)?;
            write_hotcache(mmap_directory, &mut hotcache_file)?;
            anyhow::Result::<()>::Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))?
    }

    /// Stage a split in the metastore.
    pub async fn stage(&self) -> anyhow::Result<String> {
        let metadata = BundleAndSplitMetadata {
            split_metadata: self.metadata.clone(),
            bundle_offsets: Default::default(),
        };
        self.metastore.stage_split(&self.index_id, metadata).await?;
        Ok(self.id.to_string())
    }

    /// Upload all split artifacts using the storage.
    pub async fn upload(&self) -> anyhow::Result<Manifest> {
        let manifest = put_split_files_to_storage(&*self.storage, self).await?;
        Ok(manifest)
    }

    // TODO: This is here to prove to ourselves that tags are working
    // with tests. This indexing will be replaced by the actor indexing pipeline.
    /// Extracts tags from the split.
    pub async fn extract_tags(&mut self, tags_field: Option<Field>) -> anyhow::Result<()> {
        let index_reader = self.index.reader()?;
        if let Some(tags_field) = tags_field {
            for reader in index_reader.searcher().segment_readers() {
                let inv_index = reader.inverted_index(tags_field)?;
                let mut terms_streamer = inv_index.terms().stream()?;
                while let Some((term_data, _)) = terms_streamer.next() {
                    self.metadata
                        .tags
                        .push(String::from_utf8_lossy(term_data).to_string());
                }
            }
        }
        Ok(())
    }
}

/// Upload all files within a single split to the storage
async fn put_split_files_to_storage(
    storage: &dyn Storage,
    split: &Split,
) -> anyhow::Result<Manifest> {
    info!("upload-split");
    let start = Instant::now();

    let mut manifest = Manifest::new(split.metadata.clone());
    manifest.segments = split.index.searchable_segment_ids()?;

    let mut files_to_upload: Vec<PathBuf> = split
        .index
        .searchable_segment_metas()?
        .into_iter()
        .flat_map(|segment_meta| segment_meta.list_files())
        .filter(|filepath| {
            // the list given by segment_meta.list_files() can contain false positives.
            // Some of those files actually do not exists.
            // Lets' filter them out.
            // TODO modify tantivy to make this list
            split.index.directory().exists(filepath).unwrap_or(true) //< true might look like a very odd choice here.
                                                                     // It has the benefit of triggering an error when we will effectively try to upload the files.
        })
        .map(|relative_filepath| split.split_scratch_dir.path().join(relative_filepath))
        .collect();
    files_to_upload.push(split.split_scratch_dir.path().join("meta.json"));
    files_to_upload.push(split.split_scratch_dir.path().join("hotcache"));

    let mut upload_res_futures = vec![];

    for path in files_to_upload {
        let file: tokio::fs::File = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("Failed to get metadata for {:?}", &path))?;
        let metadata = file.metadata().await?;
        let file_name = match path.file_name() {
            Some(fname) => fname.to_string_lossy().to_string(),
            _ => {
                warn!(path = %path.display(), "Could not extract path as string");
                continue;
            }
        };

        manifest.push(&file_name, metadata.len());
        let key = PathBuf::from(file_name);
        let payload = quickwit_storage::PutPayload::from(path.clone());
        let upload_res_future = async move {
            storage.put(&key, payload).await.with_context(|| {
                format!(
                    "Failed uploading key {} in bucket {}",
                    key.display(),
                    split.index_uri
                )
            })?;
            Result::<(), anyhow::Error>::Ok(())
        };

        upload_res_futures.push(upload_res_future);
    }

    futures::future::try_join_all(upload_res_futures).await?;

    let manifest_body = manifest.to_json()?.into_bytes();
    let manifest_path = PathBuf::from(".manifest");
    storage
        .put(&manifest_path, PutPayload::from(manifest_body))
        .await?;

    let elapsed_secs = start.elapsed().as_secs();
    let file_statistics = manifest.file_statistics();
    info!(
        min_file_size_in_bytes = %file_statistics.min_file_size_in_bytes,
        max_file_size_in_bytes = %file_statistics.max_file_size_in_bytes,
        avg_file_size_in_bytes = %file_statistics.avg_file_size_in_bytes,
        split_size_in_megabytes = %manifest.split_size_in_bytes / 1000,
        elapsed_secs = %elapsed_secs,
        throughput_mb_s = %manifest.split_size_in_bytes / 1000 / elapsed_secs.max(1),
        "Uploaded split to storage"
    );

    Ok(manifest)
}

/// [`FileEntry`] is an alias of [`ManifestEntry`] for
/// holding the full path & size of a file.
pub type FileEntry = ManifestEntry;

/// Removes all files contained within a single split from storage at `split_uri`.
/// This function only cares about cleaning up files without any concern for the metastore.
/// You should therefore make sure the metastore is left in good state.
///
/// * `split_uri` - The target index split uri.
/// * `storage_resolver` - The storage resolver object.
/// * `dry_run` - Should this only return a list of affected files without performing deletion.
///
pub async fn remove_split_files_from_storage(
    split_uri: &str,
    storage_resolver: StorageUriResolver,
    dry_run: bool,
) -> anyhow::Result<Vec<FileEntry>> {
    info!(split_uri =% split_uri, "delete-split");
    let storage = storage_resolver.resolve(split_uri)?;

    let manifest_file = Path::new(".manifest");
    // Removing a non-existing split is considered ok.
    // A split can be listed by the metastore when it doesn't in fact exist on disk for some reasons:
    // - split was staged but failed to upload.
    // - operation canceled by the user right in the middle.
    if !storage.exists(manifest_file).await? {
        return Ok(vec![]);
    }
    let data = storage.get_all(manifest_file).await?;
    let manifest: Manifest = serde_json::from_slice(&data)?;

    if !dry_run {
        let mut delete_file_futures: Vec<_> = manifest
            .files
            .iter()
            .map(|entry| storage.delete(Path::new(&entry.file_name)))
            .collect();
        delete_file_futures.push(storage.delete(manifest_file));

        futures::future::try_join_all(delete_file_futures).await?;
    }

    let mut file_entries: Vec<_> = manifest
        .files
        .iter()
        .map(|entry| FileEntry {
            file_name: format!("{}/{}", split_uri, entry.file_name),
            file_size_in_bytes: entry.file_size_in_bytes,
        })
        .collect();
    file_entries.push(FileEntry {
        file_name: format!("{}/{}", split_uri, ".manifest"),
        file_size_in_bytes: data.len() as u64,
    });

    Ok(file_entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickwit_index_config::AllFlattenIndexConfig;
    use quickwit_metastore::checkpoint::Checkpoint;
    use quickwit_metastore::IndexMetadata;
    use quickwit_metastore::MockMetastore;
    use tantivy::{
        schema::{Schema, FAST, STRING},
        Document,
    };
    use tokio::task;

    #[tokio::test]
    async fn test_split() -> anyhow::Result<()> {
        let index_uri = "ram://test-index/index";
        let params = &IndexDataParams {
            index_id: "test".to_string(),
            temp_dir: Arc::new(tempfile::tempdir()?),
            num_threads: 1,
            heap_size: 3000000,
            overwrite: false,
        };
        let schema = Schema::builder().build();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_index_metadata()
            .times(1)
            .returning(move |index_id| {
                Ok(IndexMetadata {
                    index_id: index_id.to_string(),
                    index_uri: index_uri.to_string(),
                    index_config: Arc::new(AllFlattenIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        mock_metastore
            .expect_stage_split()
            .times(1)
            .returning(move |index_id, _split_metadata| {
                assert_eq!(index_id, "test");
                Ok(())
            });

        let metastore = Arc::new(mock_metastore);
        let mut split = Split::create(
            params,
            StorageUriResolver::default(),
            metastore,
            schema,
            None,
        )
        .await?;

        for _ in 0..20 {
            split.add_document(Document::default(), 0)?;
        }
        assert_eq!(split.metadata.num_records, 20);
        assert_eq!(split.metadata.time_range, None);
        assert_eq!(split.num_parsing_errors, 0);
        assert!(!split.has_enough_docs());

        for _ in 0..90 {
            split.add_document(Document::default(), 0)?;
        }
        assert_eq!(split.metadata.num_records, 110);
        assert_eq!(split.metadata.time_range, None);
        assert!(split.has_enough_docs());

        split.commit().await?;
        split.merge_all_segments().await?;
        split.build_hotcache().await?;

        task::spawn(async move {
            split.stage().await?;
            split.upload().await
        })
        .await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_split_time_range() -> anyhow::Result<()> {
        let index_uri = "ram://test-index/index";
        let params = &IndexDataParams {
            index_id: "test".to_string(),
            temp_dir: Arc::new(tempfile::tempdir()?),
            num_threads: 1,
            heap_size: 3000000,
            overwrite: false,
        };

        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("name", STRING);
        let timestamp = schema_builder.add_i64_field("timestamp", FAST);
        let schema = schema_builder.build();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_index_metadata()
            .times(1)
            .returning(move |index_id| {
                Ok(IndexMetadata {
                    index_id: index_id.to_string(),
                    index_uri: index_uri.to_string(),
                    index_config: Arc::new(AllFlattenIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });

        let metastore = Arc::new(mock_metastore);
        let split_result = Split::create(
            params,
            StorageUriResolver::default(),
            metastore,
            schema.clone(),
            Some(timestamp),
        )
        .await;
        assert!(split_result.is_ok());

        let mut split = split_result?;
        let docs = vec![
            r#"{"timestamp": 2, "name": "INFO"}"#,
            r#"{"timestamp": 6, "name": "WARNING"}"#,
            r#"{"name": "UNKNOWN"}"#,
            r#"{"timestamp": 4, "name": "WARNING"}"#,
            r#"{"timestamp": 3, "name": "DEBUG"}"#,
        ];

        for doc in docs {
            split.add_document(schema.parse_document(doc)?, doc.as_bytes().len())?;
        }

        assert_eq!(split.metadata.num_records, 5);
        assert_eq!(split.metadata.time_range, Some(2..=6));
        Ok(())
    }
}
