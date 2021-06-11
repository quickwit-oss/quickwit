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
use std::ops::Range;
use std::path::Path;

use crate::indexing::manifest::Manifest;
use anyhow::{self, Context};
use quickwit_directories::write_hotcache;
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
    pub split_scratch_dir: Arc<TempDir>,
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
    ) -> anyhow::Result<Self> {
        let id = Uuid::new_v4();
        let split_scratch_dir = Arc::new(tempfile::tempdir_in(params.temp_dir.path())?);
        let index = tantivy::Index::create_in_dir(split_scratch_dir.path(), schema)?;
        let index_writer =
            index.writer_with_num_threads(params.num_threads, params.heap_size as usize)?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let index_uri = metastore.index_metadata(&params.index_id).await?.index_uri;
        let metadata = SplitMetadata::new(id.to_string());

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
        })
    }

    /// Add document to the index split.
    pub fn add_document(
        &mut self,
        doc: Document,
        timestamp_field_opt: Option<Field>,
    ) -> anyhow::Result<()> {
        let mut current_time_range = self.metadata.time_range.clone().unwrap_or(Range {
            start: u64::MAX,
            end: u64::MIN,
        }); //u64::MAX..u64::MIN
        if let Some(timestamp_field) = timestamp_field_opt {
            if let Some(timestamp) = doc
                .get_first(timestamp_field)
                .and_then(|field_value| field_value.u64_value())
            {
                if timestamp < current_time_range.start {
                    current_time_range.start = timestamp;
                }
                if timestamp > current_time_range.end {
                    current_time_range.end = timestamp;
                }
            }
        }

        self.index_writer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing index writer."))?
            .add_document(doc);
        self.metadata.num_records += 1;
        self.metadata.time_range = Some(current_time_range);
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
        let split_scratch_dir = self.split_scratch_dir.clone();
        tokio::task::spawn_blocking(move || {
            let hotcache_path = split_scratch_dir.path().join("hotcache");
            let mut hotcache_file = std::fs::File::create(&hotcache_path)?;
            let mmap_directory = MmapDirectory::open(split_scratch_dir.path())?;
            write_hotcache(mmap_directory, &mut hotcache_file)?;
            anyhow::Result::<()>::Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))?
    }

    /// Stage a split in the metastore.
    pub async fn stage(&self) -> anyhow::Result<String> {
        self.metastore
            .stage_split(&self.index_id, self.metadata.clone())
            .await?;
        Ok(self.id.to_string())
    }

    /// Upload all split artifacts using the storage.
    pub async fn upload(&self) -> anyhow::Result<Manifest> {
        let manifest = put_split_files_to_storage(&*self.storage, self).await?;
        Ok(manifest)
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
) -> anyhow::Result<Vec<PathBuf>> {
    info!(split_uri =% split_uri, "delete-split");
    let storage = storage_resolver.resolve(split_uri)?;

    let manifest_file = Path::new(".manifest");
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

    let mut files: Vec<_> = manifest
        .files
        .iter()
        .map(|entry| PathBuf::from(format!("{}/{}", split_uri, entry.file_name)))
        .collect();
    files.push(PathBuf::from(format!("{}/{}", split_uri, ".manifest")));

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickwit_doc_mapping::AllFlattenDocMapper;
    use quickwit_doc_mapping::IndexSettings;
    use quickwit_metastore::IndexMetadata;
    use quickwit_metastore::MockMetastore;
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
                    doc_mapper: Box::new(AllFlattenDocMapper::new()),
                    settings: IndexSettings::default(),
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
        let split_result =
            Split::create(params, StorageUriResolver::default(), metastore, schema).await;
        assert_eq!(split_result.is_ok(), true);

        let mut split = split_result?;
        for _ in 0..20 {
            split.add_document(Document::default(), None)?;
        }
        assert_eq!(split.metadata.num_records, 20);
        assert_eq!(split.num_parsing_errors, 0);
        assert_eq!(split.has_enough_docs(), false);

        for _ in 0..90 {
            split.add_document(Document::default(), None)?;
        }
        assert_eq!(split.metadata.num_records, 110);
        assert_eq!(split.has_enough_docs(), true);

        let commit_result = split.commit().await;
        assert_eq!(commit_result.is_ok(), true);

        let merge_result = split.merge_all_segments().await;
        assert_eq!(merge_result.is_ok(), true);

        let hotcache_result = split.build_hotcache().await;
        assert_eq!(hotcache_result.is_ok(), true);

        task::spawn(async move {
            split.stage().await?;
            split.upload().await
        })
        .await??;

        Ok(())
    }
}
