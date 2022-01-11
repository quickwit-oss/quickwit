use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;
use quickwit_config::{IndexerConfig, SourceConfig};
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;
 
use crate::actors::{IndexingPipeline, IndexingPipelineParams, IndexingServer};
use crate::models::IndexingStatistics;
pub use crate::split_store::{
    get_tantivy_directory_from_split_bundle, IndexingSplitStore, IndexingSplitStoreParams,
    SplitFolder,
};

pub mod actors;
mod controlled_directory;
mod garbage_collection;
pub mod merge_policy;
pub mod models;
pub mod source;
mod split_store;
mod test_utils;

pub use test_utils::{mock_split, mock_split_meta, TestSandbox};

pub use self::garbage_collection::{delete_splits_with_files, run_garbage_collect, FileEntry};
pub use self::merge_policy::{MergePolicy, StableMultitenantWithTimestampMergePolicy};
pub use self::source::check_source_connectivity;


pub async fn index_data (
    index_id: String,
    data_dir_path: PathBuf,
    indexer_config: IndexerConfig,
    source: SourceConfig,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
) -> anyhow::Result<IndexingStatistics> {
    let client = IndexingServer::spawn(data_dir_path, indexer_config, metastore, storage_resolver);
    let pipeline_id = client.spawn_pipeline(index_id, source).await?;
    let pipeline_handle = client.detach_pipeline(&pipeline_id).await?;

    let (exit_status, statistics) = pipeline_handle.join().await;
    if !exit_status.is_success() {
        bail!(exit_status);
    }
    Ok(statistics)
}


pub fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}