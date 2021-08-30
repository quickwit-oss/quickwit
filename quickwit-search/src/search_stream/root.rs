// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use itertools::Either;
use itertools::Itertools;
use quickwit_metastore::SplitMetadataAndFooterOffsets;
use quickwit_proto::LeafSearchStreamRequest;
use quickwit_proto::SearchStreamRequest;
use quickwit_proto::SplitIdAndFooterOffsets;
use tracing::*;

use quickwit_metastore::Metastore;
use quickwit_proto::SearchRequest;

use crate::client_pool::Job;
use crate::list_relevant_splits;
use crate::root::job_for_splits;
use crate::root::NodeSearchError;
use crate::ClientPool;
use crate::SearchClientPool;
use crate::SearchError;

/// Perform a distributed search stream.
pub async fn root_search_stream(
    search_stream_request: &SearchStreamRequest,
    metastore: &dyn Metastore,
    client_pool: &Arc<SearchClientPool>,
) -> Result<Vec<Bytes>, SearchError> {
    let start_instant = tokio::time::Instant::now();
    // TODO: building a search request should not be necessary for listing splits.
    // This needs some refactoring: relevant splits, metadata_map, jobs...
    let search_request = SearchRequest::from(search_stream_request.clone());
    let split_metadata_list = list_relevant_splits(&search_request, metastore).await?;
    let split_metadata_map: HashMap<String, SplitMetadataAndFooterOffsets> = split_metadata_list
        .into_iter()
        .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
        .collect();
    let leaf_search_jobs: Vec<Job> =
        job_for_splits(&split_metadata_map.keys().collect(), &split_metadata_map);
    let assigned_leaf_search_jobs = client_pool
        .assign_jobs(leaf_search_jobs, &HashSet::default())
        .await?;

    debug!(assigned_leaf_search_jobs=?assigned_leaf_search_jobs, "Assigned leaf search jobs.");

    let mut handles = Vec::new();
    for (mut search_client, jobs) in assigned_leaf_search_jobs {
        let split_metadata_list: Vec<SplitIdAndFooterOffsets> = jobs
            .iter()
            .map(|job| SplitIdAndFooterOffsets {
                split_id: job.metadata.split_metadata.split_id.clone(),
                split_footer_start: job.metadata.footer_offsets.start,
                split_footer_end: job.metadata.footer_offsets.end,
            })
            .collect();
        let leaf_request = LeafSearchStreamRequest {
            request: Some(search_stream_request.clone()),
            split_metadata: split_metadata_list.clone(),
        };
        debug!(leaf_request=?leaf_request, grpc_addr=?search_client.grpc_addr(), "Leaf node search stream.");
        let handle = tokio::spawn(async move {
            let mut receiver = search_client
                .leaf_search_stream(leaf_request)
                .await
                .map_err(|search_error| NodeSearchError {
                    search_error,
                    split_ids: split_metadata_list
                        .iter()
                        .map(|split| split.split_id.clone())
                        .collect(),
                })?;

            let mut leaf_bytes: Vec<Bytes> = Vec::new();
            while let Some(leaf_result) = receiver.next().await {
                let leaf_data = leaf_result.map_err(|search_error| NodeSearchError {
                    search_error,
                    split_ids: split_metadata_list
                        .iter()
                        .map(|split| split.split_id.clone())
                        .collect(),
                })?;
                leaf_bytes.push(Bytes::from(leaf_data.data));
            }
            Result::<Vec<Bytes>, NodeSearchError>::Ok(leaf_bytes)
        });
        handles.push(handle);
    }

    let nodes_results = futures::future::try_join_all(handles).await?;
    let (nodes_bytes, errors): (Vec<Vec<Bytes>>, Vec<_>) =
        nodes_results
            .into_iter()
            .partition_map(|result| match result {
                Ok(bytes) => Either::Left(bytes),
                Err(error) => Either::Right(error),
            });
    let overall_bytes: Vec<Bytes> = itertools::concat(nodes_bytes);
    if !errors.is_empty() {
        error!(error=?errors, "Some leaf requests have failed");
        return Err(SearchError::InternalError(format!("{:?}", errors)));
    }
    let elapsed = start_instant.elapsed();
    info!("Root search stream completed in {:?}", elapsed);
    Ok(overall_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Range;

    use crate::MockSearchService;
    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_metastore::SplitMetadata;
    use quickwit_metastore::{
        checkpoint::Checkpoint, IndexMetadata, MockMetastore, SplitMetadataAndFooterOffsets,
        SplitState,
    };
    use quickwit_proto::OutputFormat;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    fn mock_split_meta(split_id: &str) -> SplitMetadataAndFooterOffsets {
        SplitMetadataAndFooterOffsets {
            footer_offsets: 700..800,
            split_metadata: SplitMetadata {
                split_id: split_id.to_string(),
                split_state: SplitState::Published,
                num_records: 10,
                size_in_bytes: 256,
                time_range: None,
                generation: 1,
                update_timestamp: 0,
                tags: vec![],
            },
        }
    }

    #[tokio::test]
    async fn test_root_search_stream_single_split() -> anyhow::Result<()> {
        let request = quickwit_proto::SearchStreamRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            tags: vec![],
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );
        let mut mock_search_service = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        result_sender.send(Ok(quickwit_proto::LeafSearchStreamResult {
            data: b"123".to_vec(),
        }))?;
        result_sender.send(Ok(quickwit_proto::LeafSearchStreamResult {
            data: b"456".to_vec(),
        }))?;
        mock_search_service.expect_leaf_search_stream().return_once(
            |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                Ok(UnboundedReceiverStream::new(result_receiver))
            },
        );
        // The test will hang on indefinitely if we don't drop the receiver.
        drop(result_sender);
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?);
        let result: Vec<Bytes> = root_search_stream(&request, &metastore, &client_pool).await?;
        assert_eq!(result.len(), 2);
        assert_eq!(&result[0], &b"123"[..]);
        assert_eq!(&result[1], &b"456"[..]);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_stream_single_split_with_error() -> anyhow::Result<()> {
        let request = quickwit_proto::SearchStreamRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            tags: vec![],
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );
        let mut mock_search_service = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        result_sender.send(Ok(quickwit_proto::LeafSearchStreamResult {
            data: b"123".to_vec(),
        }))?;
        result_sender.send(Err(SearchError::InternalError("error".to_string())))?;
        mock_search_service.expect_leaf_search_stream().return_once(
            |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                Ok(UnboundedReceiverStream::new(result_receiver))
            },
        );
        // The test will hang on indefinitely if we don't drop the receiver.
        drop(result_sender);
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?);
        let result = root_search_stream(&request, &metastore, &client_pool).await;
        assert_eq!(result.is_err(), true);
        assert_eq!(result.unwrap_err().to_string(), "Internal error: `[NodeSearchError { search_error: InternalError(\"error\"), split_ids: [\"split1\"] }]`.");
        Ok(())
    }
}
