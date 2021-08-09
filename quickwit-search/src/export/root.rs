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
use std::io::Write;
use std::sync::Arc;

use futures::StreamExt;
use quickwit_proto::ExportRequest;
use quickwit_proto::LeafExportRequest;
use tracing::*;

use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::SearchRequest;

use crate::client_pool::Job;
use crate::error::parse_grpc_error;
use crate::list_relevant_splits;
use crate::root::job_for_splits;
use crate::root::NodeSearchError;
use crate::ClientPool;
use crate::SearchClientPool;
use crate::SearchError;

/// Perform a distributed export.
pub async fn root_export(
    export_request: &ExportRequest,
    metastore: &dyn Metastore,
    client_pool: &Arc<SearchClientPool>,
) -> Result<Vec<u8>, SearchError> {
    let start_instant = tokio::time::Instant::now();
    // TODO: building a search request should not be necessary for listing splits.
    // This needs some refactoring: relevant splits, metadata_map, jobs...
    let search_request = SearchRequest::from(export_request.clone());
    let split_metadata_list = list_relevant_splits(&search_request, metastore).await?;
    let split_metadata_map: HashMap<String, SplitMetadata> = split_metadata_list
        .into_iter()
        .map(|split_metadata| (split_metadata.split_id.clone(), split_metadata))
        .collect();
    let leaf_search_jobs: Vec<Job> =
        job_for_splits(&split_metadata_map.keys().collect(), &split_metadata_map);
    let assigned_leaf_search_jobs = client_pool
        .assign_jobs(leaf_search_jobs, &HashSet::default())
        .await?;

    debug!(assigned_leaf_search_jobs=?assigned_leaf_search_jobs, "Assigned leaf search jobs.");

    let mut handles = Vec::new();
    for (mut search_client, jobs) in assigned_leaf_search_jobs {
        let split_ids: Vec<String> = jobs.iter().map(|job| job.split.clone()).collect();
        let leaf_export_request = LeafExportRequest {
            export_request: Some(export_request.clone()),
            split_ids: split_ids.clone(),
        };
        debug!(leaf_export_request=?leaf_export_request, grpc_addr=?search_client.grpc_addr(), "Leaf node export.");
        let handle = tokio::spawn(async move {
            let mut stream = search_client
                .leaf_export(leaf_export_request)
                .await
                .map_err(|search_error| NodeSearchError {
                    search_error,
                    split_ids: split_ids.clone(),
                })?;

            let mut leaf_bytes = Vec::new();
            while let Some(leaf_result) = stream.next().await {
                let leaf_data = leaf_result.map_err(|status| NodeSearchError {
                    search_error: parse_grpc_error(&status),
                    split_ids: split_ids.clone(),
                })?;
                leaf_bytes.extend(leaf_data.data);
            }
            Result::<Vec<u8>, NodeSearchError>::Ok(leaf_bytes)
        });
        handles.push(handle);
    }
    let export_results = futures::future::try_join_all(handles).await?;
    let mut errors = Vec::new();
    // TODO: refactor this part...
    let mut buffer = Vec::new();
    for response in export_results {
        if response.is_err() {
            errors.push(response.unwrap_err());
        } else {
            buffer.write(&response.unwrap()).map_err(|_| {
                SearchError::InternalError(
                    "Error when writing leaf export bytes into root buffer".to_owned(),
                )
            })?;
        }
    }
    if !errors.is_empty() {
        error!(error=?errors, "Some export leaf requests have failed");
        return Err(SearchError::InternalError(format!("{:?}", errors)));
    }
    let elapsed = start_instant.elapsed();
    info!("Root export completed in {:?}", elapsed);
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Range;

    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use crate::MockSearchService;

    fn mock_split_meta(split_id: &str) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Published,
            num_records: 10,
            size_in_bytes: 256,
            time_range: None,
            generation: 1,
            update_timestamp: 0,
        }
    }

    #[tokio::test]
    async fn test_root_export_single_split() -> anyhow::Result<()> {
        let export_request = quickwit_proto::ExportRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "timestamp".to_string(),
            output_format: "csv".to_string(),
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
                Ok(vec![mock_split_meta("split1")])
            },
        );
        let mut mock_search_service = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        result_sender.send(Ok(quickwit_proto::LeafExportResult {
            data: "123".as_bytes().to_vec(),
        }))?;
        result_sender.send(Ok(quickwit_proto::LeafExportResult {
            data: "456".as_bytes().to_vec(),
        }))?;
        mock_search_service.expect_leaf_export().return_once(
            |_leaf_search_req: quickwit_proto::LeafExportRequest| {
                Ok(UnboundedReceiverStream::new(result_receiver))
            },
        );
        // The test will hang on indefinitely if we don't drop the receiver.
        drop(result_sender);
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?);
        let export_result = root_export(&export_request, &metastore, &client_pool).await?;
        assert_eq!(export_result.len(), 6);
        assert_eq!(export_result, "123456".as_bytes().to_vec());
        Ok(())
    }
}
