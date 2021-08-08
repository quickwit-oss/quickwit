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
use crate::list_relevant_splits;
use crate::root::job_for_splits;
use crate::root::NodeSearchError;
use crate::ClientPool;
use crate::SearchClientPool;
use crate::SearchError;

/// Perform a distributed search by streaming the result.
pub async fn root_export(
    export_request: &ExportRequest,
    metastore: &dyn Metastore,
    client_pool: &Arc<SearchClientPool>,
) -> Result<Vec<u8>, SearchError> {
    let _start_instant = tokio::time::Instant::now();

    // Create a job for leaf node search and assign the splits that the node is responsible for based on the job.
    let search_request = SearchRequest::from(export_request.clone());
    let split_metadata_list = list_relevant_splits(&search_request, metastore).await?;

    // Create a hash map of SplitMetadata with split id as a key.
    let split_metadata_map: HashMap<String, SplitMetadata> = split_metadata_list
        .into_iter()
        .map(|split_metadata| (split_metadata.split_id.clone(), split_metadata))
        .collect();

    // Create a job for fetching docs and assign the splits that the node is responsible for based on the job.
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
            split_ids: jobs.iter().map(|job| job.split.clone()).collect(),
        };
        debug!(leaf_export_request=?leaf_export_request, grpc_addr=?search_client.grpc_addr(), "Leaf node search.");
        let handle = tokio::spawn(async move {
            let mut stream = search_client
                .leaf_export(leaf_export_request)
                .await
                .map_err(|search_error| NodeSearchError {
                    search_error,
                    split_ids: split_ids.clone(),
                })?;

            let mut leaf_results = Vec::new();
            while let Some(leaf_result) = stream.next().await {
                let leaf_data = leaf_result.map_err(|_| NodeSearchError {
                    search_error: SearchError::InternalError("todo".to_owned()),
                    split_ids: split_ids.clone(),
                })?;
                leaf_results.extend(leaf_data.data);
            }
            Result::<Vec<u8>, NodeSearchError>::Ok(leaf_results)
        });
        handles.push(handle);
    }
    let responses = futures::future::try_join_all(handles).await?;
    let mut errors = Vec::new();
    // TODO: refactor this part...
    let mut buffer = Vec::new();
    for response in responses {
        if response.is_err() {
            errors.push(response.unwrap_err());
        } else {
            buffer.write(&response.unwrap());
        }
    }
    if !errors.is_empty() {
        error!(error=?errors, "Some export leaf requests have failed");
        return Err(SearchError::InternalError(format!("{:?}", errors)));
    }
    Ok(buffer)
}
