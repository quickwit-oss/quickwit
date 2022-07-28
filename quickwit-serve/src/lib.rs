// Copyright (C) 2022 Quickwit, Inc.
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

mod args;
mod error;
mod format;
mod metrics;

mod grpc;
mod rest;

mod cluster_api;
mod health_check_api;
mod index_management_api;
mod indexing_api;
mod ingest_api;
mod node_info_handler;
mod search_api;
mod ui_handler;

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;

use format::Format;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::{Cluster, QuickwitService};
use quickwit_common::uri::Uri;
use quickwit_config::QuickwitConfig;
use quickwit_index_management::{
    start_control_plane_service, IndexManagementClient, IndexManagementService,
};
use quickwit_indexing::actors::IndexingService;
use quickwit_indexing::start_indexer_service;
use quickwit_ingest_api::{init_ingest_api, IngestApiService};
use quickwit_metastore::quickwit_metastore_uri_resolver;
use quickwit_search::{start_searcher_service, SearchService};
use quickwit_storage::quickwit_storage_uri_resolver;
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};

pub use crate::args::ServeArgs;
pub use crate::metrics::SERVE_METRICS;
#[cfg(test)]
use crate::rest::recover_fn;

fn require<T: Clone + Send>(
    val_opt: Option<T>,
) -> impl Filter<Extract = (T,), Error = Rejection> + Clone {
    warp::any().and_then(move || {
        let val_opt_clone = val_opt.clone();
        async move {
            if let Some(val) = val_opt_clone {
                Ok(val)
            } else {
                Err(warp::reject())
            }
        }
    })
}

fn with_arg<T: Clone + Send>(arg: T) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || arg.clone())
}

struct QuickwitServices {
    pub config: Arc<QuickwitConfig>,
    pub build_info: Arc<QuickwitBuildInfo>,
    pub cluster: Arc<Cluster>,
    /// We do have a search service even on nodes that are not running `search`.
    /// It is only used to serve the rest API calls and will only execute
    /// the root requests.
    pub search_service: Arc<dyn SearchService>,
    pub indexer_service: Option<Mailbox<IndexingService>>,
    pub ingest_api_service: Option<Mailbox<IngestApiService>>,
    pub index_management_service: Option<Mailbox<IndexManagementService>>,
    pub services: HashSet<QuickwitService>,
}

pub async fn serve_quickwit(
    config: QuickwitConfig,
    services: &HashSet<QuickwitService>,
) -> anyhow::Result<()> {
    let metastore = quickwit_metastore_uri_resolver()
        .resolve(&config.metastore_uri())
        .await?;
    let indexes = metastore
        .list_indexes_metadatas()
        .await?
        .into_iter()
        .map(|index| (index.index_id, index.index_uri));

    check_is_configured_for_cluster(&config.peer_seeds, metastore.uri(), indexes)?;

    let storage_resolver = quickwit_storage_uri_resolver().clone();

    let cluster = quickwit_cluster::start_cluster_service(&config, services).await?;

    // Check

    let universe = Universe::new();

    let ingest_api_service: Option<Mailbox<IngestApiService>> = if services
        .contains(&QuickwitService::Indexer)
    {
        let ingest_api_service = init_ingest_api(&universe, &config.data_dir_path.join("queues"))?;
        Some(ingest_api_service)
    } else {
        None
    };

    let search_service: Arc<dyn SearchService> = start_searcher_service(
        &config,
        metastore.clone(),
        storage_resolver.clone(),
        cluster.clone(),
    )
    .await?;

    // Start IndexManagementService (future ControlPlane).
    let index_management_service: Option<Mailbox<IndexManagementService>> =
        if services.contains(&QuickwitService::ControlPlane) {
            // start_control_plane_service that returns index service mailbox is confusing.
            let index_management_service_mailbox = start_control_plane_service(
                &universe,
                metastore.clone(),
                storage_resolver.clone(),
                config.default_index_root_uri(),
            )
            .await?;
            Some(index_management_service_mailbox)
        } else {
            None
        };

    // Starts gRPC server now as the index management client will make gRPC calls.
    // The indexer will need that to start.;
    let grpc_listen_addr = config.grpc_listen_addr().await?;
    let grpc_server = grpc::start_grpc_server(
        grpc_listen_addr,
        Some(search_service.clone()),
        index_management_service.clone(),
    );

    let grpc_server_handle = tokio::task::spawn(async move {
        let _ = grpc_server.await;
    });

    let index_management_client =
        IndexManagementClient::create_and_update_from_cluster(cluster.clone()).await?;

    let indexer_service: Option<Mailbox<IndexingService>> =
        if services.contains(&QuickwitService::Indexer) {
            let indexer_service = start_indexer_service(
                &universe,
                &config,
                metastore.clone(),
                index_management_client,
                storage_resolver.clone(),
                ingest_api_service.clone(),
            )
            .await?;
            Some(indexer_service)
        } else {
            None
        };

    let rest_listen_addr = config.rest_listen_addr().await?;

    let quickwit_services = QuickwitServices {
        config: Arc::new(config),
        build_info: Arc::new(build_quickwit_build_info()),
        cluster,
        ingest_api_service,
        search_service,
        indexer_service,
        index_management_service,
        services: services.clone(),
    };
    let rest_server = rest::start_rest_server(rest_listen_addr, &quickwit_services);

    tokio::try_join!(rest_server)?;
    Ok(())
}

/// Checks if the conditions required to smoothly run a Quickwit cluster are met.
/// Currently we don't allow cluster feature upon using:
/// - A FileBacked metastore
/// - A FileStorage
fn check_is_configured_for_cluster(
    peer_seeds: &[String],
    metastore_uri: &Uri,
    mut indexes: impl Iterator<Item = (String, Uri)>,
) -> anyhow::Result<()> {
    if peer_seeds.is_empty() {
        return Ok(());
    }
    if metastore_uri.protocol().is_file() || metastore_uri.protocol().is_s3() {
        anyhow::bail!(
            "Quickwit cannot run in cluster mode with a file-backed metastore. Please, use a \
             PostgreSQL metastore instead."
        );
    }
    if let Some((index_id, index_uri)) =
        indexes.find(|(_, index_uri)| index_uri.protocol().is_file())
    {
        anyhow::bail!(
            "Quickwit cannot run in cluster mode with an index whose data is stored on a local \
             file system. Index URI for index `{}` is `{}`.",
            index_id,
            index_uri,
        );
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct QuickwitBuildInfo {
    pub commit_version_tag: &'static str,
    pub cargo_pkg_version: &'static str,
    pub cargo_build_target: &'static str,
    pub commit_short_hash: &'static str,
    pub commit_date: &'static str,
    pub version: &'static str,
}

/// Builds QuickwitBuildInfo from env variables.
pub fn build_quickwit_build_info() -> QuickwitBuildInfo {
    let commit_version_tag = env!("QW_COMMIT_VERSION_TAG");
    let cargo_pkg_version = env!("CARGO_PKG_VERSION");
    let version = if commit_version_tag == "none" {
        // concat macro only accepts literals.
        concat!(env!("CARGO_PKG_VERSION"), "nightly")
    } else {
        cargo_pkg_version
    };
    QuickwitBuildInfo {
        commit_version_tag,
        cargo_pkg_version,
        cargo_build_target: env!("CARGO_BUILD_TARGET"),
        commit_short_hash: env!("QW_COMMIT_SHORT_HASH"),
        commit_date: env!("QW_COMMIT_DATE"),
        version,
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::ops::Range;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use quickwit_indexing::mock_split;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_proto::search_service_server::SearchServiceServer;
    use quickwit_proto::{tonic, OutputFormat};
    use quickwit_search::{
        root_search_stream, ClusterClient, MockSearchService, SearchClientPool, SearchError,
        SearchService,
    };
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::Server;

    use super::*;
    use crate::search_api::GrpcSearchAdapter;

    async fn start_test_server(
        address: SocketAddr,
        search_service: Arc<dyn SearchService>,
    ) -> anyhow::Result<()> {
        let search_grpc_adpater = GrpcSearchAdapter::from(search_service);
        tokio::spawn(async move {
            Server::builder()
                .add_service(SearchServiceServer::new(search_grpc_adpater))
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_serve_search_stream_with_a_leaf_error_on_leaf_node() -> anyhow::Result<()> {
        // This test aims at checking the client gRPC implementation.
        let request = quickwit_proto::SearchStreamRequest {
            index_id: "test-index".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            partition_by_field: None,
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split_1"), mock_split("split_2")])
            },
        );
        let mut mock_search_service = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        result_sender.send(Ok(quickwit_proto::LeafSearchStreamResponse {
            data: b"123".to_vec(),
            split_id: "split_1".to_string(),
        }))?;
        result_sender.send(Err(SearchError::InternalError(
            "Error on `split2`".to_string(),
        )))?;
        mock_search_service
            .expect_leaf_search_stream()
            .withf(|request| request.split_offsets.len() == 2) // First request.
            .return_once(
                |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                    Ok(UnboundedReceiverStream::new(result_receiver))
                },
            );
        mock_search_service
            .expect_leaf_search_stream()
            .withf(|request| request.split_offsets.len() == 1) // Retry request on the failing split.
            .return_once(
                |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                    Err(SearchError::InternalError(
                        "Error again on `split2`".to_string(),
                    ))
                },
            );
        // The test will hang on indefinitely if we don't drop the sender.
        drop(result_sender);

        let grpc_addr: SocketAddr = "127.0.0.1:20000".parse()?;
        start_test_server(grpc_addr, Arc::new(mock_search_service)).await?;
        let client_pool = SearchClientPool::for_addrs(&[grpc_addr]).await?;
        let cluster_client = ClusterClient::new(client_pool.clone());
        let stream = root_search_stream(request, &metastore, cluster_client, &client_pool).await?;
        let result: Result<Vec<_>, SearchError> = stream.try_collect().await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Internal error: `Internal error: `Error again on `split2``.`."
        );
        Ok(())
    }

    #[test]
    fn test_check_is_configured_for_cluster_on_single_node() {
        check_is_configured_for_cluster(
            &[],
            &Uri::new("file://qwdata/indexes".to_string()),
            [(
                "foo-index".to_string(),
                Uri::new("file:///qwdata/indexes/foo-index".to_string()),
            )]
            .into_iter(),
        )
        .unwrap();
    }
}
