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
mod counters;
mod error;
mod format;

mod grpc;
mod rest;

mod cluster_api;
mod health_check_api;
mod index_api;
mod indexing_api;
mod ingest_api;
mod search_api;
mod ui_handler;

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;

use format::Format;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::{ClusterService, QuickwitService};
use quickwit_common::uri::{Uri, FILE_PROTOCOL, S3_PROTOCOL};
use quickwit_config::QuickwitConfig;
use quickwit_core::IndexService;
use quickwit_indexing::actors::IndexingService;
use quickwit_indexing::start_indexer_service;
use quickwit_ingest_api::{init_ingest_api, IngestApiService};
use quickwit_metastore::{quickwit_metastore_uri_resolver, Metastore};
use quickwit_search::{start_searcher_service, SearchService};
use quickwit_storage::quickwit_storage_uri_resolver;
use warp::{Filter, Rejection};

pub use crate::args::ServeArgs;
pub use crate::counters::COUNTERS;
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
    pub cluster_service: Arc<dyn ClusterService>,
    /// We do have a search service even on nodes that are not running `search`.
    /// It is only used to serve the rest API calls and will only execute
    /// the root requests.
    pub search_service: Arc<dyn SearchService>,
    pub indexer_service: Option<Mailbox<IndexingService>>,
    pub ingest_api_service: Option<Mailbox<IngestApiService>>,
    pub index_service: Arc<IndexService>,
    pub services: HashSet<QuickwitService>,
}

pub async fn serve_quickwit(
    config: &QuickwitConfig,
    services: &HashSet<QuickwitService>,
) -> anyhow::Result<()> {
    let metastore = quickwit_metastore_uri_resolver()
        .resolve(&config.metastore_uri())
        .await?;
    check_is_configured_for_cluster(config, metastore.clone()).await?;
    let storage_resolver = quickwit_storage_uri_resolver().clone();

    let cluster_service = quickwit_cluster::start_cluster_service(config, services).await?;

    let universe = Universe::new();

    let ingest_api_service: Option<Mailbox<IngestApiService>> = if services
        .contains(&QuickwitService::Indexer)
    {
        let ingest_api_service = init_ingest_api(&universe, &config.data_dir_path.join("queues"))?;
        Some(ingest_api_service)
    } else {
        None
    };

    let indexer_service: Option<Mailbox<IndexingService>> =
        if services.contains(&QuickwitService::Indexer) {
            let indexer_service = start_indexer_service(
                &universe,
                config,
                metastore.clone(),
                storage_resolver.clone(),
                ingest_api_service.clone(),
            )
            .await?;
            Some(indexer_service)
        } else {
            None
        };

    let search_service: Arc<dyn SearchService> = start_searcher_service(
        config,
        metastore.clone(),
        storage_resolver.clone(),
        cluster_service.clone(),
    )
    .await?;

    // Always instanciate index management service.
    let index_service = Arc::new(IndexService::new(
        metastore,
        storage_resolver,
        config.default_index_root_uri(),
    ));
    let quickwit_services = QuickwitServices {
        ingest_api_service,
        cluster_service,
        search_service,
        indexer_service,
        index_service,
        services: services.clone(),
    };
    let grpc_listen_addr = config.grpc_listen_addr().await?;
    let grpc_server = grpc::start_grpc_server(grpc_listen_addr, &quickwit_services);

    let rest_listen_addr = config.rest_listen_addr().await?;
    let rest_server = rest::start_rest_server(rest_listen_addr, &quickwit_services);

    tokio::try_join!(grpc_server, rest_server)?;
    Ok(())
}

/// Checks if the conditions required to smoothly run a Quickwit cluster are met.
/// Currently we don't allow cluster feature upon using:
/// - A FileBacked metastore
/// - A FileStorage
async fn check_is_configured_for_cluster(
    config: &QuickwitConfig,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<()> {
    if config.peer_seeds.is_empty() {
        return Ok(());
    }
    let metastore_uri = Uri::try_new(&metastore.uri())?;
    if metastore_uri.protocol() == FILE_PROTOCOL || metastore_uri.protocol() == S3_PROTOCOL {
        anyhow::bail!(
            "Quickwit cannot run in cluster mode with a file-backed metastore. Please, use a \
             PostgreSQL metastore instead."
        );
    }
    for index_metadata in metastore.list_indexes_metadatas().await? {
        let index_uri = Uri::try_new(&index_metadata.index_uri)?;
        if index_uri.protocol() == FILE_PROTOCOL {
            anyhow::bail!(
                "Quickwit cannot run in cluster mode with an index whose data is stored on a \
                 local file system. Index URI for index `{}` is `{}`.",
                index_metadata.index_id,
                index_uri
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::ops::Range;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use quickwit_config::QuickwitConfig;
    use quickwit_indexing::mock_split;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_proto::search_service_server::SearchServiceServer;
    use quickwit_proto::{tonic, OutputFormat};
    use quickwit_search::{
        root_search_stream, ClusterClient, MockSearchService, SearchClientPool, SearchError,
        SearchService,
    };
    use rand::seq::SliceRandom;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::Server;

    use crate::check_is_configured_for_cluster;
    use crate::search_api::GrpcSearchAdapter;

    async fn start_test_server(
        address: SocketAddr,
        search_service: Arc<dyn SearchService>,
    ) -> anyhow::Result<()> {
        let search_grpc_adpater = GrpcSearchAdapter::from(search_service);
        let _ = tokio::spawn(async move {
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
        // This test aims at checking the client grpc implementation.
        let request = quickwit_proto::SearchStreamRequest {
            index_id: "test-idx".to_string(),
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
                    "test-idx",
                    "file:///path/to/index/test-idx",
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

        let grpc_addr: SocketAddr = format!("127.0.0.1:{}", 10000).parse()?;
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

    #[tokio::test]
    async fn test_check_is_configured_for_cluster_on_single_node() {
        let mut mock_metastore = MockMetastore::new();
        mock_metastore
            .expect_uri()
            .returning(|| "file:///path/to/metastore".to_string());
        let metastore = Arc::new(mock_metastore);

        let config = QuickwitConfig::default();
        assert!(matches!(
            check_is_configured_for_cluster(&config, metastore.clone()).await,
            Ok(())
        ));
    }

    #[tokio::test]
    async fn test_check_is_configured_for_cluster_on_file_backed_metastore() {
        let mut mock_metastore = MockMetastore::new();
        mock_metastore.expect_uri().returning(|| {
            let uris = vec!["file:///path/to/metastore", "s3:///path/to/metastore"];
            uris.choose(&mut rand::thread_rng()).unwrap().to_string()
        });
        let metastore = Arc::new(mock_metastore);

        let mut config = QuickwitConfig::default();
        config.peer_seeds = vec!["127.0.0.1:1234".to_string()];
        assert!(matches!(
            check_is_configured_for_cluster(&config, metastore).await,
            Err(..)
        ));
    }

    #[tokio::test]
    async fn test_check_is_configured_for_cluster_on_file_storage() {
        let mut mock_metastore = MockMetastore::new();
        mock_metastore
            .expect_uri()
            .returning(|| "postgres:///host/to/metastore".to_string());
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|| {
                Ok(vec![IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                )])
            });
        let metastore = Arc::new(mock_metastore);

        let mut config = QuickwitConfig::default();
        config.peer_seeds = vec!["127.0.0.1:1234".to_string()];
        assert!(matches!(
            check_is_configured_for_cluster(&config, metastore).await,
            Err(..)
        ));
    }

    #[tokio::test]
    async fn test_check_is_configured_for_cluster_on_object_storage() {
        let mut mock_metastore = MockMetastore::new();
        mock_metastore
            .expect_uri()
            .returning(|| "postgres:///host/to/metastore".to_string());
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|| {
                Ok(vec![IndexMetadata::for_test(
                    "test-idx",
                    "s3:///path/to/index/test-idx",
                )])
            });
        let metastore = Arc::new(mock_metastore);

        let mut config = QuickwitConfig::default();
        config.peer_seeds = vec!["127.0.0.1:1234".to_string()];
        assert!(matches!(
            check_is_configured_for_cluster(&config, metastore).await,
            Ok(())
        ));
    }
}
