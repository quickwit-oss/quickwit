// Copyright (C) 2021 Quickwit, Inc.
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
mod push_api;
mod search_api;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::bail;
use format::Format;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::ClusterService;
use quickwit_config::QuickwitConfig;
use quickwit_core::IndexService;
use quickwit_indexing::actors::IndexingServer;
use quickwit_indexing::start_indexer_service;
use quickwit_metastore::quickwit_metastore_uri_resolver;
use quickwit_pushapi::{init_push_api, OltpService, PushApiService};
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

#[derive(Debug, PartialEq, Eq, Copy, Hash, Clone)]
pub enum QuickwitService {
    Indexer,
    Searcher,
}

impl TryFrom<&str> for QuickwitService {
    type Error = anyhow::Error;

    fn try_from(service_str: &str) -> Result<Self, Self::Error> {
        match service_str {
            "indexer" => Ok(QuickwitService::Indexer),
            "searcher" => Ok(QuickwitService::Searcher),
            _ => {
                bail!("Service `{service_str}` unknown");
            }
        }
    }
}
struct QuickwitServices {
    pub cluster_service: Arc<dyn ClusterService>,
    pub search_service: Option<Arc<dyn SearchService>>,
    pub oltp_service: Option<OltpService>,
    pub indexer_service: Option<Mailbox<IndexingServer>>,
    pub push_api_service: Option<Mailbox<PushApiService>>,
    pub index_service: Arc<IndexService>,
}

pub async fn serve_quickwit(
    config: &QuickwitConfig,
    services: &HashSet<QuickwitService>,
) -> anyhow::Result<()> {
    let metastore = quickwit_metastore_uri_resolver()
        .resolve(&config.metastore_uri())
        .await?;
    let storage_resolver = quickwit_storage_uri_resolver().clone();

    let cluster_service = quickwit_cluster::start_cluster_service(config).await?;

    let universe = Universe::new();

    let push_api_service: Option<Mailbox<PushApiService>> =
        if services.contains(&QuickwitService::Indexer) {
            let push_api_service = init_push_api(
                &universe,
                &config.data_dir_path.join("queues"),
                metastore.clone(),
            )?;
            Some(push_api_service)
        } else {
            None
        };

    let indexer_service: Option<Mailbox<IndexingServer>> =
        if services.contains(&QuickwitService::Indexer) {
            let indexer_service = start_indexer_service(
                &universe,
                config,
                metastore.clone(),
                storage_resolver.clone(),
            )
            .await?;
            Some(indexer_service)
        } else {
            None
        };

    let search_service: Option<Arc<dyn SearchService>> =
        if services.contains(&QuickwitService::Searcher) {
            let search_service = start_searcher_service(
                config,
                metastore.clone(),
                storage_resolver.clone(),
                cluster_service.clone(),
            )
            .await?;
            Some(search_service)
        } else {
            None
        };

    // Always instanciate index management service.
    let index_service = Arc::new(IndexService::new(
        metastore,
        storage_resolver,
        config.default_index_root_uri(),
    ));

    let quickwit_services = QuickwitServices {
        push_api_service,
        cluster_service,
        search_service,
        indexer_service,
        index_service,
        oltp_service: Some(OltpService::default()),
    };

    let rest_addr = config.rest_socket_addr()?;
    let grpc_addr = config.grpc_socket_addr()?;

    let grpc_server = grpc::start_grpc_server(grpc_addr, &quickwit_services);
    let rest_server = rest::start_rest_server(rest_addr, &quickwit_services);

    tokio::try_join!(rest_server, grpc_server)?;

    Ok(())
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
}
