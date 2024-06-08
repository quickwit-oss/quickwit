// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use http::Method;
use quickwit_config::service::QuickwitService;
use quickwit_config::SearcherConfig;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::{
    ClusterClient, SearchJobPlacer, SearchService, SearchServiceClient, SearchServiceImpl,
    SearcherContext, SearcherPool,
};
use quickwit_serve::lambda_search_api::*;
use quickwit_storage::StorageResolver;
use quickwit_telemetry::payload::{QuickwitFeature, QuickwitTelemetryInfo, TelemetryEvent};
use tracing::{error, info};
use warp::filters::path::FullPath;
use warp::reject::Rejection;
use warp::Filter;

use crate::searcher::environment::CONFIGURATION_TEMPLATE;
use crate::utils::load_node_config;

async fn create_local_search_service(
    searcher_config: SearcherConfig,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> Arc<dyn SearchService> {
    let searcher_pool = SearcherPool::default();
    let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
    let cluster_client = ClusterClient::new(search_job_placer);
    // TODO configure split cache
    let searcher_context = Arc::new(SearcherContext::new(searcher_config, None));
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_resolver,
        cluster_client.clone(),
        searcher_context.clone(),
    ));
    // Add search service to pool to avoid "no available searcher nodes in the pool" error
    let socket_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 7280u16);
    let search_service_client =
        SearchServiceClient::from_service(search_service.clone(), socket_addr);
    searcher_pool.insert(socket_addr, search_service_client);
    search_service
}

fn native_api(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_get_handler(search_service.clone()).or(search_post_handler(search_service))
}

fn es_compat_api(
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    es_compat_search_handler(search_service.clone())
        .or(es_compat_index_search_handler(search_service.clone()))
        .or(es_compat_index_count_handler(search_service.clone()))
        .or(es_compat_scroll_handler(search_service.clone()))
        .or(es_compat_index_multi_search_handler(search_service.clone()))
        .or(es_compat_index_field_capabilities_handler(
            search_service.clone(),
        ))
        .or(es_compat_index_stats_handler(metastore.clone()))
        .or(es_compat_stats_handler(metastore.clone()))
        .or(es_compat_index_cat_indices_handler(metastore.clone()))
        .or(es_compat_cat_indices_handler(metastore.clone()))
}

fn index_api(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    get_index_metadata_handler(metastore)
}

fn v1_searcher_api(
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("api" / "v1" / ..)
        .and(
            native_api(search_service.clone())
                .or(es_compat_api(search_service, metastore.clone()))
                .or(index_api(metastore)),
        )
        .with(warp::filters::compression::gzip())
        .recover(|rejection| {
            error!(?rejection, "request rejected");
            recover_fn(rejection)
        })
}

pub async fn setup_searcher_api(
) -> anyhow::Result<impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone> {
    let (node_config, storage_resolver, metastore) =
        load_node_config(CONFIGURATION_TEMPLATE).await?;

    let telemetry_info = QuickwitTelemetryInfo::new(
        HashSet::from_iter([QuickwitService::Searcher.as_str().to_string()]),
        HashSet::from_iter([QuickwitFeature::AwsLambda]),
    );
    let _telemetry_handle_opt = quickwit_telemetry::start_telemetry_loop(telemetry_info);

    let search_service = create_local_search_service(
        node_config.searcher_config,
        metastore.clone(),
        storage_resolver,
    )
    .await;

    let before_hook = warp::path::full()
        .and(warp::method())
        .and_then(|route: FullPath, method: Method| async move {
            info!(
                method = method.as_str(),
                route = route.as_str(),
                "new request"
            );
            quickwit_telemetry::send_telemetry_event(TelemetryEvent::RunCommand).await;
            Ok::<_, std::convert::Infallible>(())
        })
        .untuple_one();

    let after_hook = warp::log::custom(|info| {
        info!(status = info.status().as_str(), "request completed");
    });

    let api = warp::any()
        .and(before_hook)
        .and(v1_searcher_api(search_service, metastore))
        .with(after_hook);

    Ok(api)
}
