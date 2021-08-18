/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

mod args;
mod error;
mod grpc;
mod grpc_adapter;
mod http_handler;
mod quickwit_cache;
mod rest;

use quickwit_cache::QuickwitCache;
use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use termcolor::{self, Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tracing::debug;

use quickwit_cluster::cluster::{read_host_key, Cluster};
use quickwit_cluster::service::ClusterServiceImpl;
use quickwit_metastore::{Metastore, MetastoreUriResolver};
use quickwit_search::{
    http_addr_to_grpc_addr, http_addr_to_swim_addr, SearchClientPool, SearchServiceImpl,
};
use quickwit_storage::{
    localstack_region, LocalFileStorageFactory, S3CompatibleObjectStorageFactory,
    StorageUriResolver, StorageWithCacheFactory,
};
use quickwit_telemetry::payload::{ServeEvent, TelemetryEvent};

pub use crate::args::ServeArgs;
pub use crate::error::ApiError;
use crate::grpc::start_grpc_service;
use crate::grpc_adapter::cluster_adapter::GrpcClusterAdapter;
use crate::grpc_adapter::search_adapter::GrpcSearchAdapter;

use crate::rest::start_rest_service;

async fn get_from_cache_or_create_metastore(
    metastore_cache: &mut HashMap<String, Arc<dyn Metastore>>,
    metastore_uri_resolver: &MetastoreUriResolver,
    metastore_uri: &str,
) -> anyhow::Result<Arc<dyn Metastore>> {
    if let Some(metastore_in_cache) = metastore_cache.get(metastore_uri) {
        return Ok(metastore_in_cache.clone());
    }
    let metastore = metastore_uri_resolver.resolve(metastore_uri).await?;
    metastore_cache.insert(metastore_uri.to_string(), metastore.clone());
    Ok(metastore)
}

async fn create_index_to_metastore_router(
    metastore_resolver: &MetastoreUriResolver,
    index_uris: &[String],
) -> anyhow::Result<HashMap<String, Arc<dyn Metastore>>> {
    let mut index_to_metastore_router = HashMap::default();
    let mut metastore_cache: HashMap<String, Arc<dyn Metastore>> = HashMap::default();
    for index_uri in index_uris {
        let (metastore_uri, index_id) =
            quickwit_common::extract_metastore_uri_and_index_id_from_index_uri(index_uri)?;
        if index_to_metastore_router.contains_key(index_id) {
            anyhow::bail!("Index id `{}` appears twice.", index_id);
        }
        let metastore = get_from_cache_or_create_metastore(
            &mut metastore_cache,
            metastore_resolver,
            metastore_uri,
        )
        .await?;
        metastore
            .list_all_splits(index_id)
            .await
            .with_context(|| format!("Index `{}` was not found", index_uri))?;
        index_to_metastore_router.insert(index_id.to_string(), metastore.clone());
    }
    Ok(index_to_metastore_router)
}

fn display_help_message(
    rest_socket_addr: SocketAddr,
    example_index_name: &str,
) -> anyhow::Result<()> {
    // No-color if we are not in a terminal.
    let mut stdout = StandardStream::stdout(ColorChoice::Auto);
    write!(&mut stdout, "Server started on ")?;
    stdout.set_color(ColorSpec::new().set_fg(Some(Color::Green)))?;
    writeln!(&mut stdout, "http://{}/", &rest_socket_addr)?;
    stdout.set_color(&ColorSpec::new())?;
    writeln!(
        &mut stdout,
        "\nYou can test it using the following command:"
    )?;
    stdout.set_color(ColorSpec::new().set_fg(Some(Color::Blue)))?;
    writeln!(
        &mut stdout,
        "curl 'http://{}/api/v1/{}/search?query=my+query'",
        rest_socket_addr, example_index_name
    )?;
    stdout.set_color(&ColorSpec::new())?;
    // TODO add link to the documentation of the query language.
    Ok(())
}

/// Builds a storage uri resolver that handles
/// - s3:// uris. This storage comes with a cache that stores hotcache files.
/// - s3+localstack://
/// - file:// uris.
fn storage_uri_resolver() -> StorageUriResolver {
    let s3_storage = StorageWithCacheFactory::new(
        Arc::new(S3CompatibleObjectStorageFactory::default()),
        Arc::new(QuickwitCache::default()),
    );
    StorageUriResolver::builder()
        .register(LocalFileStorageFactory::default())
        .register(s3_storage)
        .register(S3CompatibleObjectStorageFactory::new(
            localstack_region(),
            "s3+localstack",
        ))
        .build()
}

/// Start Quickwit search node.
pub async fn serve_cli(args: ServeArgs) -> anyhow::Result<()> {
    debug!(args=?args, "serve-cli");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Serve(ServeEvent {
        has_seed: !args.peer_socket_addrs.is_empty(),
    }))
    .await;
    let storage_resolver = storage_uri_resolver();
    let metastore_resolver = MetastoreUriResolver::with_storage_resolver(storage_resolver.clone());
    let metastore_router =
        create_index_to_metastore_router(&metastore_resolver, &args.index_uris).await?;
    let example_index_name = metastore_router
        .keys()
        .next()
        .with_context(|| "No index available.")?
        .to_string();

    let host_key = read_host_key(args.host_key_path.as_path())?;
    let swim_addr = http_addr_to_swim_addr(args.rest_socket_addr);
    let cluster = Arc::new(Cluster::new(host_key, swim_addr)?);
    for peer_socket_addr in args
        .peer_socket_addrs
        .iter()
        .filter(|peer_rest_addr| peer_rest_addr != &&args.rest_socket_addr)
    {
        // If the peer address is specified,
        // it joins the cluster in which that node participates.
        let peer_swim_addr = http_addr_to_swim_addr(*peer_socket_addr);
        debug!(peer_swim_addr=?peer_swim_addr, "Add peer node.");
        cluster.add_peer_node(peer_swim_addr);
    }

    let client_pool = Arc::new(SearchClientPool::new(cluster.clone()).await?);

    let search_service = Arc::new(SearchServiceImpl::new(
        metastore_router,
        storage_resolver,
        client_pool,
    ));

    let cluster_service = Arc::new(ClusterServiceImpl::new(cluster.clone()));

    let grpc_socket_addr = http_addr_to_grpc_addr(args.rest_socket_addr);
    let grpc_search_service = GrpcSearchAdapter::from(search_service.clone());
    let grpc_cluster_service = GrpcClusterAdapter::from(cluster_service.clone());
    let grpc_server =
        start_grpc_service(grpc_socket_addr, grpc_search_service, grpc_cluster_service);

    let rest_server = start_rest_service(args.rest_socket_addr, search_service, cluster_service);

    display_help_message(args.rest_socket_addr, &example_index_name)?;

    tokio::try_join!(rest_server, grpc_server)?;

    Ok(())
}
