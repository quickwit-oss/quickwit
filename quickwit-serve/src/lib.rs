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
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use termcolor::{self, Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tracing::debug;

use quickwit_cluster::cluster::{read_host_key, Cluster};
use quickwit_cluster::service::ClusterServiceImpl;
use quickwit_metastore::MetastoreUriResolver;
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
    let metastore_resolver = MetastoreUriResolver::default();
    let example_index_name = "my_index".to_string();
    let metastore = metastore_resolver.resolve(&args.metastore_uri).await?;

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
        cluster.add_peer_node(peer_swim_addr).await;
    }

    let client_pool = Arc::new(SearchClientPool::new(cluster.clone()).await?);

    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
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
