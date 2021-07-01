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
mod grpc_adapter;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
pub use args::ServeArgs;
use async_trait::async_trait;
use bytes::Bytes;
use quickwit_directories::HOTCACHE_FILENAME;
use quickwit_metastore::{Metastore, MetastoreUriResolver};
use quickwit_search::SearchServiceImpl;
use quickwit_storage::{
    localstack_region, Cache, LocalFileStorageFactory, S3CompatibleObjectStorageFactory,
    SliceCache, StorageUriResolver, StorageWithCacheFactory,
};
use quickwit_telemetry::payload::{ServeEvent, TelemetryEvent};
use tracing::debug;
mod error;
mod rest;
use std::io::Write;
use termcolor::WriteColor;
use termcolor::{self, Color, ColorChoice, ColorSpec, StandardStream};

const FULL_SLICE: Range<usize> = 0..usize::MAX;

/// Hotcache cache capacity is hardcoded to 500 MB.
/// Once the capacity is reached, a LRU strategy is used.
const HOTCACHE_CACHE_CAPACITY: usize = 500_000_000;

pub use crate::error::ApiError;

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
            &metastore_resolver,
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
    stdout.set_color(&ColorSpec::new().set_fg(Some(Color::Green)))?;
    writeln!(&mut stdout, "http://{}/", &rest_socket_addr)?;
    stdout.set_color(&ColorSpec::new())?;
    writeln!(
        &mut stdout,
        "\nYou can test it using the following command:"
    )?;
    stdout.set_color(&ColorSpec::new().set_fg(Some(Color::Blue)))?;
    writeln!(
        &mut stdout,
        "curl http://{}/api/v1/{}/search?query=my+query",
        rest_socket_addr, example_index_name
    )?;
    stdout.set_color(&ColorSpec::new())?;
    // TODO add link to the documentation of the query language.
    Ok(())
}

/// The Quickwit cache logic is very simple for the moment.
///
/// It stores hotcache files using an LRU cache.
///
/// HACK! We use `0..usize::MAX` to signify the "entire file".
/// TODO fixme
struct SimpleCache {
    slice_cache: SliceCache,
}

impl SimpleCache {
    fn with_capacity_in_bytes(capacity_in_bytes: usize) -> Self {
        SimpleCache {
            slice_cache: SliceCache::with_capacity_in_bytes(capacity_in_bytes),
        }
    }
}

#[async_trait]
impl Cache for SimpleCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<Bytes> {
        if let Some(bytes) = self.get_all(path).await {
            return Some(bytes.slice(byte_range.clone()));
        }
        if let Some(bytes) = self.slice_cache.get(path, byte_range) {
            return Some(bytes);
        }
        None
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: Bytes) {
        self.slice_cache.put(path, byte_range, bytes);
    }

    async fn get_all(&self, path: &Path) -> Option<Bytes> {
        self.slice_cache.get(path, FULL_SLICE.clone())
    }

    async fn put_all(&self, path: PathBuf, bytes: Bytes) {
        self.slice_cache.put(path, FULL_SLICE.clone(), bytes);
    }
}

pub struct QuickwitCache {
    router: Vec<(&'static str, Arc<dyn Cache>)>,
}

impl Default for QuickwitCache {
    fn default() -> Self {
        QuickwitCache {
            router: vec![(
                HOTCACHE_FILENAME,
                Arc::new(SimpleCache::with_capacity_in_bytes(HOTCACHE_CACHE_CAPACITY)),
            )],
        }
    }
}

impl QuickwitCache {
    fn get_relevant_cache(&self, path: &Path) -> Option<&dyn Cache> {
        for (suffix, cache) in &self.router {
            if path.ends_with(suffix) {
                return Some(cache.as_ref());
            }
        }
        None
    }
}

#[async_trait]
impl Cache for QuickwitCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<Bytes> {
        if let Some(cache) = self.get_relevant_cache(path) {
            return cache.get(path, byte_range).await;
        }
        None
    }

    async fn get_all(&self, path: &Path) -> Option<Bytes> {
        if let Some(cache) = self.get_relevant_cache(path) {
            return cache.get_all(path).await;
        }
        None
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: Bytes) {
        if let Some(cache) = self.get_relevant_cache(&path) {
            cache.put(path, byte_range, bytes).await;
        }
    }

    async fn put_all(&self, path: PathBuf, bytes: Bytes) {
        if let Some(cache) = self.get_relevant_cache(&path) {
            cache.put(path, FULL_SLICE, bytes).await;
        }
    }
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

pub async fn serve_cli(args: ServeArgs) -> anyhow::Result<()> {
    debug!(args=?args, "serve-cli");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Serve(ServeEvent {
        has_seed: !args.peers.is_empty(),
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
    let search_service_impl = SearchServiceImpl::new(metastore_router, storage_resolver);
    let rest_routes = rest::search_handler(Arc::new(search_service_impl));
    display_help_message(args.rest_socket_addr, &example_index_name)?;
    warp::serve(rest_routes).run(args.rest_socket_addr).await;
    Ok(())
}
