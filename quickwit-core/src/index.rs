/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::sync::Arc;

use quickwit_metastore::{IndexMetadata, Metastore, MetastoreUriResolver, SplitState};
use quickwit_storage::Storage;

/// Creates an index at `index-path` extracted from `metastore_uri`. The command fails if an index
/// already exists at `index-path`.
///
/// * `metastore_uri` - The metastore Uri for accessing the metastore.
/// * `index_metadata` - The metadata used to create the target index.
///
pub async fn create_index(
    metastore_uri: &str,
    index_metadata: IndexMetadata,
) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default()
        .resolve(&metastore_uri)
        .await?;
    metastore.create_index(index_metadata).await?;
    Ok(())
}

/// Searches the index with `index_id` and returns the documents matching the query query.
/// The offset of the first hit returned and the number of hits returned can be set with the `start-offset`
/// and max-hits options.
/// By default, the search fields  are those specified at index creation unless restricted to `target-fields`.
///
/// TODO: interface does not currently match the docs.
///
pub async fn search_index(metastore_uri: &str, index_id: &str) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default()
        .resolve(&metastore_uri)
        .await?;
    let _splits = metastore
        .list_splits(index_id, SplitState::Published, None)
        .await?;
    Ok(())
}

/// Deletes the index specified with `index_id`.
/// This is equivalent to running `rm -rf <index path>` for a local index or
/// `aws s3 rm --recursive <index path>` for a remote Amazon S3 index.
///
/// * `metastore_uri` - The metastore Uri for accessing the metastore.
/// * `index_id` - The target index Id.
///
pub async fn delete_index(metastore_uri: &str, index_id: &str) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default()
        .resolve(&metastore_uri)
        .await?;
    metastore.delete_index(index_id).await?;
    Ok(())
}

/// Removes all danglings files from an index specified at `index_uri`.
/// It should leave the index  and its metastore in good state.
///
/// * `index_uri` - The target index uri.
/// * `storage` - The storage object.
/// * `metastore` - The metastore object.
///
pub async fn garbage_collect(
    _index_uri: &str,
    _storage: Arc<dyn Storage>,
    _metastore: Arc<dyn Metastore>,
) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_create_index() -> anyhow::Result<()> {
        Ok(())
    }

    #[test]
    fn test_index_data() -> anyhow::Result<()> {
        Ok(())
    }

    #[test]
    fn test_search_index() -> anyhow::Result<()> {
        Ok(())
    }

    #[test]
    fn test_delete_index() -> anyhow::Result<()> {
        Ok(())
    }
}
