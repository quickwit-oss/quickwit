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

use quickwit_doc_mapping::DocMapping;
use quickwit_metastore::{Metastore, MetastoreUriResolver, SplitState};
use quickwit_storage::Storage;

type IndexUri = String;

// anyhow errors are fine for now but we'll want to move to a proper error type eventually.
pub async fn create_index(index_uri: IndexUri, doc_mapping: DocMapping) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;
    metastore.create_index(index_uri, doc_mapping).await?;
    Ok(())
}

// TODO
pub async fn search_index(index_uri: IndexUri) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;
    let _splits = metastore
        .list_splits(index_uri, SplitState::Published, None)
        .await?;
    Ok(())
}

pub async fn delete_index(index_uri: IndexUri) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;
    metastore.delete_index(index_uri).await?;
    Ok(())
}

// TODO
pub async fn garbage_collect(
    _index_uri: IndexUri,
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
