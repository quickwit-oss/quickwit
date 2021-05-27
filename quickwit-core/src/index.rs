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

use quickwit_doc_mapping::DocMapping;
use quickwit_metastore::MetastoreUriResolver;

// anyhow errors are fine for now but we'll want to move to a proper error type eventually.
pub async fn create_index(
    metastore_uri: &str,
    index_id: &str,
    doc_mapping: DocMapping,
) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default().resolve(&metastore_uri)?;
    metastore.create_index(index_id, doc_mapping).await?;
    Ok(())
}

// TODO
pub async fn index_data(_index_id: &str) -> anyhow::Result<()> {
    unimplemented!()
}

// TODO
pub async fn search_index(_index_id: &str) -> anyhow::Result<()> {
    unimplemented!()
}

// TODO
pub async fn delete_index(_metastore_uri: &str, _index_id: &str) -> anyhow::Result<()> {
    unimplemented!()
}
