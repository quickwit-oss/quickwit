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

use quickwit_metastore::MetastoreUriResolver;
use tracing::debug;

use quickwit_doc_mapping::DocMapping;

use crate::CreateIndexArgs;

pub async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        timestamp_field =? args.timestamp_field,
        overwrite = args.overwrite,
        "create-index"
    );
    let index_uri = args.index_uri.to_string_lossy().to_string();
    let doc_mapping = DocMapping::Dynamic;

    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;
    if args.overwrite {
        metastore.delete_index(index_uri.clone()).await?;
    }

    metastore.create_index(index_uri, doc_mapping).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_create_index_cli() -> anyhow::Result<()> {
        Ok(())
    }
}
