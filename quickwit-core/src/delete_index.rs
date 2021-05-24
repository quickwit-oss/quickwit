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

use crate::DeleteIndexArgs;

pub async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        dry_run = args.dry_run,
        "delete-index"
    );

    let index_uri = args.index_uri.to_string_lossy().to_string();

    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;
    metastore.delete_index(index_uri).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_delete_index_cli() -> anyhow::Result<()> {
        Ok(())
    }
}
