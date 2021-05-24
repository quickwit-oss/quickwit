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

use quickwit_metastore::{MetastoreUriResolver, SplitState};
use std::path::PathBuf;
use tracing::debug;

use crate::IndexDataArgs;

pub async fn index_data_cli(args: IndexDataArgs) -> anyhow::Result<()> {
    debug!(
        index_uri =% args.index_uri.display(),
        input_uri =% args.input_uri.unwrap_or_else(|| PathBuf::from("stdin")).display(),
        temp_dir =% args.temp_dir.display(),
        num_threads = args.num_threads,
        heap_size = args.heap_size,
        overwrite = args.overwrite,
        "indexing"
    );

    let index_uri = args.index_uri.to_string_lossy().to_string();

    let metastore = MetastoreUriResolver::default().resolve(&index_uri)?;
    let _splits = metastore
        .list_splits(index_uri, SplitState::Published, None)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_index_data_cli() -> anyhow::Result<()> {
        Ok(())
    }
}
