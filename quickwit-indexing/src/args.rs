// Copyright (C) 2021 Quickwit, Inc.
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

use std::net::SocketAddr;
use std::path::PathBuf;

use byte_unit::Byte;

#[derive(Debug, PartialEq)]
pub struct ServeIndexingArgs {
    /// Socket address of the REST server.
    pub rest_socket_addr: SocketAddr,
        
    /// Metastore URI.
    pub metastore_uri: String,
    
    /// Index IDs for which indexers will be spawns
    // TODO: remove this args once we can get index IDs from the
    // metastore or from the source config storage.  
    pub index_ids: Vec<String>,

    /// Indexes sources config URI.
    pub source_config_uri: String,

    /// Indexer heap size.
    pub heap_size: Byte,

    /// Scratch dir.
    pub temp_dir: Option<PathBuf>,
}
