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

use crate::cli_command::IndexDataArgs;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::{self, BufReader};

async fn read_documents(input_uri: Option<PathBuf>) -> anyhow::Result<()> {
    match input_uri {
        Some(path_buf) => {
            let file = File::open(path_buf).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            while let Some(line) = lines.next_line().await? {
                println!("{}", line);
            }
        }
        None => {
            let file = io::stdin();
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            while let Some(line) = lines.next_line().await? {
                println!("{}", line);
            }
        }
    };
    Ok(())
}

pub async fn index_data(args: IndexDataArgs) -> anyhow::Result<()> {
    if args.input_uri.is_none() {
        println!("Please enter your new line delimited json documents.");
    }

    read_documents(args.input_uri).await?;

    Ok(())
}
