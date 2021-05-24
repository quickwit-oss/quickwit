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
use futures::try_join;
use tokio::sync::mpsc::channel;

use crate::indexing::document_indexer::index_documents;
use crate::indexing::document_retriever::retrieve_documents;
use crate::indexing::split_deployer::deploy_splits;
use crate::indexing::statistics::StatisticsCollector;

static DOCUMENT_CHANNEL_SIZE: usize = 1000;

pub async fn index_data(args: IndexDataArgs) -> anyhow::Result<()> {
    if args.overwrite {
        reset_index(args.clone()).await?;
    }

    if args.input_uri.is_none() {
        println!("Please enter your new line delimited json documents.");
    }

    let mut statistic_collector = StatisticsCollector::new();
    let statistic_sender = statistic_collector.start_collection();
    let (document_sender, document_receiver) = channel::<String>(DOCUMENT_CHANNEL_SIZE);
    let retrieve_fut = retrieve_documents(args.input_uri.clone(), document_sender);
    let index_fut = index_documents(&args, document_receiver, statistic_sender.clone());
    let (_, splits) = try_join!(retrieve_fut, index_fut)?;

    // TODO: this is a little bit subobtimal, we might want to deploy progressively the splits once they are ready
    let searchable_splits = deploy_splits(splits, statistic_sender.clone()).await?;
    statistic_collector.display_report();

    Ok(())
}

async fn reset_index(_args: IndexDataArgs) -> anyhow::Result<()> {
    //TODO: list splits via metastore

    //TODO: delete all splits via metastore

    Ok(())
}
