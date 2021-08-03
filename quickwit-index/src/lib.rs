// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::path::Path;
use std::time::Duration;

use quickwit_actors::AsyncActor;
use quickwit_actors::KillSwitch;
use quickwit_actors::QueueCapacity;
use quickwit_actors::SyncActor;
use tantivy::IndexReaderBuilder;

use crate::actors::Indexer;
use crate::actors::Publisher;
use crate::actors::Uploader;
use crate::actors::source::FileSource;
use crate::models::RawDocBatch;

pub mod actors;
pub mod models;

const COMMIT_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn run_indexing() -> anyhow::Result<()> {
    let kill_switch = KillSwitch::default();
    let publisher = Publisher;
    let publisher_handler = publisher.spawn(QueueCapacity::Bounded(3), kill_switch.clone());
    let uploader = Uploader;
    let uploader_handler = uploader.spawn(QueueCapacity::Bounded(3), kill_switch.clone());
    let indexer = Indexer::try_new(COMMIT_TIMEOUT)?;
    let indexer_handler = indexer.spawn(QueueCapacity::Bounded(10), kill_switch.clone());
    let source = FileSource::try_new(Path::new("data/test_corpus.json"), indexer_handler.mailbox().clone()).await?;
    let source = source.spawn(QueueCapacity::Bounded(1), kill_switch.clone());
    Ok(())
}

fn main() {}
