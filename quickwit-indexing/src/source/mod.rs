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

//! # Sources
//!
//! Quickwit gets its data from so-called `Sources`.
//!
//! The role of a source is to push message to an indexer mailbox.
//! Implementers need to focus on the implementation of the [`Source`] trait
//! and in particular its emit_batches method.
//! In addition, they need to implement a source factory.
//!
//! The source trait will executed in an actor.
//!
//! # Checkpoints and exactly-once semantics
//!
//! Quickwit is designed to offer exactly-once semantics whenever possible using the following
//! strategy, using checkpoints.
//!
//! Messages are split into partitions, and within a partition messages are totally ordered: they
//! are marked by a unique position within this partition.
//!
//! Sources are required to emit messages in a way that respects this partial order.
//! If two message belong 2 different partitions, they can be emitted in any order.
//! If two message belong to the same partition, they  are required to be emitted in the order of
//! their position.
//!
//! The set of documents processed by a source can then be expressed entirely as Checkpoint, that is
//! simply a mapping `(PartitionId -> Position)`.
//!
//! This checkpoint is used in Quickwit to implement exactly-once semantics.
//! When a new split is published, it is atomically published with an update of the last indexed
//! checkpoint.
//!
//! If the indexing pipeline is restarted, the source will simply be recreated with that checkpoint.
//!
//! # Example sources
//!
//! Right now two sources are implemented in quickwit.
//! - the file source: there partition here is a filepath, and the position is a byte-offset within
//!   that file.
//! - the kafka source: the partition id is a kafka topic partition id, and the position is a kafka
//!   offset.
mod file_source;
#[cfg(feature = "kafka")]
mod kafka_source;
#[cfg(feature = "kinesis")]
mod kinesis;
mod source_factory;
mod vec_source;
mod void_source;

use std::fmt;
use std::path::Path;

use anyhow::bail;
use async_trait::async_trait;
pub use file_source::{FileSource, FileSourceFactory};
#[cfg(feature = "kafka")]
pub use kafka_source::{KafkaSource, KafkaSourceFactory};
use once_cell::sync::OnceCell;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, AsyncActor, Mailbox};
use quickwit_config::{SourceConfig, SourceParams};
pub use source_factory::{SourceFactory, SourceLoader, TypedSourceFactory};
pub use vec_source::{VecSource, VecSourceFactory};
pub use void_source::{VoidSource, VoidSourceFactory};

use crate::models::IndexerMessage;

/// Reserved source id used for the CLI ingest command.
pub const INGEST_SOURCE_ID: &str = ".cli-ingest-source";

pub type SourceContext = ActorContext<SourceActor>;

/// A Source is a trait that is mounted in a light wrapping Actor called `SourceActor`.
///
/// For this reason, its methods mimics those of Actor.
/// One key difference is the absence of messages.
///
/// The `SourceActor` implements a loop until emit_batches returns an
/// ActorExitStatus.
///
/// Conceptually, a source execution works as if it was a simple loop
/// as follow:
///
/// ```ignore
/// # fn whatever() -> anyhow::Result<()>
/// source.initialize(ctx)?;
/// let exit_status = loop {
///   if let Err(exit_status) = source.emit_batches()? {
///      break exit_status;
////  }
/// };
/// source.finalize(exit_status)?;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// This method will be called before any calls to `emit_batches`.
    async fn initialize(&mut self, _ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    /// Main part of the source implementation, `emit_batches` can emit 0..n batches.
    ///
    /// The `batch_sink` is a mailbox that has a bounded capacity.
    /// In that case, `batch_sink` will block.
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<IndexerMessage>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus>;

    /// Finalize is called once after the actor terminates.
    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// A name identifying the type of source.
    fn name(&self) -> String;

    /// Returns an observable_state for the actor.
    ///
    /// This object is simply a json object, and its content may vary depending on the
    /// source.
    fn observable_state(&self) -> serde_json::Value;
}

/// The SourceActor acts as a thin wrapper over a source trait object to execute
/// it as an `AsyncActor`.
///
/// It mostly takes care of running a loop calling `emit_batches(...)`.
pub struct SourceActor {
    pub source: Box<dyn Source>,
    pub batch_sink: Mailbox<IndexerMessage>,
}

/// The goal of this struct is simply to prevent the construction of a Loop object.
struct PrivateToken;

/// Message used for the SourceActor.
pub struct Loop(PrivateToken);

impl fmt::Debug for Loop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Loop").finish()
    }
}

impl Actor for SourceActor {
    type Message = Loop;
    type ObservableState = serde_json::Value;

    fn name(&self) -> String {
        self.source.name()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.source.observable_state()
    }
}

#[async_trait]
impl AsyncActor for SourceActor {
    async fn initialize(&mut self, ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        self.source.initialize(ctx).await?;
        self.process_message(Loop(PrivateToken), ctx).await?;
        Ok(())
    }

    async fn process_message(
        &mut self,
        _message: Loop,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        self.source.emit_batches(&self.batch_sink, ctx).await?;
        ctx.send_self_message(Loop(PrivateToken)).await?;
        Ok(())
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.source.finalize(exit_status, ctx).await?;
        Ok(())
    }
}

pub fn quickwit_supported_sources() -> &'static SourceLoader {
    static SOURCE_LOADER: OnceCell<SourceLoader> = OnceCell::new();
    SOURCE_LOADER.get_or_init(|| {
        let mut source_factory = SourceLoader::default();
        source_factory.add_source("file", FileSourceFactory);
        #[cfg(feature = "kafka")]
        source_factory.add_source("kafka", KafkaSourceFactory);
        source_factory.add_source("vec", VecSourceFactory);
        source_factory.add_source("void", VoidSourceFactory);
        source_factory
    })
}

pub async fn check_source_connectivity(source_config: &SourceConfig) -> anyhow::Result<()> {
    match &source_config.source_params {
        SourceParams::File(params) => {
            if let Some(filepath) = &params.filepath {
                if !Path::new(filepath).exists() {
                    bail!("File `{}` does not exist.", filepath.display())
                }
            }
            Ok(())
        }
        #[allow(unused_variables)]
        SourceParams::Kafka(params) => {
            #[cfg(not(feature = "kafka"))]
            bail!("Quickwit binary was not compiled with the `kafka` feature.");

            #[cfg(feature = "kafka")]
            {
                kafka_source::check_connectivity(params.clone()).await?;
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::VecSourceParams;

    use super::*;

    #[tokio::test]
    async fn test_check_source_connectivity() -> anyhow::Result<()> {
        {
            let source_config = SourceConfig {
                source_id: "void".to_string(),
                source_params: SourceParams::void(),
            };
            check_source_connectivity(&source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "vec".to_string(),
                source_params: SourceParams::Vec(VecSourceParams::default()),
            };
            check_source_connectivity(&source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                source_params: SourceParams::file("file-does-not-exist.json"),
            };
            assert!(check_source_connectivity(&source_config).await.is_err());
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                source_params: SourceParams::file("data/test_corpus.json"),
            };
            assert!(check_source_connectivity(&source_config).await.is_ok());
        }
        Ok(())
    }
}
