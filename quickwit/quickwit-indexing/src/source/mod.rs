// Copyright (C) 2022 Quickwit, Inc.
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
mod ingest_api_source;
#[cfg(feature = "kafka")]
mod kafka_source;
#[cfg(feature = "kinesis")]
mod kinesis;
mod source_factory;
mod vec_source;
mod void_source;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
pub use file_source::{FileSource, FileSourceFactory};
#[cfg(feature = "kafka")]
pub use kafka_source::{KafkaSource, KafkaSourceFactory};
#[cfg(feature = "kinesis")]
pub use kinesis::kinesis_source::{KinesisSource, KinesisSourceFactory};
use once_cell::sync::OnceCell;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::{SourceConfig, SourceParams};
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_metastore::Metastore;
pub use source_factory::{SourceFactory, SourceLoader, TypedSourceFactory};
use tokio::runtime::Handle;
use tracing::error;
pub use vec_source::{VecSource, VecSourceFactory};
pub use void_source::{VoidSource, VoidSourceFactory};

use crate::actors::DocProcessor;
use crate::source::ingest_api_source::IngestApiSourceFactory;

/// Reserved source ID used for the ingest API.
pub const INGEST_API_SOURCE_ID: &str = ".ingest-api";

/// Runtime configuration used during execution of a source actor.
pub struct SourceExecutionContext {
    pub metastore: Arc<dyn Metastore>,
    pub index_id: String,
    pub source_config: SourceConfig,
}

impl SourceExecutionContext {
    #[cfg(test)]
    fn for_test(
        metastore: Arc<dyn Metastore>,
        index_id: &str,
        source_config: SourceConfig,
    ) -> Arc<SourceExecutionContext> {
        Arc::new(Self {
            metastore,
            index_id: index_id.to_string(),
            source_config,
        })
    }
}

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
    async fn initialize(
        &mut self,
        _doc_processor_mailbox: &Mailbox<DocProcessor>,
        _ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    /// Main part of the source implementation, `emit_batches` can emit 0..n batches.
    ///
    /// The `batch_sink` is a mailbox that has a bounded capacity.
    /// In that case, `batch_sink` will block.
    ///
    /// It returns an optional duration specifying how long the batch requester
    /// should wait before pooling gain.
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus>;

    /// After publication of a split, `suggest_truncate` is called.
    /// This makes it possible for the implementation of a source to
    /// release some resources associated to the data that was just published.
    ///
    /// This method is for instance useful for the ingest API, as it is possible
    /// to delete all message anterior to the checkpoint in the ingest API queue.
    ///
    /// It is perfectly fine for implementation to ignore this function.
    /// For instance, message queue like kafka are meant to be shared by different
    /// client, and rely on a retention strategy to delete messages.
    ///
    /// Returning an error has no effect on the source actor itself or the
    /// indexing pipeline, as truncation is just "a suggestion".
    /// The error will however be logged.
    async fn suggest_truncate(
        &self,
        _checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

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

/// The SourceActor acts as a thin wrapper over a source trait object to execute.
///
/// It mostly takes care of running a loop calling `emit_batches(...)`.
pub struct SourceActor {
    pub source: Box<dyn Source>,
    pub doc_processor_mailbox: Mailbox<DocProcessor>,
}

#[derive(Debug)]
struct Loop;

#[async_trait]
impl Actor for SourceActor {
    type ObservableState = serde_json::Value;

    fn name(&self) -> String {
        self.source.name()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.source.observable_state()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::NonBlocking.get_runtime_handle()
    }

    fn yield_after_each_message(&self) -> bool {
        false
    }

    async fn initialize(&mut self, ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        self.source
            .initialize(&self.doc_processor_mailbox, ctx)
            .await?;
        self.handle(Loop, ctx).await?;
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

#[async_trait]
impl Handler<Loop> for SourceActor {
    type Reply = ();

    async fn handle(&mut self, _message: Loop, ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        let wait_for = self
            .source
            .emit_batches(&self.doc_processor_mailbox, ctx)
            .await?;
        ctx.record_progress();
        if wait_for.is_zero() {
            ctx.send_self_message(Loop).await?;
            return Ok(());
        }
        ctx.schedule_self_msg(wait_for, Loop).await;
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
        #[cfg(feature = "kinesis")]
        source_factory.add_source("kinesis", KinesisSourceFactory);
        source_factory.add_source("vec", VecSourceFactory);
        source_factory.add_source("void", VoidSourceFactory);
        source_factory.add_source("ingest-api", IngestApiSourceFactory);
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
        #[allow(unused_variables)]
        SourceParams::Kinesis(params) => {
            #[cfg(not(feature = "kinesis"))]
            bail!("Quickwit binary was not compiled with the `kinesis` feature.");

            #[cfg(feature = "kinesis")]
            {
                kinesis::check_connectivity(params.clone()).await?;
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

#[derive(Debug)]
pub struct SuggestTruncate(pub SourceCheckpoint);

#[async_trait]
impl Handler<SuggestTruncate> for SourceActor {
    type Reply = ();

    async fn handle(
        &mut self,
        suggest_truncate: SuggestTruncate,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let SuggestTruncate(checkpoint) = suggest_truncate;
        if let Err(err) = self.source.suggest_truncate(checkpoint, ctx).await {
            // Failing to process suggest truncate does not
            // kill the source nor the indexing pipeline, but we log the error.
            error!(err=?err, "suggest-truncate-error");
        }
        Ok(())
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
                num_pipelines: 1,
                source_params: SourceParams::void(),
            };
            check_source_connectivity(&source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "vec".to_string(),
                num_pipelines: 1,
                source_params: SourceParams::Vec(VecSourceParams::default()),
            };
            check_source_connectivity(&source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                num_pipelines: 1,
                source_params: SourceParams::file("file-does-not-exist.json"),
            };
            assert!(check_source_connectivity(&source_config).await.is_err());
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                num_pipelines: 1,
                source_params: SourceParams::file("data/test_corpus.json"),
            };
            assert!(check_source_connectivity(&source_config).await.is_ok());
        }
        Ok(())
    }
}
