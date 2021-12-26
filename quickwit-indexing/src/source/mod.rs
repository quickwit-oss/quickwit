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
pub use file_source::{FileSource, FileSourceFactory, FileSourceParams, STD_IN_SOURCE_ID};
#[cfg(feature = "kafka")]
pub use kafka_source::{KafkaSource, KafkaSourceFactory, KafkaSourceParams};
use once_cell::sync::OnceCell;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, AsyncActor, Mailbox};
use quickwit_config::SourceConfig;
pub use source_factory::{SourceFactory, SourceLoader, TypedSourceFactory};
pub use vec_source::{VecSource, VecSourceFactory, VecSourceParams};
pub use void_source::{VoidSource, VoidSourceFactory, VoidSourceParams};

use crate::models::IndexerMessage;

pub type SourceContext = ActorContext<SourceActor>;

/// A source is a trait that is mounted in a light wrapping Actor called `SourceActor`.
///
/// For this reason, its methods mimics those of Actor.
/// One key difference is the absence of messages.
///
/// The `SourceActor` implements a loop until emit_batches returns an
/// ActorExitStatus.
///
/// Conceptually, a source execution works as if it was a simple loop
/// as follow:
/// ```ignore
/// source.initialize(ctx)?
/// let exit_status = loop {
///   if let Err(exit_status) = source.emit_batches()? {
///      break exit_status;
////  }
/// };
/// source.finalize(exit_status)?;
/// ```
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// This method will be called before any calls to `emit_batches`.
    async fn initialize(&mut self, _ctx: &SourceContext) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    /// Main part of the source implementation, `emit_batches` can emit 0..n batches.
    /// It is expected to return relatively fast.
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
    match source_config.source_type.as_ref() {
        "file" => {
            if source_config.source_id == STD_IN_SOURCE_ID {
                return Ok(());
            }
            match source_config.params.get("filepath") {
                Some(serde_json::Value::String(path)) => {
                    if Path::new(&path).exists() {
                        Ok(())
                    } else {
                        bail!("File `{}` does not exist.", path)
                    }
                }
                Some(_) => {
                    bail!(
                        "Failed to parse source config params: property `filepath` is not a \
                         string."
                    )
                }
                None => {
                    bail!("Failed to parse source config params: property `filepath` is missing.")
                }
            }
        }
        "kafka" => {
            #[cfg(not(feature = "kafka"))]
            bail!("Quickwit binary was not compiled with the Kafka source feature.");

            #[cfg(feature = "kafka")]
            kafka_source::check_connectivity(source_config.params.clone())
        }
        "vec" => Ok(()),
        "void" => Ok(()),
        unrecognized_source_type => {
            bail!(
                "Unknown source type `{}` or connectivity check not implemented for this source \
                 type.",
                unrecognized_source_type
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn test_check_source_connectivity() -> anyhow::Result<()> {
        {
            let source_config = SourceConfig {
                source_id: "void".to_string(),
                source_type: "void".to_string(),
                params: json!(null),
            };
            check_source_connectivity(&source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "vec".to_string(),
                source_type: "vec".to_string(),
                params: json!(null),
            };
            check_source_connectivity(&source_config).await?;
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                source_type: "file".to_string(),
                params: json!({
                    "filepath": "non-existing-file.json"
                }),
            };
            assert!(check_source_connectivity(&source_config).await.is_err());
        }
        {
            let source_config = SourceConfig {
                source_id: "file".to_string(),
                source_type: "file".to_string(),
                params: json!({
                    "filepath": "data/test_corpus.json"
                }),
            };
            assert!(check_source_connectivity(&source_config).await.is_ok());
        }
        Ok(())
    }
}
