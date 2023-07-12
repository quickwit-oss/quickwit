// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_compression::futures::bufread::GzipDecoder;
use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncRead;
use futures_util::io::{BufReader, Lines};
use futures_util::{AsyncBufReadExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use once_cell::sync::Lazy;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::HttpSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use regex::Regex;
use serde::Serialize;
use tracing::log::warn;
use tracing::{debug, info};

use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, SourceExecutionContext, TypedSourceFactory};

/// Number of bytes after which a new batch is cut.

static URI_EXPAND_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\{\d+..\d+})").unwrap());

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HttpSourceCounters {
    pub http_offset: u64,
    pub previous_offset: u64,
    pub current_offset: u64,
    pub num_lines_processed: u64,
}

pub struct HttpSource {
    ctx: Arc<SourceExecutionContext>,
    current_counters: BTreeMap<PartitionId, HttpSourceCounters>,
    current_reader: Option<Lines<BufReader<Box<dyn AsyncRead + Send + Unpin>>>>,
    uri_with_errors: Vec<String>,
}

impl fmt::Debug for HttpSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HttpSource {{ source_id: {} }}",
            self.ctx.pipeline_id.source_id
        )
    }
}

static NODE_IDX_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z0-9-_\.]+[_-](\d+)$").unwrap());

impl HttpSource {
    pub fn new(
        ctx: Arc<SourceExecutionContext>,
        params: HttpSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> Self {
        let node_idx = parse_node_idx(ctx.pipeline_id.node_id.as_str());
        let mut uris = params
            .uris
            .iter()
            .map(|uri| uri.as_str().to_string())
            .collect_vec();
        if let Some(uri_pattern) = params.uri_pattern {
            uris.extend(expand_uris(uri_pattern.as_str()));
        }
        let desired_num_pipelines = ctx.source_config.desired_num_pipelines;
        let mut current_counters = BTreeMap::new();
        debug!("`HttpSource` partitions expressed as URIs: {:?}", uris);
        info!(
            "`HttpSource` will select partitions that satisfy `idx % desired_num_pipelines.get() \
             == node_idx` with node_idx={node_idx} and \
             desired_num_pipelines={desired_num_pipelines:?}"
        );
        for (idx, uri) in uris.iter().enumerate() {
            if idx % desired_num_pipelines.get() != node_idx {
                continue;
            }
            let partition_id = PartitionId::from(uri.to_string());
            let position_opt = checkpoint.position_for_partition(&partition_id).cloned();
            if let Some(position) = &position_opt {
                let next_offset = position_to_u64(position);
                if next_offset == u64::MAX {
                    // Skip partitions that have been fully processed.
                    continue;
                }
                let counters = HttpSourceCounters {
                    previous_offset: next_offset,
                    current_offset: next_offset,
                    num_lines_processed: 0,
                    http_offset: 0,
                };
                current_counters.insert(partition_id.clone(), counters);
            } else {
                let counters = HttpSourceCounters {
                    previous_offset: 0,
                    current_offset: 0,
                    num_lines_processed: 0,
                    http_offset: 0,
                };
                current_counters.insert(partition_id.clone(), counters);
            }
        }
        info!("HttpSource current counters: {current_counters:?}");
        Self {
            ctx,
            current_counters,
            current_reader: None,
            uri_with_errors: Vec::new(),
        }
    }
}

fn position_to_u64(position: &Position) -> u64 {
    match position {
        Position::Beginning => 0,
        Position::Offset(offset_str) => offset_str
            .parse()
            .expect("Failed to parse checkpoint position to u64."),
    }
}

pub(crate) const BATCH_NUM_BYTES_LIMIT: u64 = 5_000_000u64;

async fn read_lines(
    uri: &str,
) -> anyhow::Result<Lines<BufReader<Box<dyn AsyncRead + Send + Unpin>>>> {
    let client = reqwest::ClientBuilder::new()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(15))
        .build()
        .context("Failed to build reqwest client.")?;
    let stream = client
        .get(uri)
        .send()
        .await
        .context(format!("Failed to get response from uri: {uri:?}"))?
        .error_for_status()
        .context("Invalid status code returned.")?
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read();
    let reader =
        Box::new(GzipDecoder::new(BufReader::new(stream))) as Box<dyn AsyncRead + Unpin + Send>;
    Ok(BufReader::new(reader).lines())
}

#[async_trait]
impl Source for HttpSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        // Get next partition and position to read from
        let Some((partition, counters)) = self
            .current_counters
            .iter_mut()
            .filter(|(partition, _)| !self.uri_with_errors.contains(&partition.0))
            .find(|(_, partition_counters)| partition_counters.current_offset < u64::MAX)
        else {
            info!("No more partitions to read from, stopping source.");
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        };
        let uri = partition.0.as_str();
        let lines_result: anyhow::Result<Lines<BufReader<Box<dyn AsyncRead + Send + Unpin>>>> =
            match self.current_reader.take() {
                Some(lines) => Ok(lines),
                None => {
                    counters.http_offset = 0;
                    read_lines(uri).await
                }
            };
        let Ok(mut lines) = lines_result else {
            warn!("Failed to read from uri: {uri:?}, skip it.");
            self.uri_with_errors.push(partition.0.to_string());
            return Ok(Duration::default());
        };
        let mut doc_batch = RawDocBatch::default();
        let mut reach_eof = true;
        while let Some(Ok(line)) = lines.next().await {
            counters.http_offset += 1 + line.as_bytes().len() as u64; // +1 for newline
            counters.num_lines_processed += 1;
            if counters.previous_offset >= counters.http_offset {
                continue;
            }
            if counters.http_offset - counters.current_offset > BATCH_NUM_BYTES_LIMIT {
                reach_eof = false;
                break;
            }
            doc_batch.docs.push(Bytes::from(line));
        }

        if !doc_batch.docs.is_empty() {
            let final_offset: u64 = if reach_eof {
                u64::MAX
            } else {
                counters.http_offset
            };
            counters.previous_offset = counters.current_offset;
            counters.current_offset = final_offset;
            doc_batch
                .checkpoint_delta
                .record_partition_delta(
                    partition.clone(),
                    Position::from(counters.previous_offset),
                    Position::from(counters.current_offset),
                )
                .unwrap();
            ctx.send_message(doc_processor_mailbox, doc_batch).await?;
        }
        if !reach_eof {
            self.current_reader = Some(lines);
        }
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("HttpSource{{source_id={}}}", self.ctx.pipeline_id.source_id)
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::to_value(&self.current_counters).unwrap()
    }
}

pub struct HttpSourceFactory;

#[async_trait]
impl TypedSourceFactory for HttpSourceFactory {
    type Source = HttpSource;
    type Params = HttpSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: HttpSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        Ok(HttpSource::new(ctx, params, checkpoint))
    }
}

fn parse_node_idx(node_id: &str) -> usize {
    match NODE_IDX_PATTERN.captures(node_id) {
        Some(captured) => {
            let idx = captured.get(1).unwrap().as_str().parse::<usize>().unwrap();
            idx
        }
        None => {
            warn!("`HttpSource` cannot use `{node_id}` to select a subset of partitions.");
            0
        }
    }
}

pub struct RangeExpand<'a> {
    replace_str: &'a str,
    range: Range<usize>,
    zero_pad_by: usize,
}

/// Expands a uri with the range syntax into the exported/expected uris.
fn expand_uris(uri: &str) -> Vec<String> {
    let mut total_variants = 0;
    let mut ranges = Vec::new();
    for capture in URI_EXPAND_PATTERN.captures_iter(uri) {
        let cap = capture.get(0).unwrap();
        let replace_str = cap.as_str();

        let range_str = replace_str.trim_matches('{').trim_matches('}');
        let (start, end) = range_str.split_once("..").unwrap();
        let pad_start = start.starts_with('0');
        let zero_pad_by = if pad_start { start.len() } else { 0 };
        let start = start.parse::<usize>().unwrap();
        let end = end.parse::<usize>().unwrap();
        let range = start..end;

        total_variants += range.len();
        ranges.push(RangeExpand {
            replace_str,
            range,
            zero_pad_by,
        })
    }

    let mut uris = Vec::with_capacity(total_variants);
    populate_uri(uri, &ranges, &mut uris);
    uris
}

fn populate_uri(uri: &str, range_expand: &[RangeExpand], uris: &mut Vec<String>) {
    assert!(!range_expand.is_empty());
    let uri_clone = uri.to_string();
    let current_expand_range = &range_expand[0];
    for i in current_expand_range.range.clone() {
        let value = format!("{i:0>pad_by$}", pad_by = current_expand_range.zero_pad_by);
        let updated_uri = uri_clone.replacen(range_expand[0].replace_str, &value, 1);
        if range_expand.len() > 1 {
            populate_uri(&updated_uri, &range_expand[1..], uris);
        } else {
            uris.push(updated_uri);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::PathBuf;

    use quickwit_actors::{Command, Universe};
    use quickwit_common::uri::Uri;
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::checkpoint::{SourceCheckpoint, SourceCheckpointDelta};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::IndexUid;

    use super::*;
    use crate::models::IndexingPipelineId;
    use crate::source::SourceActor;

    #[test]
    fn test_parse_node_idx() {
        assert_eq!(parse_node_idx("kafka-node-0"), 0);
        assert_eq!(parse_node_idx("searcher-1"), 1);
        assert_eq!(parse_node_idx("kafka_node_020"), 20);
    }

    #[test]
    fn test_gh_archive_uri_expand() {
        let uri = "https://data.gharchive.org/{2015..2024}-{01..13}-{01..32}-{0..24}.json.gz";
        let uris = expand_uris(uri);
        for uri in uris {
            println!("{}", uri);
        }
        panic!("test")
    }

    #[test]
    fn test_uri_expand() {
        let uri = "http://localhost:3000/{00..2}-{0..3}.json";
        let uris = expand_uris(uri);

        assert_eq!(
            uris,
            vec![
                "http://localhost:3000/00-0.json",
                "http://localhost:3000/00-1.json",
                "http://localhost:3000/00-2.json",
                "http://localhost:3000/01-0.json",
                "http://localhost:3000/01-1.json",
                "http://localhost:3000/01-2.json",
            ]
        )
    }

    #[tokio::test]
    async fn test_http_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let params = HttpSourceParams::from_pattern(Uri::from_well_formed(
            "https://data.gharchive.org/2015-01-01-{0..2}.json.gz",
        ));

        let metastore = metastore_for_test();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new("test-index"),
            source_id: "kafka-file-source".to_string(),
            node_id: "kafka-node".to_string(),
            pipeline_ord: 0,
        };
        let file_source = HttpSourceFactory::typed_create_source(
            SourceExecutionContext::for_test(
                metastore,
                pipeline_id,
                PathBuf::from("./queues"),
                SourceConfig {
                    source_id: "test-http-source".to_string(),
                    desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
                    max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                    enabled: true,
                    source_params: SourceParams::Http(params.clone()),
                    transform_config: None,
                    input_format: SourceInputFormat::Json,
                },
            ),
            params,
            SourceCheckpoint::default(),
        )
        .await?;
        let file_source_actor = SourceActor {
            source: Box::new(file_source),
            doc_processor_mailbox,
        };
        let (_http_source_mailbox, http_source_handle) =
            universe.spawn_builder().spawn(file_source_actor);
        let (actor_termination, positions) = http_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            positions,
            serde_json::json!({
                "https://data.gharchive.org/2015-01-01-0.json.gz": {
                    "current_offset": 18446744073709551615u64,
                    "http_offset": 18023797,
                    "num_lines_processed": 7702,
                    "previous_offset": 15002237,
                },
                "https://data.gharchive.org/2015-01-01-1.json.gz": {
                    "current_offset": 18446744073709551615u64,
                    "num_lines_processed": 7427,
                    "http_offset": 17649671,
                    "previous_offset": 15007652,
                }
            })
        );
        let indexer_msgs = indexer_inbox.drain_for_test();
        assert_eq!(indexer_msgs.len(), 9);
        let batch1 = indexer_msgs[0].downcast_ref::<RawDocBatch>().unwrap();
        let batch2 = indexer_msgs[1].downcast_ref::<RawDocBatch>().unwrap();
        let command = indexer_msgs[8].downcast_ref::<Command>().unwrap();
        assert_eq!(
            format!("{:?}", &batch1.checkpoint_delta),
            format!(
                "∆({}:{})",
                "https://data.gharchive.org/2015-01-01-0.json.gz",
                "(00000000000000000000..00000000000005000080]"
            )
        );
        assert_eq!(
            &extract_position_delta(&batch2.checkpoint_delta).unwrap(),
            "00000000000005000080..00000000000010000360"
        );
        assert!(matches!(command, &Command::ExitWithSuccess));
        Ok(())
    }

    fn extract_position_delta(checkpoint_delta: &SourceCheckpointDelta) -> Option<String> {
        let checkpoint_delta_str = format!("{checkpoint_delta:?}");
        let (_left, right) =
            &checkpoint_delta_str[..checkpoint_delta_str.len() - 2].rsplit_once('(')?;
        Some(right.to_string())
    }

    #[tokio::test]
    async fn test_http_source_resume_from_checkpoint() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let source_uri = "https://data.gharchive.org/2015-01-01-0.json.gz";
        let params = HttpSourceParams::from_list(vec![Uri::from_well_formed(
            "https://data.gharchive.org/2015-01-01-0.json.gz",
        )]);
        let mut checkpoint = SourceCheckpoint::default();
        let partition_id = PartitionId::from(source_uri.to_string());
        let checkpoint_delta = SourceCheckpointDelta::from_partition_delta(
            partition_id,
            Position::from(0u64),
            Position::from(18017456u64),
        )
        .unwrap();
        checkpoint.try_apply_delta(checkpoint_delta).unwrap();

        let metastore = metastore_for_test();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new("test-index"),
            source_id: "test-file-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let source = HttpSourceFactory::typed_create_source(
            SourceExecutionContext::for_test(
                metastore,
                pipeline_id,
                PathBuf::from("./queues"),
                SourceConfig {
                    source_id: "test-http-source".to_string(),
                    desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
                    max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                    enabled: true,
                    source_params: SourceParams::Http(params.clone()),
                    transform_config: None,
                    input_format: SourceInputFormat::Json,
                },
            ),
            params,
            checkpoint,
        )
        .await
        .unwrap();
        let http_source_actor = SourceActor {
            source: Box::new(source),
            doc_processor_mailbox,
        };
        let (_file_source_mailbox, file_source_handle) =
            universe.spawn_builder().spawn(http_source_actor);
        let (actor_termination, counters) = file_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(
            counters,
            serde_json::json!({
                "https://data.gharchive.org/2015-01-01-0.json.gz": {
                    "current_offset": 18446744073709551615u64,
                    "http_offset": 18023797,
                    "num_lines_processed": 7702,
                    "previous_offset": 18017456,
                }
            })
        );
        let indexer_messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(&indexer_messages[0].docs[0].starts_with(b"{\"id\""));
    }
}
