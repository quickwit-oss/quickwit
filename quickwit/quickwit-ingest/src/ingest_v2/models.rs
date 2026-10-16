// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use quickwit_common::rate_limiter::RateLimiter;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{IndexUid, Position, QueueId, ShardId, SourceId, SourceUid, queue_id};
use tokio::sync::watch;
use tracing::error;

use crate::ingest_v2::rate_meter::{RateMeter, SharedRateMeter};

/// Status of a shard: state + position of the last record written.
pub(super) type ShardStatus = (ShardState, Position);

#[derive(Debug)]
pub(super) struct IngesterShard {
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    /// Position of the last record written in the shard's mrecordlog queue.
    pub replication_position_inclusive: Position,
    /// Position up to which the shard has been truncated.
    pub truncation_position_inclusive: Position,
    /// The queue size is the amount of total in-flight bytes on this shard that are yet to be
    /// indexed.
    pub queue_size: ByteSize,
    pub rate_limiter: RateLimiter,
    pub rate_meter: RateMeter,
    /// The shared rate meter contains throughput and status readings for all shards, centralized
    /// to be able to report to the control plane.
    shared_rate_meter: Arc<SharedRateMeter>,
    /// Whether the shard should be advertised to other nodes (routers) via gossip.
    ///
    /// Because shards  are created in multiple steps, (e.g., init shard on ingester, create shard
    /// in metastore), we must receive a "signal" from the control plane confirming that a
    /// shard was successfully opened before advertising it. Currently, this confirmation comes
    /// in the form of `PersistRequest` or `FetchRequest`.
    pub is_advertisable: bool,
    /// Document mapper for the shard. Closed shards do not have one.
    pub doc_mapper_opt: Option<Arc<DocMapper>>,
    /// Whether to validate documents in this shard. True if no preprocessing (VRL) will happen
    /// before indexing.
    pub validate_docs: bool,
    pub shard_status_tx: watch::Sender<ShardStatus>,
    pub shard_status_rx: watch::Receiver<ShardStatus>,
    /// Instant at which the shard was last written to.
    pub last_write_instant: Instant,
}

/// Builder for `IngesterShard`. By default, the shard is open, is empty (i.e. the replication and
/// truncation positions are at the beginning), has a zero queue size, uses the default rate limiter
/// and rate meter, has no doc mapper, does not validate documents, and is not advertisable.
pub(super) struct IngesterShardBuilder {
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    shard_state: ShardState,
    replication_position_inclusive: Position,
    truncation_position_inclusive: Position,
    queue_size: ByteSize,
    rate_limiter: RateLimiter,
    rate_meter: RateMeter,
    shared_rate_meter: Arc<SharedRateMeter>,
    doc_mapper_opt: Option<Arc<DocMapper>>,
    validate_docs: bool,
    is_advertisable: bool,
    last_write_instant: Option<Instant>,
}

impl IngesterShardBuilder {
    /// Sets the shard state. Defaults to `ShardState::Open`.
    pub fn with_state(mut self, shard_state: ShardState) -> Self {
        self.shard_state = shard_state;
        self
    }

    /// Sets the queue size. Defaults to zero. Should only be used when restoring closed shards.
    pub fn with_queue_size(mut self, queue_size: ByteSize) -> Self {
        self.queue_size = queue_size;
        self
    }

    /// Sets the rate limiter. Defaults to `RateLimiter::default()`.
    pub fn with_rate_limiter(mut self, rate_limiter: RateLimiter) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    /// Sets the rate meter. Defaults to `RateMeter::default()`.
    pub fn with_rate_meter(mut self, rate_meter: RateMeter) -> Self {
        self.rate_meter = rate_meter;
        self
    }

    pub fn with_shared_rate_meter(mut self, shared_rate_meter: Arc<SharedRateMeter>) -> Self {
        self.shared_rate_meter = shared_rate_meter;
        self
    }

    /// Sets the doc mapper.
    pub fn with_doc_mapper(mut self, doc_mapper: Arc<DocMapper>) -> Self {
        self.doc_mapper_opt = Some(doc_mapper);
        self
    }

    /// Sets the replication position. Defaults to `Position::Beginning`.
    pub fn with_replication_position_inclusive(mut self, position: Position) -> Self {
        self.replication_position_inclusive = position;
        self
    }

    /// Sets the truncation position. Defaults to `Position::Beginning`.
    pub fn with_truncation_position_inclusive(mut self, position: Position) -> Self {
        self.truncation_position_inclusive = position;
        self
    }

    /// Sets whether to validate documents. Defaults to `false`.
    pub fn with_validate_docs(mut self, validate_docs: bool) -> Self {
        self.validate_docs = validate_docs;
        self
    }

    /// Sets whether the shard should be advertised to other nodes via gossip. Defaults to `false`.
    pub fn advertisable(mut self) -> Self {
        self.is_advertisable = true;
        self
    }

    /// Sets the last write instant. Defaults to `Instant::now()`.
    pub fn with_last_write(mut self, last_write_instant: Instant) -> Self {
        self.last_write_instant = Some(last_write_instant);
        self
    }

    /// Builds the `IngesterShard`. Uses `Instant::now()` for last write time if not specified.
    pub fn build(self) -> IngesterShard {
        let shard_status = (
            self.shard_state,
            self.replication_position_inclusive.clone(),
        );
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);
        self.shared_rate_meter.insert(
            queue_id(&self.index_uid, &self.source_id, &self.shard_id),
            SourceUid {
                index_uid: self.index_uid.clone(),
                source_id: self.source_id.clone(),
            },
            self.shard_id.clone(),
            self.shard_state,
            self.is_advertisable,
        );
        IngesterShard {
            index_uid: self.index_uid,
            source_id: self.source_id,
            shard_id: self.shard_id,
            shard_state: self.shard_state,
            replication_position_inclusive: self.replication_position_inclusive,
            truncation_position_inclusive: self.truncation_position_inclusive,
            queue_size: self.queue_size,
            rate_limiter: self.rate_limiter,
            rate_meter: self.rate_meter,
            shared_rate_meter: self.shared_rate_meter,
            is_advertisable: self.is_advertisable,
            doc_mapper_opt: self.doc_mapper_opt,
            validate_docs: self.validate_docs,
            shard_status_tx,
            shard_status_rx,
            last_write_instant: self.last_write_instant.unwrap_or_else(Instant::now),
        }
    }
}

impl IngesterShard {
    /// Creates a shard builder.
    pub fn builder(
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
    ) -> IngesterShardBuilder {
        IngesterShardBuilder {
            index_uid,
            source_id,
            shard_id,
            shard_state: ShardState::Open,
            replication_position_inclusive: Position::Beginning,
            truncation_position_inclusive: Position::Beginning,
            queue_size: ByteSize::default(),
            rate_limiter: RateLimiter::default(),
            rate_meter: RateMeter::default(),
            shared_rate_meter: Arc::new(SharedRateMeter::default()),
            doc_mapper_opt: None,
            validate_docs: false,
            is_advertisable: false,
            last_write_instant: None,
        }
    }

    pub fn close(&mut self) {
        self.shard_state = ShardState::Closed;
        self.shared_rate_meter.close(&self.queue_id());
        self.notify_shard_status();
    }

    pub fn make_advertisable(&mut self) {
        if self.is_advertisable {
            return;
        }
        self.is_advertisable = true;
        self.shared_rate_meter.make_advertisable(&self.queue_id());
    }

    pub fn record_append(
        &mut self,
        replication_position_inclusive: Position,
        queue_size: ByteSize,
        num_persisted_bytes: u64,
        now: Instant,
    ) {
        self.queue_size = queue_size;
        self.shared_rate_meter
            .record_persisted_bytes(&self.queue_id(), num_persisted_bytes);
        self.set_replication_position_inclusive(replication_position_inclusive, now);
    }

    pub fn is_closed(&self) -> bool {
        self.shard_state.is_closed()
    }

    pub fn is_open(&self) -> bool {
        self.shard_state.is_open()
    }

    pub fn is_idle(&self, now: Instant, idle_timeout: Duration) -> bool {
        now.duration_since(self.last_write_instant) >= idle_timeout
    }

    /// Returns `true` if the shard is unreachable to the control plane and empty.
    // A non-advertisable shard means the control plane never got confirmation that this
    // shard was initialized on this ingester (e.g. it lost the `init_shards` response),
    // so it never recorded the shard in the metastore or handed it out to any router or
    // indexer. Such a shard can never become advertisable (that requires a persist/fetch
    // request, which requires the control plane to know about it) and is invisible to
    // the other RPC or gossip-driven cleanup mechanisms. It's safe to delete: never
    // having been advertised, it can't have received any writes.
    pub fn is_empty_orphan(&self) -> bool {
        if self.is_advertisable {
            return false;
        }
        let is_empty = self.replication_position_inclusive.is_beginning();
        if !is_empty {
            error!(
                "shard `{}` is not advertisable but is not empty, this should never happen, \
                 please report",
                self.queue_id()
            );
        }
        is_empty
    }

    pub fn notify_shard_status(&self) {
        let shard_status = (
            self.shard_state,
            self.replication_position_inclusive.clone(),
        );
        // `shard_status_tx` is guaranteed to be open because `self` also holds a receiver.
        self.shard_status_tx
            .send(shard_status)
            .expect("channel should be open");
    }

    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, &self.shard_id)
    }

    fn set_replication_position_inclusive(
        &mut self,
        replication_position_inclusive: Position,
        now: Instant,
    ) {
        if self.replication_position_inclusive == replication_position_inclusive {
            return;
        }
        self.replication_position_inclusive = replication_position_inclusive;
        self.last_write_instant = now;
        self.notify_shard_status();
    }
}

impl Drop for IngesterShard {
    // Removing deleted shards from the rate meter is important so we don't report inaccurate
    // throughput readings to the control plane.
    fn drop(&mut self) {
        self.shared_rate_meter.remove(&self.queue_id());
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::{DocMapping, SearchSettings, build_doc_mapper};

    use super::*;

    impl IngesterShard {
        #[track_caller]
        pub fn assert_is_open(&self) {
            assert!(self.shard_state.is_open())
        }

        #[track_caller]
        pub fn assert_is_closed(&self) {
            assert!(self.shard_state.is_closed())
        }

        #[track_caller]
        pub fn assert_replication_position(&self, expected_replication_position: Position) {
            assert_eq!(
                self.replication_position_inclusive, expected_replication_position,
                "expected replication position at `{:?}`, got `{:?}`",
                expected_replication_position, self.replication_position_inclusive
            );
        }

        #[track_caller]
        pub fn assert_truncation_position(&self, expected_truncation_position: Position) {
            assert_eq!(
                self.truncation_position_inclusive, expected_truncation_position,
                "expected truncation position at `{:?}`, got `{:?}`",
                expected_truncation_position, self.truncation_position_inclusive
            );
        }
    }

    #[test]
    fn test_shard_builder() {
        let doc_mapping: DocMapping = serde_json::from_str("{}").unwrap();
        let search_settings = SearchSettings::default();
        let doc_mapper = build_doc_mapper(&doc_mapping, &search_settings).unwrap();

        let shard = IngesterShard::builder(
            IndexUid::for_test("test-index", 0),
            SourceId::from("test-source"),
            ShardId::from(1),
        )
        .with_state(ShardState::Closed)
        .with_replication_position_inclusive(Position::offset(42u64))
        .with_doc_mapper(doc_mapper)
        .with_validate_docs(true)
        .build();

        assert_eq!(shard.shard_state, ShardState::Closed);
        assert_eq!(
            shard.replication_position_inclusive,
            Position::offset(42u64)
        );
        assert_eq!(shard.truncation_position_inclusive, Position::Beginning);
        assert!(!shard.is_advertisable);
        assert_eq!(shard.queue_size.as_u64(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_shard_updates_shared_rate_meter() {
        let meter = Arc::new(SharedRateMeter::default());
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut shard = IngesterShard::builder(index_uid, source_id, ShardId::from(1))
            .with_shared_rate_meter(meter.clone())
            .build();
        assert!(meter.harvest().per_source_readings.is_empty());

        shard.make_advertisable();
        shard.record_append(
            Position::offset(0u64),
            ByteSize::b(100),
            100,
            Instant::now(),
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        let readings = meter.harvest();
        let reading = &readings.per_source_readings[&source_uid][0];
        assert_eq!(reading.shard_state, ShardState::Open);
        assert_eq!(reading.short_term_ingestion_rate, ByteSize::b(100));

        shard.close();
        let readings = meter.harvest();
        assert_eq!(
            readings.per_source_readings[&source_uid][0].shard_state,
            ShardState::Closed
        );

        drop(shard);
        assert!(meter.harvest().per_source_readings.is_empty());
    }

    #[test]
    fn test_is_empty_orphan() {
        let non_advertisable_empty_shard = IngesterShard::builder(
            IndexUid::for_test("test-index", 0),
            SourceId::from("test-source"),
            ShardId::from(1),
        )
        .with_state(ShardState::Closed)
        .build();
        assert!(non_advertisable_empty_shard.is_empty_orphan());

        let advertisable_empty_shard = IngesterShard::builder(
            IndexUid::for_test("test-index", 0),
            SourceId::from("test-source"),
            ShardId::from(2),
        )
        .with_state(ShardState::Closed)
        .advertisable()
        .build();
        assert!(!advertisable_empty_shard.is_empty_orphan());

        let non_advertisable_non_empty_shard = IngesterShard::builder(
            IndexUid::for_test("test-index", 0),
            SourceId::from("test-source"),
            ShardId::from(3),
        )
        .with_state(ShardState::Closed)
        .with_replication_position_inclusive(Position::offset(42u64))
        .build();
        assert!(!non_advertisable_non_empty_shard.is_empty_orphan());
    }
}
