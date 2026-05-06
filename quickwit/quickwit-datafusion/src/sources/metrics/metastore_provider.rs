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

//! Real `MetricsSplitProvider` backed by the Quickwit metastore.

use std::ops::Bound;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use quickwit_metastore::{
    ListParquetSplitsQuery, ListParquetSplitsRequestExt, ListParquetSplitsResponseExt,
};
use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};
use quickwit_proto::metastore::{
    ListMetricsSplitsRequest, ListSketchSplitsRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use tracing::{debug, instrument};

use super::predicate::MetricsSplitQuery;
use super::table_provider::MetricsSplitProvider;

/// Per-page split count for metastore split pagination.
///
/// The list split RPCs are unary, so we need to page client side.
/// TODO: Use streaming RPCs for listing Parquet splits.
const SPLIT_PAGE_SIZE: usize = 200;

/// `MetricsSplitProvider` backed by the Quickwit metastore RPC.
#[derive(Debug, Clone)]
pub struct MetastoreSplitProvider {
    metastore: MetastoreServiceClient,
    index_uid: IndexUid,
    split_kind: ParquetSplitKind,
}

impl MetastoreSplitProvider {
    pub fn new(
        metastore: MetastoreServiceClient,
        index_uid: IndexUid,
        split_kind: ParquetSplitKind,
    ) -> Self {
        Self {
            metastore,
            index_uid,
            split_kind,
        }
    }
}

#[async_trait]
impl MetricsSplitProvider for MetastoreSplitProvider {
    #[instrument(
        skip(self, query),
        fields(
            index_uid = %self.index_uid,
            metric_names = ?query.metric_names,
            time_range_start = ?query.time_range_start,
            time_range_end = ?query.time_range_end,
            split_kind = ?self.split_kind,
            num_splits,
        )
    )]
    async fn list_splits(&self, query: &MetricsSplitQuery) -> DFResult<Vec<ParquetSplitMetadata>> {
        let mut metastore_query = to_metastore_query(&self.index_uid, query);
        metastore_query.limit = Some(SPLIT_PAGE_SIZE);

        let mut splits: Vec<ParquetSplitMetadata> = Vec::new();
        let mut num_pages: usize = 0;

        loop {
            let records = match self.split_kind {
                ParquetSplitKind::Metrics => {
                    let request = ListMetricsSplitsRequest::try_from_query(
                        self.index_uid.clone(),
                        &metastore_query,
                    )
                    .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

                    self.metastore
                        .clone()
                        .list_metrics_splits(request)
                        .await
                        .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?
                        .deserialize_splits()
                        .map_err(|err| {
                            datafusion::error::DataFusionError::External(Box::new(err))
                        })?
                }
                ParquetSplitKind::Sketches => {
                    let request = ListSketchSplitsRequest::try_from_query(
                        self.index_uid.clone(),
                        &metastore_query,
                    )
                    .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

                    self.metastore
                        .clone()
                        .list_sketch_splits(request)
                        .await
                        .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?
                        .deserialize_splits()
                        .map_err(|err| {
                            datafusion::error::DataFusionError::External(Box::new(err))
                        })?
                }
            };

            num_pages += 1;
            let page_len = records.len();

            // The metastore guarantees only Published splits are returned because
            // `to_metastore_query` sets `split_states = vec![Published]`. No
            // client-side re-filter is needed here.
            splits.extend(records.into_iter().map(|record| record.metadata));

            // A short page (fewer rows than we asked for) means we've drained
            // the result set. The Postgres backend orders by `split_id ASC`
            // and applies `split_id > $after_split_id` for the cursor, so the
            // last metadata's split_id is the correct next cursor.
            if page_len < SPLIT_PAGE_SIZE {
                break;
            }
            let Some(last) = splits.last() else { break };
            metastore_query.after_split_id = Some(last.split_id.as_str().to_string());
        }

        tracing::Span::current().record("num_splits", splits.len());
        debug!(
            num_splits = splits.len(),
            num_pages, "metastore returned splits"
        );

        Ok(splits)
    }
}

/// Convert a DataFusion `MetricsSplitQuery` to a metastore `ListMetricsSplitsQuery`.
///
/// Only metric name and time range are forwarded — the only dimensions the
/// metastore reliably populates today. Tag-based pruning will be wired once
/// the zonemap/bloom-filter mechanism is in place.
fn to_metastore_query(index_uid: &IndexUid, query: &MetricsSplitQuery) -> ListParquetSplitsQuery {
    let mut metastore_query = ListParquetSplitsQuery::for_index(index_uid.clone());

    if let Some(ref names) = query.metric_names {
        metastore_query.metric_names = names.clone();
    }

    if let Some(start) = query.time_range_start {
        metastore_query.time_range.start = Bound::Excluded(start as i64);
    }

    if let Some(end) = query.time_range_end {
        metastore_query.time_range.end = Bound::Excluded(end as i64);
    }

    metastore_query
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_metastore_query_uses_half_open_time_range_bounds() {
        let index_uid = IndexUid::for_test("metrics", 0);
        let query = MetricsSplitQuery {
            metric_names: Some(vec!["cpu.usage".to_string()]),
            time_range_start: Some(1_000),
            time_range_end: Some(2_000),
        };

        let metastore_query = to_metastore_query(&index_uid, &query);

        assert_eq!(metastore_query.time_range.start, Bound::Excluded(1_000));
        assert_eq!(metastore_query.time_range.end, Bound::Excluded(2_000));
        assert_eq!(metastore_query.metric_names, vec!["cpu.usage".to_string()]);
    }
}
