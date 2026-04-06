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


use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use quickwit_metastore::{
    ListMetricsSplitsQuery, ListMetricsSplitsRequestExt, ListMetricsSplitsResponseExt,
};
use quickwit_parquet_engine::split::MetricsSplitMetadata;
use quickwit_proto::metastore::{
    ListMetricsSplitsRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use tracing::{debug, instrument};

use super::predicate::MetricsSplitQuery;
use super::table_provider::MetricsSplitProvider;

/// `MetricsSplitProvider` backed by the Quickwit metastore RPC.
#[derive(Debug, Clone)]
pub struct MetastoreSplitProvider {
    metastore: MetastoreServiceClient,
    index_uid: IndexUid,
}

impl MetastoreSplitProvider {
    pub fn new(metastore: MetastoreServiceClient, index_uid: IndexUid) -> Self {
        Self {
            metastore,
            index_uid,
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
            num_splits,
        )
    )]
    async fn list_splits(
        &self,
        query: &MetricsSplitQuery,
    ) -> DFResult<Vec<MetricsSplitMetadata>> {
        let metastore_query = to_metastore_query(&self.index_uid, query);

        let request =
            ListMetricsSplitsRequest::try_from_query(self.index_uid.clone(), &metastore_query)
                .map_err(|err| {
                    datafusion::error::DataFusionError::External(Box::new(err))
                })?;

        let response = self
            .metastore
            .clone()
            .list_metrics_splits(request)
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let records = response
            .deserialize_splits()
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        // The metastore guarantees only Published splits are returned because
        // `to_metastore_query` sets `split_states = vec![Published]`. No
        // client-side re-filter is needed here.
        let splits: Vec<MetricsSplitMetadata> = records
            .into_iter()
            .map(|record| record.metadata)
            .collect();

        tracing::Span::current().record("num_splits", splits.len());
        debug!(num_splits = splits.len(), "metastore returned splits");

        Ok(splits)
    }
}

/// Convert a DataFusion `MetricsSplitQuery` to a metastore `ListMetricsSplitsQuery`.
///
/// Note: The OSS parquet column names are bare (service, env, etc.) but the
/// metastore `ListMetricsSplitsQuery` still uses the `tag_service`, `tag_env`
/// field names — this is just the metastore's internal naming convention.
///
/// # Tag field pushdown limitation
///
/// `ListMetricsSplitsQuery` accepts at most one value per tag field
/// (`Option<String>`). When a DataFusion `IN (...)` predicate produces
/// multiple candidate values for a tag column, the metastore cannot express
/// the full filter, so **no metastore-level pruning is applied for that
/// dimension** — the value is left as `None`. The parquet-level filter
/// (applied after the split is opened) will still enforce the predicate
/// correctly. Only single-value equalities (`WHERE service = 'web'`) or
/// single-element IN lists are pushed down to the metastore.
fn to_metastore_query(index_uid: &IndexUid, query: &MetricsSplitQuery) -> ListMetricsSplitsQuery {
    let mut metastore_query = ListMetricsSplitsQuery::for_index(index_uid.clone());

    if let Some(ref names) = query.metric_names {
        metastore_query.metric_names = names.clone();
    }

    if let Some(start) = query.time_range_start {
        metastore_query.time_range_start = Some(start as i64);
    }

    if let Some(end) = query.time_range_end {
        metastore_query.time_range_end = Some(end as i64);
    }

    // Push down a tag filter to the metastore only when there is exactly one
    // candidate value. Multi-value IN lists cannot be expressed as a single
    // `Option<String>` on `ListMetricsSplitsQuery`; passing only the first
    // value would silently skip splits that match the other values, producing
    // incorrect (incomplete) results. For multi-value lists we pass `None`
    // (no metastore pruning) and rely on the parquet-level filter instead.
    metastore_query.tag_service = single_value(query.tag_service.as_deref());
    metastore_query.tag_env = single_value(query.tag_env.as_deref());
    metastore_query.tag_datacenter = single_value(query.tag_datacenter.as_deref());
    metastore_query.tag_region = single_value(query.tag_region.as_deref());
    metastore_query.tag_host = single_value(query.tag_host.as_deref());

    metastore_query
}

/// Returns the single element of `values` as `Some(value)`, or `None` if
/// `values` is absent, empty, or contains more than one element.
fn single_value(values: Option<&[String]>) -> Option<String> {
    match values {
        Some([single]) => Some(single.clone()),
        _ => None,
    }
}

