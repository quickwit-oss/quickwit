use quickwit_proto::cloudprem::Aggregation as EvpAggregation;
use tantivy::aggregation::agg_req::Aggregations as TantivyAggregations;

use crate::InvalidQuery;

pub fn to_tantivy_aggregation(
    _cloudprem_aggregation: EvpAggregation,
) -> Result<TantivyAggregations, InvalidQuery> {
    Err(anyhow::anyhow!("unsupported yet").into())
}
