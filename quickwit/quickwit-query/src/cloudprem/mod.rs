mod aggregation;
mod query;

pub use aggregation::{
    intermediate_aggregation_result_to_proto, sanitize_metric_id_aggregations,
    to_tantivy_aggregation,
};
pub use query::{parse_query, to_quickwit_query};

use crate::InvalidQuery;

// field is relative to the closest `node` (except when a `node` is what is missing)
fn missing_required(field: &str) -> InvalidQuery {
    InvalidQuery::Other(anyhow::anyhow!(
        "missing required field '{field}', this likely means a protobuf missmatch"
    ))
}

fn unsupported_query_error(feature: &str) -> InvalidQuery {
    InvalidQuery::Other(anyhow::anyhow!(
        "unsupported feature in CloudPrem: {feature}"
    ))
}

fn internal_error(msg: &str) -> InvalidQuery {
    InvalidQuery::Other(anyhow::anyhow!("internal error: {msg}"))
}
