use serde::{Deserialize, Serialize};

use super::{from_simple_list, to_simple_list, TrackTotalHits};
#[serde_with::skip_serializing_none]
#[derive(Default, Serialize, Deserialize)]
pub struct SearchQueryParams {
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    _source: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    _source_excludes: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    _source_includes: Option<Vec<String>>,
    allow_no_indices: Option<bool>,
    allow_partial_search_results: Option<bool>,
    analyze_wildcard: Option<bool>,
    analyzer: Option<String>,
    batched_reduce_size: Option<i64>,
    ccs_minimize_roundtrips: Option<bool>,
    default_operator: Option<DefaultOperator>,
    df: Option<String>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    docvalue_fields: Option<Vec<String>>,
    error_trace: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    expand_wildcards: Option<Vec<ExpandWildcards>>,
    explain: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    filter_path: Option<Vec<String>>,
    force_synthetic_source: Option<bool>,
    from: Option<i64>,
    human: Option<bool>,
    ignore_throttled: Option<bool>,
    ignore_unavailable: Option<bool>,
    lenient: Option<bool>,
    max_concurrent_shard_requests: Option<i64>,
    min_compatible_shard_node: Option<String>,
    pre_filter_shard_size: Option<i64>,
    preference: Option<String>,
    pretty: Option<bool>,
    q: Option<String>,
    request_cache: Option<bool>,
    rest_total_hits_as_int: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    routing: Option<Vec<String>>,
    scroll: Option<String>,
    search_type: Option<SearchType>,
    seq_no_primary_term: Option<bool>,
    size: Option<i64>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    sort: Option<Vec<String>>,
    source: Option<String>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    stats: Option<Vec<String>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    stored_fields: Option<Vec<String>>,
    suggest_field: Option<String>,
    suggest_mode: Option<SuggestMode>,
    suggest_size: Option<i64>,
    suggest_text: Option<String>,
    terminate_after: Option<i64>,
    timeout: Option<String>,
    track_scores: Option<bool>,
    track_total_hits: Option<TrackTotalHits>,
    typed_keys: Option<bool>,
    version: Option<bool>,
}
#[doc = "The default operator for query string query (AND or OR)"]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum DefaultOperator {
    #[serde(rename = "AND")]
    And,
    #[serde(rename = "OR")]
    Or,
}
#[doc = "Whether to expand wildcard expression to concrete indices that are open, closed or both."]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum ExpandWildcards {
    #[serde(rename = "open")]
    Open,
    #[serde(rename = "closed")]
    Closed,
    #[serde(rename = "hidden")]
    Hidden,
    #[serde(rename = "none")]
    None,
    #[serde(rename = "all")]
    All,
}
#[doc = "Search operation type"]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum SearchType {
    #[serde(rename = "query_then_fetch")]
    QueryThenFetch,
    #[serde(rename = "dfs_query_then_fetch")]
    DfsQueryThenFetch,
}
#[doc = "Specify suggest mode"]
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum SuggestMode {
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "popular")]
    Popular,
    #[serde(rename = "always")]
    Always,
}
