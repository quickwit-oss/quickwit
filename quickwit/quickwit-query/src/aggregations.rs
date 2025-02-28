use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tantivy::aggregation::agg_result::{
    AggregationResult as TantivyAggregationResult, AggregationResults as TantivyAggregationResults,
    BucketEntries as TantivyBucketEntries, BucketResult as TantivyBucketResult,
    MetricResult as TantivyMetricResult,
};
use tantivy::aggregation::metric::{
    ExtendedStats, PercentilesMetricResult, SingleMetricResult, Stats, TopHitsMetricResult,
};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
/// The final aggegation result.
pub struct AggregationResults(pub FxHashMap<String, AggregationResult>);

impl From<TantivyAggregationResults> for AggregationResults {
    fn from(value: TantivyAggregationResults) -> AggregationResults {
        AggregationResults(value.0.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl From<AggregationResults> for TantivyAggregationResults {
    fn from(value: AggregationResults) -> TantivyAggregationResults {
        TantivyAggregationResults(value.0.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// An aggregation is either a bucket or a metric.
pub enum AggregationResult {
    /// Bucket result variant.
    BucketResult(BucketResult),
    /// Metric result variant.
    MetricResult(MetricResult),
}

impl From<TantivyAggregationResult> for AggregationResult {
    fn from(value: TantivyAggregationResult) -> AggregationResult {
        match value {
            TantivyAggregationResult::BucketResult(bucket) => {
                AggregationResult::BucketResult(bucket.into())
            }
            TantivyAggregationResult::MetricResult(metric) => {
                AggregationResult::MetricResult(metric.into())
            }
        }
    }
}

impl From<AggregationResult> for TantivyAggregationResult {
    fn from(value: AggregationResult) -> TantivyAggregationResult {
        match value {
            AggregationResult::BucketResult(bucket) => {
                TantivyAggregationResult::BucketResult(bucket.into())
            }
            AggregationResult::MetricResult(metric) => {
                TantivyAggregationResult::MetricResult(metric.into())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// MetricResult
pub enum MetricResult {
    /// Average metric result.
    Average(SingleMetricResult),
    /// Count metric result.
    Count(SingleMetricResult),
    /// Max metric result.
    Max(SingleMetricResult),
    /// Min metric result.
    Min(SingleMetricResult),
    /// Stats metric result.
    Stats(Stats),
    /// ExtendedStats metric result.
    ExtendedStats(Box<ExtendedStats>),
    /// Sum metric result.
    Sum(SingleMetricResult),
    /// Percentiles metric result.
    Percentiles(PercentilesMetricResult),
    /// Top hits metric result
    TopHits(TopHitsMetricResult),
    /// Cardinality metric result
    Cardinality(SingleMetricResult),
}

impl From<TantivyMetricResult> for MetricResult {
    fn from(value: TantivyMetricResult) -> MetricResult {
        match value {
            TantivyMetricResult::Average(val) => MetricResult::Average(val),
            TantivyMetricResult::Count(val) => MetricResult::Count(val),
            TantivyMetricResult::Max(val) => MetricResult::Max(val),
            TantivyMetricResult::Min(val) => MetricResult::Min(val),
            TantivyMetricResult::Stats(val) => MetricResult::Stats(val),
            TantivyMetricResult::ExtendedStats(val) => MetricResult::ExtendedStats(val),
            TantivyMetricResult::Sum(val) => MetricResult::Sum(val),
            TantivyMetricResult::Percentiles(val) => MetricResult::Percentiles(val),
            TantivyMetricResult::TopHits(val) => MetricResult::TopHits(val),
            TantivyMetricResult::Cardinality(val) => MetricResult::Cardinality(val),
        }
    }
}

impl From<MetricResult> for TantivyMetricResult {
    fn from(value: MetricResult) -> TantivyMetricResult {
        match value {
            MetricResult::Average(val) => TantivyMetricResult::Average(val),
            MetricResult::Count(val) => TantivyMetricResult::Count(val),
            MetricResult::Max(val) => TantivyMetricResult::Max(val),
            MetricResult::Min(val) => TantivyMetricResult::Min(val),
            MetricResult::Stats(val) => TantivyMetricResult::Stats(val),
            MetricResult::ExtendedStats(val) => TantivyMetricResult::ExtendedStats(val),
            MetricResult::Sum(val) => TantivyMetricResult::Sum(val),
            MetricResult::Percentiles(val) => TantivyMetricResult::Percentiles(val),
            MetricResult::TopHits(val) => TantivyMetricResult::TopHits(val),
            MetricResult::Cardinality(val) => TantivyMetricResult::Cardinality(val),
        }
    }
}

/// BucketEntry holds bucket aggregation result types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BucketResult {
    /// This is the range entry for a bucket, which contains a key, count, from, to, and optionally
    /// sub-aggregations.
    Range {
        /// The range buckets sorted by range.
        buckets: BucketEntries<RangeBucketEntry>,
    },
    /// This is the histogram entry for a bucket, which contains a key, count, and optionally
    /// sub-aggregations.
    Histogram {
        /// The buckets.
        ///
        /// If there are holes depends on the request, if min_doc_count is 0, then there are no
        /// holes between the first and last bucket.
        /// See [`HistogramAggregation`](super::bucket::HistogramAggregation)
        buckets: BucketEntries<BucketEntry>,
    },
    /// This is the term result
    Terms {
        /// The buckets.
        ///
        /// See [`TermsAggregation`](super::bucket::TermsAggregation)
        buckets: Vec<BucketEntry>,
        /// The number of documents that didn’t make it into to TOP N due to shard_size or size
        sum_other_doc_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// The upper bound error for the doc count of each term.
        doc_count_error_upper_bound: Option<u64>,
    },
}

impl From<TantivyBucketResult> for BucketResult {
    fn from(value: TantivyBucketResult) -> BucketResult {
        match value {
            TantivyBucketResult::Range { buckets } => BucketResult::Range {
                buckets: buckets.into(),
            },
            TantivyBucketResult::Histogram { buckets } => BucketResult::Histogram {
                buckets: buckets.into(),
            },
            TantivyBucketResult::Terms {
                buckets,
                sum_other_doc_count,
                doc_count_error_upper_bound,
            } => BucketResult::Terms {
                buckets,
                sum_other_doc_count,
                doc_count_error_upper_bound,
            },
        }
    }
}

impl From<BucketResult> for TantivyBucketResult {
    fn from(value: BucketResult) -> TantivyBucketResult {
        match value {
            BucketResult::Range { buckets } => TantivyBucketResult::Range {
                buckets: buckets.into(),
            },
            BucketResult::Histogram { buckets } => TantivyBucketResult::Histogram {
                buckets: buckets.into(),
            },
            BucketResult::Terms {
                buckets,
                sum_other_doc_count,
                doc_count_error_upper_bound,
            } => TantivyBucketResult::Terms {
                buckets,
                sum_other_doc_count,
                doc_count_error_upper_bound,
            },
        }
    }
}

/// This is the wrapper of buckets entries, which can be vector or hashmap
/// depending on if it's keyed or not.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BucketEntries<T> {
    /// Vector format bucket entries
    Vec(Vec<T>),
    /// HashMap format bucket entries
    HashMap(FxHashMap<String, T>),
}

impl<T> From<TantivyBucketEntries<T>> for BucketEntries<T> {
    fn from(value: TantivyBucketEntries<T>) -> BucketEntries<T> {
        match value {
            TantivyBucketEntries::Vec(vec) => BucketEntries::Vec(vec),
            TantivyBucketEntries::HashMap(map) => BucketEntries::HashMap(map),
        }
    }
}

impl<T> From<BucketEntries<T>> for TantivyBucketEntries<T> {
    fn from(value: BucketEntries<T>) -> TantivyBucketEntries<T> {
        match value {
            BucketEntries::Vec(vec) => TantivyBucketEntries::Vec(vec),
            BucketEntries::HashMap(map) => TantivyBucketEntries::HashMap(map),
        }
    }
}

pub type BucketEntry = tantivy::aggregation::agg_result::BucketEntry;
pub type RangeBucketEntry = tantivy::aggregation::agg_result::RangeBucketEntry;
