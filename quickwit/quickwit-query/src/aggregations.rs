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

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tantivy::aggregation::Key as TantivyKey;
use tantivy::aggregation::agg_result::{
    AggregationResult as TantivyAggregationResult, AggregationResults as TantivyAggregationResults,
    BucketEntries as TantivyBucketEntries, BucketEntry as TantivyBucketEntry,
    BucketResult as TantivyBucketResult, MetricResult as TantivyMetricResult,
    RangeBucketEntry as TantivyRangeBucketEntry,
};
use tantivy::aggregation::metric::{
    ExtendedStats, PercentileValues as TantivyPercentileValues, PercentileValuesVecEntry,
    PercentilesMetricResult as TantivyPercentilesMetricResult, SingleMetricResult, Stats,
    TopHitsMetricResult,
};

// hopefully all From in this module are no-ops, otherwise, this is a very sad situation

#[derive(Clone, Debug, Serialize, Deserialize)]
/// The final aggregation result.
pub struct AggregationResults(pub Vec<(String, AggregationResult)>);

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

#[derive(Clone, Debug, Serialize, Deserialize)]
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
            TantivyMetricResult::Percentiles(val) => MetricResult::Percentiles(val.into()),
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
            MetricResult::Percentiles(val) => TantivyMetricResult::Percentiles(val.into()),
            MetricResult::TopHits(val) => TantivyMetricResult::TopHits(val),
            MetricResult::Cardinality(val) => TantivyMetricResult::Cardinality(val),
        }
    }
}

/// BucketEntry holds bucket aggregation result types.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
        /// See `HistogramAggregation`
        buckets: BucketEntries<BucketEntry>,
    },
    /// This is the term result
    Terms {
        /// The buckets.
        ///
        /// See `TermsAggregation`
        buckets: Vec<BucketEntry>,
        /// The number of documents that didn’t make it into to TOP N due to shard_size or size
        sum_other_doc_count: u64,
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
                buckets: buckets.into_iter().map(Into::into).collect(),
                sum_other_doc_count,
                doc_count_error_upper_bound,
            },
            TantivyBucketResult::Filter(_filter_bucket_result) => {
                unimplemented!("filter aggregation is not yet supported in quickwit")
            }
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
                buckets: buckets.into_iter().map(Into::into).collect(),
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
    HashMap(Vec<(String, T)>),
}

impl<T, U> From<TantivyBucketEntries<T>> for BucketEntries<U>
where U: From<T>
{
    fn from(value: TantivyBucketEntries<T>) -> BucketEntries<U> {
        match value {
            TantivyBucketEntries::Vec(vec) => {
                BucketEntries::Vec(vec.into_iter().map(Into::into).collect())
            }
            TantivyBucketEntries::HashMap(map) => {
                BucketEntries::HashMap(map.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
        }
    }
}

impl<T, U> From<BucketEntries<T>> for TantivyBucketEntries<U>
where U: From<T>
{
    fn from(value: BucketEntries<T>) -> TantivyBucketEntries<U> {
        match value {
            BucketEntries::Vec(vec) => {
                TantivyBucketEntries::Vec(vec.into_iter().map(Into::into).collect())
            }
            BucketEntries::HashMap(map) => {
                TantivyBucketEntries::HashMap(map.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RangeBucketEntry {
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    /// Sub-aggregations in this bucket.
    // here we had a flatten, postcard didn't like that (unknown map size)
    pub sub_aggregation: AggregationResults,
    /// The from range of the bucket. Equals `f64::MIN` when `None`.
    pub from: Option<f64>,
    /// The to range of the bucket. Equals `f64::MAX` when `None`.
    pub to: Option<f64>,
    /// The optional string representation for the `from` range.
    pub from_as_string: Option<String>,
    /// The optional string representation for the `to` range.
    pub to_as_string: Option<String>,
}

impl From<TantivyRangeBucketEntry> for RangeBucketEntry {
    fn from(value: TantivyRangeBucketEntry) -> RangeBucketEntry {
        RangeBucketEntry {
            key: value.key.into(),
            doc_count: value.doc_count,
            from: value.from,
            to: value.to,
            from_as_string: value.from_as_string,
            to_as_string: value.to_as_string,
            sub_aggregation: value.sub_aggregation.into(),
        }
    }
}

impl From<RangeBucketEntry> for TantivyRangeBucketEntry {
    fn from(value: RangeBucketEntry) -> TantivyRangeBucketEntry {
        TantivyRangeBucketEntry {
            key: value.key.into(),
            doc_count: value.doc_count,
            from: value.from,
            to: value.to,
            from_as_string: value.from_as_string,
            to_as_string: value.to_as_string,
            sub_aggregation: value.sub_aggregation.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketEntry {
    /// The string representation of the bucket.
    pub key_as_string: Option<String>,
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    /// Sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
}

impl From<TantivyBucketEntry> for BucketEntry {
    fn from(value: TantivyBucketEntry) -> BucketEntry {
        BucketEntry {
            key_as_string: value.key_as_string,
            key: value.key.into(),
            doc_count: value.doc_count,
            sub_aggregation: value.sub_aggregation.into(),
        }
    }
}

impl From<BucketEntry> for TantivyBucketEntry {
    fn from(value: BucketEntry) -> TantivyBucketEntry {
        TantivyBucketEntry {
            key_as_string: value.key_as_string,
            key: value.key.into(),
            doc_count: value.doc_count,
            sub_aggregation: value.sub_aggregation.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Key {
    /// String key
    Str(String),
    /// `i64` key
    I64(i64),
    /// `u64` key
    U64(u64),
    /// `f64` key
    F64(f64),
}

impl From<TantivyKey> for Key {
    fn from(value: TantivyKey) -> Key {
        match value {
            TantivyKey::Str(s) => Key::Str(s),
            TantivyKey::I64(i) => Key::I64(i),
            TantivyKey::U64(u) => Key::U64(u),
            TantivyKey::F64(f) => Key::F64(f),
        }
    }
}

impl From<Key> for TantivyKey {
    fn from(value: Key) -> TantivyKey {
        match value {
            Key::Str(s) => TantivyKey::Str(s),
            Key::I64(i) => TantivyKey::I64(i),
            Key::U64(u) => TantivyKey::U64(u),
            Key::F64(f) => TantivyKey::F64(f),
        }
    }
}

/// Single-metric aggregations use this common result structure.
///
/// Main reason to wrap it in value is to match elasticsearch output structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PercentilesMetricResult {
    /// The result of the percentile metric.
    pub values: PercentileValues,
}

/// This is the wrapper of percentile entries, which can be vector or hashmap
/// depending on if it's keyed or not.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PercentileValues {
    /// Vector format percentile entries
    Vec(Vec<PercentileValuesVecEntry>),
    /// HashMap format percentile entries. Key is the serialized percentile
    // we use a hashmap here because neither key nor value require conversion, almost
    // all usage of PercentileValues will be direct conversion to TantivyPercentilesValue
    HashMap(FxHashMap<String, f64>),
}

impl From<TantivyPercentilesMetricResult> for PercentilesMetricResult {
    fn from(value: TantivyPercentilesMetricResult) -> PercentilesMetricResult {
        let values = match value.values {
            TantivyPercentileValues::Vec(vec) => PercentileValues::Vec(vec),
            TantivyPercentileValues::HashMap(map) => PercentileValues::HashMap(map),
        };
        PercentilesMetricResult { values }
    }
}

impl From<PercentilesMetricResult> for TantivyPercentilesMetricResult {
    fn from(value: PercentilesMetricResult) -> TantivyPercentilesMetricResult {
        let values = match value.values {
            PercentileValues::Vec(vec) => TantivyPercentileValues::Vec(vec),
            PercentileValues::HashMap(map) => TantivyPercentileValues::HashMap(map),
        };
        TantivyPercentilesMetricResult { values }
    }
}
