---
title: Aggregation
sidebar_position: 11
---

An aggregation summarizes your data as statistics on buckets or metrics.

Aggregations can provide answer to questions like:

- What is the average price of all sold articles?
- How many errors with status code 500 do we have per day?
- What is the average listing price of cars grouped by color?

There are two categories: [Metrics](#metric) and [Buckets](#bucket-aggregations).

#### Prerequisite

To be able to use aggregations on a field, the field needs to have a fast field index created. A fast field index is a columnar storage, 
where documents values are extracted and stored to.

Example to create a fast field on text for term aggregations.
```yaml
name: category
type: text
tokenizer: raw
record: basic
fast: true
```

See the [index config](./index-config.md) for more details and examples.

#### Format

The aggregation request and result de/serialize into elasticsearch compatible JSON. 
If not documented otherwise you should be able to drop in your elasticsearch aggregation queries.

In some examples below is not the full request shown, but only the payload for `aggregations`.

#### Example

Request
```json
{
  "query": "*",
  "max_hits": 0,
  "aggregations": {
    "sites_and_aqi": {
      "terms": {
        "field": "County",
        "size": 2,
        "order": {"average_aqi": "asc"}
      },
      "aggs": {
        "average_aqi": {
          "avg": {
            "field": "AQI"
          }
        }
      }
    }
  }
}
```


Response
```json
...
"aggregations": {
    "sites_and_aqi": {
      "buckets": [
        {
          "average_aqi": {
            "value": 32.62267569707098
          },
          "doc_count": 56845,
          "key": "臺東縣"
        },
        {
          "average_aqi": {
            "value": 35.97893635571055
          },
          "doc_count": 28675,
          "key": "花蓮縣"
        }
      ],
      "sum_other_doc_count": 1872055
    }
}
```
#### Limitations

Currently aggregations work only on single value fast fields of type u64, f64, i64 and on string fields.



### Supported Aggregations

 - Bucket
    - [Histogram](#histogram)
    - [Range](#range)
    - [Terms](#terms)
- Metric
    - [Average](#average)
    - [Stats](#stats)


## Bucket Aggregations

BucketAggregations create buckets of documents. Each bucket is associated with a rule which determines whether or not a document falls into it. 
In other words, the buckets effectively define document sets. Buckets are not necessarily disjunct, therefore a document can fall into multiple buckets. 
In addition to the buckets themselves, the bucket aggregations also compute and return the number of documents for each bucket. 
Bucket aggregations, as opposed to metric aggregations, can hold sub-aggregations. 
These sub-aggregations will be aggregated for the buckets created by their “parent” bucket aggregation. 
There are different bucket aggregators, each with a different “bucketing” strategy. 
Some define a single bucket, some define fixed number of multiple buckets, and others dynamically create the buckets during the aggregation process.

Example request, histogram with stats in each bucket:
```json
{
  "query": "*",
  "max_hits": 1,
  "aggregations": {
    "stats_per_day": {
      "histogram": {
        "field": "timestamp",
        "interval": 86400
      },
      "aggs": {
        "timestamp_stats": {
          "stats": {
            "field": "timestamp"
          }
        }
      }
    }
  }
}
```

#### Limitations/Compatibility

Currently aggregations work only on single value fast fields of type u64, f64 and i64.

The keyed parameter (elasticsearch) is not yet supported.

### Histogram

Histogram is a bucket aggregation, where buckets are created dynamically for the given interval. Each document value is rounded down to its bucket.

E.g. if we have a price 18 and an interval of 5, the document will fall into the bucket with the key 15. The formula used for this is: ((val - offset) / interval).floor() * interval + offset.


#### Returned Buckets

By default buckets are returned between the min and max value of the documents, including empty buckets. Setting min_doc_count to != 0 will filter empty buckets.

The value range of the buckets can bet extended via extended_bounds or limit the range via hard_bounds.


#### Example

```json
{
    "prices": {
        "histogram": {
            "field": "price",
            "interval": 10
        }
    }
}
```

#### Parameters

###### **field**

The field to aggregate on.

###### **interval**

The interval to chunk your data range. Each bucket spans a value range of [0..interval). Must be larger than 0.

###### **offset**

Intervals implicitely defines an absolute grid of buckets `[interval * k, interval * (k + 1))`.
Offset makes it possible to shift this grid into `[offset + interval * k, offset + interval (k + 1))`. Offset has to be in the range [0, interval).

As an example, if there are two documents with value 8 and 12 and interval 10.0, they would fall into the buckets with the key 0 and 10. With offset 5 and interval 10, they would both fall into the bucket with they key 5 and the range [5..15)

```json
{
    "prices": {
        "histogram": {
            "field": "price",
            "interval": 10,
            "offset": 2.5
        }
    }
}
```


###### **min_doc_count**

The minimum number of documents in a bucket to be returned. Defaults to 0.

###### **hard_bounds**

Limits the data range to [min, max] closed interval.
This can be used to filter values if they are not in the data range.
hard_bounds only limits the buckets, to force a range set both extended_bounds and hard_bounds to the same range.

```json
{
    "prices": {
        "histogram": {
            "field": "price",
            "interval": 10,
            "hard_bounds": {
                "min": 0,
                "max": 100
            }
        }
    }
}
```

###### **extended_bounds**

Can be set to extend your bounds. The range of the buckets is by default defined by the data range of the values of the documents. As the name suggests, this can only be used to extend the value range. If the bounds for min or max are not extending the range, the value has no effect on the returned buckets.
Cannot be set in conjunction with min_doc_count > 0, since the empty buckets from extended bounds would not be returned.

```json
{
    "prices": {
        "histogram": {
            "field": "price",
            "interval": 10,
            "extended_bounds": {
                "min": 0,
                "max": 100
            }
        }
    }
}
```


### Range

Provide user-defined buckets to aggregate on. Two special buckets will automatically be created to cover the whole range of values.
The provided buckets have to be continous. During the aggregation, the values extracted from the fast_field field will be checked against each bucket range.
Note that this aggregation includes the from value and excludes the to value for each range.

```json
{
    "my_ranges": {
        "field": "score",
        "ranges": [
            { "to": 3.0 },
            { "from": 3.0, "to": 7.0 },
            { "from": 7.0, "to": 20.0 },
            { "from": 20.0 }
        ]
    }
}
```

#### Limitations/Compatibility

Overlapping ranges are not yet supported.

The keyed parameter (elasticsearch) is not yet supported.


#### Parameters

###### **field**

The field to aggregate on.

###### **ranges**

The list of buckets, with `from` and `to` values. 
The from value is inclusive in the range.
The to value is not inclusive in the range.

The first bucket can omit the `from` value, and the last bucket the `to` value.
Note that this aggregation includes the `from` value and excludes the `to` value for each range. Extra buckets will be created until the first `to`, and last `from`, if necessary.


### Terms

Creates a bucket for every unique term.

```json
{
    "genres": {
        "terms":{ "field": "genre" }
    }
}
```

#### Document count error
In quickwit we have one segment per split.
To improve performance, results from one split are cut off at `segment_size`.
When combining results of multiple splits, terms that
don't make it in the top n of a result from a split increase the theoretical upper bound error by lowest
term-count.

Even with a larger `segment_size` value, doc_count values for a terms aggregation may be
approximate. As a result, any sub-aggregations on the terms aggregation may also be approximate.
`sum_other_doc_count` is the number of documents that didn’t make it into the the top size
terms. If this is greater than 0, you can be sure that the terms agg had to throw away some
buckets, either because they didn’t fit into `size` on the root node or they didn’t fit into
`segment_size` on the leaf node.

#### Per bucket document count error
If you set the `show_term_doc_count_error` parameter to true, the terms aggregation will include
doc_count_error_upper_bound, which is an upper bound to the error on the doc_count returned by
each segment. It’s the sum of the size of the largest bucket on each segment that didn’t fit
into segment_size.

#### Parameters

###### **field**

The field to aggregate on.

###### **size**

By default, the top 10 terms with the most documents are returned. Larger values for size are more expensive.

###### **segment_size**


The get more accurate results, we fetch more than size from each segment.
Increasing this value is will increase the cost for more accuracy.

Defaults to 10 * size.

###### **show_term_doc_count_error**

If you set the show_term_doc_count_error parameter to true, the terms aggregation will include doc_count_error_upper_bound, which is an upper bound to the error on the doc_count returned by each shard. 
It’s the sum of the size of the largest bucket on each segment that didn’t fit into shard_size.

Defaults to true when ordering by count desc.


###### **min_doc_count**

Filter all terms that are lower than `min_doc_count`. Defaults to 1.

_Expensive_ : When set to 0, this will return all terms in the field.


###### **order**

Set the order. String is here a target, which is either “_count”, “_key”, or the name of a metric sub_aggregation.
Single value metrics like average can be adressed by its name. Multi value metrics like stats are required to adress their field by name e.g. “stats.avg”


Order alphabetically
```json
{
    "genres": {
        "terms":{ "field": "genre" },
        "order":{ "_key": "asc" }
    }
}
```


Order by sub_aggregation

```json
{
    "articles_by_price": {
        "terms":{ "field": "article_name" },
        "order":{ “average_price”: “asc” },
        "aggs": {
          "average_price": {
            "avg": {
              "field": "price"
            }
          }
        }
    }
}
```



## Metric

The aggregations in this family compute metrics based on values extracted from the documents that are being aggregated.
Values are extracted from the fast field of the document. Some aggregations output a single numeric metric (e.g. Average)
and are called single-value numeric metrics aggregation, others generate multiple metrics (e.g. Stats) and are called multi-value numeric metrics aggregation.

In contrast to bucket aggregations, metrics don't allow sub-aggregations, since there is no document set to aggregate on.

### Average

A single-value metric aggregation that computes the average of numeric values that are extracted from the aggregated documents.
Supported field types are u64, i64, and f64. 


**Request**
```json
{
  "query": "*",
  "max_hits": 1,
  "aggregations": {
    "average_price": {
      "avg": {
        "field": "price"
      }
    }
  }
}

```

**Response**
```json
{
  "num_hits": 9582098,
  "hits": [],
  "elapsed_time_micros": 101942,
  "errors": [],
  "aggregations": {
    "average_in_range": {
      "value": 1462014948.102553
    }
  }
}


```

### Stats

A multi-value metric aggregation that computes stats of numeric values that are extracted from the aggregated documents. 
Supported field types are u64, i64, and f64. 

**Request**
```json
{
  "query": "*",
  "max_hits": 0,
  "aggregations": {
    "timestamp_stats": {
      "avg": {
        "field": "timestamp"
      }
    }
  }
}

```



**Response**
```json
{
  "num_hits": 10000783,
  "hits": [],
  "elapsed_time_micros": 65297,
  "errors": [],
  "aggregations": {
    "timestamp_stats": {
      "avg": 1462320207.9803998,
      "count": 10000783,
      "max": 1475669670.0,
      "min": 1440670432.0,
      "standard_deviation": 11867304.28681695,
      "sum": 1.4624347076526848e16
    }
  }
}
```


