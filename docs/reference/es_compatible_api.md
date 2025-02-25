---
title: Elasticsearch compatible API
sidebar_position: 20
---


In order to facilitate migrations and integrations with existing tools,
Quickwit offers an Elasticsearch/Opensearch compatible API.
This API is incomplete. This page lists the available features and endpoints.

## Supported endpoints

All the API endpoints start with the `api/v1/_elastic/` prefix.

### `_bulk` &nbsp; Batch ingestion endpoint

```
POST api/v1/_elastic/_bulk
```
```
POST api/v1/_elastic/<index>/_bulk
```

The _bulk ingestion API makes it possible to index a batch of documents, possibly targeting several indices in the same request.

#### Request Body example

```json
{ "create" : { "_index" : "wikipedia", "_id" : "1" } }
{"url":"https://en.wikipedia.org/wiki?id=1","title":"foo","body":"foo"}
{ "create" : { "_index" : "wikipedia", "_id" : "2" } }
{"url":"https://en.wikipedia.org/wiki?id=2","title":"bar","body":"bar"}
{ "create" : { "_index" : "wikipedia", "_id" : "3" } }
{"url":"https://en.wikipedia.org/wiki?id=3","title":"baz","body":"baz"}'
```

Ingest a batch of documents to make them searchable using the [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) bulk API. This endpoint provides compatibility with tools or systems that already send data to Elasticsearch for indexing. Currently, only the `create` action of the bulk API is supported, all other actions such as `delete` or `update` are ignored.

If an index is specified via the url path, it will act as a default value
for the `_index` properties.

The [`refresh`](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html) parameter is supported.

:::caution
The quickwit API will not report errors, you need to check the server logs.

In Elasticsearch, the `create` action has a specific behavior when the ingested documents contain an identifier (the `_id` field). It only inserts such a document if it was not inserted before. This is extremely handy to achieve At-Most-Once indexing.
Quickwit does not have any notion of document id and does not support this feature.
:::

:::info
The payload size is limited to 10MB as this endpoint is intended to receive documents in batch.
:::

#### Query parameter

| Variable  | Type     | Description                                                      | Default value |
| --------- | -------- | ---------------------------------------------------------------- | ------------- |
| `refresh` | `String` | The commit behavior: blank string, `true`, `wait_for` or `false` | `false`       |

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field                     | Description                                                                                                                                                              |   Type   |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------: |
| `num_docs_for_processing` | Total number of documents ingested for processing. The documents may not have been processed. The API will not return indexing errors, check the server logs for errors. | `number` |




### `_search` &nbsp; Index search endpoint

```
POST api/v1/_elastic/<index_id>/_search
```
```
GET api/v1/_elastic/<index_id>/_search
```

#### Request Body example

```json
{
  "size": 10,
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "bitpacking"
          }
        },
        {
          "term": {
            "actor.login": {
              "value": "fulmicoton"
            }
          }
        }
      ]
    }
  },
  "sort": [
    {
      "actor.id": {
        "order": null
      }
    }
  ],
  "aggs": {
    "event_types": {
      "terms": {
        "field": "type",
        "size": 5
      }
    }
  }
}
```

Search into a specific index using the [Elasticsearch search API](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/search-search.html).

Some of the parameter can be passed as query string parameter, and some via JSON payload.
If a parameter appears both as a query string parameter and in the JSON payload, the query string parameter value will take priority.

#### Supported Query string parameters


| Variable           | Type          | Description                                                                      | Default value |
| ------------------ | ------------- | -------------------------------------------------------------------------------- | ------------- |
| `default_operator` | `AND` or `OR` | The default operator used to combine search terms. It should be `AND` or `OR`.   | `OR`          |
| `from`             | `Integer`     | The rank of the first hit to return. This is useful for pagination.              | 0             |
| `q`                | `String`      | The search query.                                                                | (Optional)    |
| `size`             | `Integer`     | Number of hits to return.                                                        | 10            |
| `sort`             | `String`      | Describes how documents should be ranked. See [Sort order](#sort-order)          | (Optional)    |
| `scroll`           | `Duration`    | Creates a scroll context for "time to live". See [Scroll](#_scroll--scroll-api). | (Optional)    |
| `allow_partial_search_results` | `Boolean` | Returns a partial response if some (but not all) of the split searches were unsuccessful. | `true` |

#### Supported Request Body parameters

| Variable           | Type              | Description                                                                    | Default value |
| ------------------ | ----------------- | ------------------------------------------------------------------------------ | ------------- |
| `default_operator` | `"AND"` or `"OR"` | The default operator used to combine search terms. It should be `AND` or `OR`. | `OR`          |
| `from`             | `Integer`         | The rank of the first hit to return. This is useful for pagination.            | 0             |
| `query`            | `Json object`     | Describe the search query. See [Query DSL](#query-dsl)                         | (Optional)    |
| `size`             | `Integer`         | Number of hits to return.                                                      | 10            |
| `sort`             | `JsonObject[]`    | Describes how documents should be ranked. See [Sort order](#sort-order)        | `[]`          |
| `search_after`     | `Any[]`           | Ignore documents with a SortingValue preceding or equal to the parameter       | (Optional)    |
| `aggs`             | `Json object`     | Aggregation definition. See [Aggregations](aggregation.md).                    | `{}`          |


#### Sort order

You can define up to two criteria on which to apply sort.
The second criterion will only be used in presence of a tie for the first criterion.

A given criterion can either be
- the name of a fast field (explicitly defined in the schema or captured by the dynamic mode)
- `_score` to sort by BM25.

By default, the sort order is `ascending` for fast fields and descending for `_score`.

When sorting by a fast field and this field contains several values in a single document, only the first value is used for sorting.

The sort order can be set as descending/ascending using the
following syntax.

```json
{
  // ...
  "sort" : [
    { "timestamp" : {"order" : "asc"}},
    { "serial_number" : "desc" }
  ]
  // ...
}

```

It is also possible to not supply an order and rely on the default order using the following syntax.

```json
{ //...
  "sort" : ["_score", "timestamp"]
  // ...
}
```

If no format is provided for timestamps, timestamps are returned with milliseconds precision.

If you need nanosecond precision, you can use the `epoch_nanos_int` format. Beware this means the resulting
JSON may contain high numbers for which there is loss of precision when using languages where all numbers are
floats, such as JavaScript.

```json
{
  // ...
  "sort" : [
    { "timestamp" : {"format": "epoch_nanos_int","order" : "asc"}},
    { "serial_number" : "desc" }
  ]
  // ...
}

#### Search after

When sorting results, the answer looks like the following

```json
{
  // ...
  "hits": {
    // ...
    "hits": [
      // ...
      {
        // ...
        "sort": [
          1701962929199
        ]
      }
    ]
  }
}
```

You can pass the `sort` value of the last hit in a subsequent request where other fields are kept unchanged:
```json
{
  // keep all fields from the original request
  "search_after": [
    1701962929199
  ]
}
```

This allows you to paginate your results.

### `_msearch` &nbsp; Multi search API

```
POST api/v1/_elastic/_msearch
```

#### Request Body example

```json
{"index": "gharchive" }
{"query" : {"match" : { "author.login": "fulmicoton"}}}
{"index": "gharchive"}
{"query" : {"match_all" : {}}}
```

[Multi search endpoint ES API reference](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/search-multi-search.html)

Runs several search requests at once.

The payload is expected to alternate:
- a `header` json object, containing the targeted index id.
- a `search request body` as defined in the [`_search` endpoint section].


### `_search/scroll` &nbsp; Scroll API

```
GET api/v1/_elastic/_search/scroll
```

#### Supported Request Body parameters

| Variable    | Type                                        | Description | Default value |
| ----------- | ------------------------------------------- | ----------- | ------------- |
| `scroll_id` | Scroll id (obtained from a search response) | Required    |               |


The `_search/scroll` endpoint, in combination with the `_search` API makes it possible to request successive pages of search results.
First, the client needs to call the `search api` with a `scroll` query parameter, and then pass the `scroll_id` returned in the response payload to  `_search/scroll` endpoint.

Each subsequent call to the `_search/scroll` endpoint will return a new `scroll_id` pointing to the next page.


### `_cat` &nbsp; Cat API

```
GET api/v1/_elastic/_cat/indices/<index>
```
```
GET api/v1/_elastic/_cat/indices
```

#### Supported Query string parameters

| Variable | Type       | Description                                                                                            | Default value |
|----------|------------|--------------------------------------------------------------------------------------------------------|---------------|
| `format` | `String`   | Format for response. Only JSON supported for now.                                                      |               |
| `h`      | `String[]` | Comma-separated list of column names to display.                                                       | (Optional)    |
| `health` | `String`   | Filter for health: `green`, `yellow`, or `red`.                                                        | (Optional)    |
| `bytes`  | `String`   | Unit used to display byte values. Unsupported for now.                                                 | (Optional)    |
| `s`      | `String`   | Comma-separated list of column names or column aliases used to sort the response. Unsupported for now. | (Optional)    |
| `v`      | `Boolean`  | If true, the response includes column headings. Unsupported for now.                                   | (Optional)    |

Use the [cat indices API](https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html) to get the following information for each index in a cluster:
* Shard count
* Document count
* Deleted document count
* Primary store size
* Total store size

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field            | Description                                      |   Type   |
|------------------|--------------------------------------------------|:--------:|
| `uuid`           | Index uuid                                       | `String` |
| `index`          | Index name                                       | `String` |
| `health`         | Health of the index `green`, `yellow`, or `red`. | `String` |
| `status`         | Status of the index `open`.                      | `String` |
| `rep`            | Replication factor.                              | `Number` |
| `pri`            | Number of primary shards                         | `Number` |
| `pri.store.size` | Stored size of primary shard.                    | `String` |
| `store.size`     | Stored size of index.                            | `String` |
| `dataset.size`   | Indexed data size.                               | `String` |
| `docs.count`     | Number of records in index.                      | `Number` |
| `docs.deleted`   | Number of deleted records in index.              | `Number` |

Example response:

```json
[
  {
    "dataset.size": "0b",
    "docs.count": "0",
    "docs.deleted": "0",
    "health": "green",
    "index": "otel-traces-v0_7",
    "pri": "1",
    "pri.store.size": "0b",
    "rep": "1",
    "status": "open",
    "store.size": "0b",
    "uuid": "otel-traces-v0_7:01HTJC6TQDGM07KBDQZ2KDHW53"
  },
  {
    "dataset.size": "387.5gb",
    "docs.count": "224453081",
    "docs.deleted": "0",
    "health": "green",
    "index": "otel-logs-v0_7",
    "pri": "1",
    "pri.store.size": "37.5gb",
    "rep": "1",
    "status": "open",
    "store.size": "37.5gb",
    "uuid": "otel-logs-v0_7:01HTJC6TME1JGXBFERHZ0FJ860"
  }
]
```

[HTTP accept header]: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html

## Query DSL

[Elasticsearch Query DSL reference](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl.html).

The following query types are supported.

### `query_string`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-query-string-query.html)


#### Example

```json
{
  "query": {
    "query_string": {
      "query": "bitpacking AND author.login:fulmicoton",
      "fields": [
        "payload.description"
      ]
    }
  }
}
```

#### Supported parameters

| Variable           | Type                  | Description                                                                                                                 | Default value |
| ------------------ | --------------------- | --------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `query`            | `String`              | Query meant to be parsed.                                                                                                   | -             |
| `fields`           | `String[]` (Optional) | Default search target fields.                                                                                               | -             |
| `default_operator` | `"AND"` or `"OR"`     | In the absence of boolean operator defines whether terms should be combined as a conjunction (`AND`) or disjunction (`OR`). | `OR`          |
| `boost`            | `Number`              | Multiplier boost for score computation.                                                                                     | 1.0           |
| `lenient`          | `Boolean`             | [See note](#about-the-lenient-argument).                                                                                    | false         |


### `bool`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-term-query.html)

#### Example

```json
{
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "bitpacking"
          }
        }
      ],
      "must_not": {
        "term": {
          "type": {
            "value": "CommitEvent"
          }
        }
      }
    }
  }
}
```

#### Supported parameters

| Variable   | Type                      | Description                                                       | Default value |
| ---------- | ------------------------- | ----------------------------------------------------------------- | ------------- |
| `must`     | `JsonObject[]` (Optional) | Sub-queries required to match the document.                       | []            |
| `must_not` | `JsonObject[]` (Optional) | Sub-queries required to not match the document.                   | []            |
| `should`   | `JsonObject[]` (Optional) | Sub-queries that should match the documents.                      | []            |
| `filter`   | `JsonObject[]`            | Like must queries, but the match does not influence the `_score`. | []            |
| `boost`    | `Number`                  | Multiplier boost for score computation.                           | 1.0           |
| `minimum_should_match`    | `Number` or `Str` | If present, quickwit will only match documents for which at least `minimum_should_match` should clauses are matching. `2`, `-1`, `"10%"` and `"-10%"` are supported. |  |

### `range`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-range-query.html)

#### Example

```json
{
  "query": {
    "range": {
      "my_date_field": {
        "lt": "2015-02-01T00:00:13Z",
        "gte": "2015-02-01T00:00:10Z"
      }
    }
  }
}

```

#### Supported parameters

| Variable | Type                            | Description                            | Default value |
| -------- | ------------------------------- | -------------------------------------- | ------------- |
| `gt`     | bool, string, Number (Optional) | Greater than                           | None          |
| `gte`    | bool, string, Number (Optional) | Greater than or equal                  | None          |
| `lt`     | bool, string, Number (Optional) | Less than                              | None          |
| `lte`    | bool, string, Number (Optional) | Less than or equal                     | None          |
| `boost`  | `Number`                        | Multiplier boost for score computation | 1.0           |


### `match`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-match-query.html)

#### Example

```json
{
  "query": {
    "match": {
        "type": {
            "query": "CommitEvent",
            "zero_terms_query": "all"
        }
    }
  }
}
```

#### Supported Parameters

| Variable           | Type              | Description                                                                                                                    | Default |
| ------------------ | ----------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `query`            | String            | Full-text search query.                                                                                                        | -       |
| `operator`         | `"AND"` or `"OR"` | Defines whether all terms should be present (`AND`) or if at least one term is sufficient to match (`OR`).                     | OR      |
| `zero_terms_query` | `all` or `none`   | Defines if all (`all`) or no documents (`none`) should be returned if the query does not contain any terms after tokenization. | `none`  |
| `boost`            | `Number`          | Multiplier boost for score computation                                                                                         | 1.0     |
| `lenient`          | `Boolean`         | [See note](#about-the-lenient-argument).                                                                                       | false   |



### `match_phrase`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-match-query-phrase.html)

#### Example

```json
{
  "query": {
    "match_phrase": {
      "title": "search keywords",
      "analyzer": "default"
    }
  }
}
```



### `match_phrase_prefix`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html)

#### Example

```json
{
  "query": {
    "match_phrase_prefix": {
      "payload.commits.message": {
        "query": "automated comm" // This will match "automated commit" for instance.
      }
    }
  }
}
```

#### Supported Parameters

| Variable           | Type            | Description                                                                                                                    | Default                     |
| ------------------ | --------------- | ------------------------------------------------------------------------------------------------------------------------------ | --------------------------- |
| `query`            | String          | Full-text search query. The last token will be prefix-matched                                                                  | -                           |
| `zero_terms_query` | `all` or `none` | Defines if all (`all`) or no documents (`none`) should be returned if the query does not contain any terms after tokenization. | `none`                      |
| `max_expansions`   | `Integer`       | Number of terms to be match by the prefix matching.                                                                            | 50                          |
| `slop`             | `Integer`       | Allows extra tokens between the query tokens.                                                                                  | 0                           |
| `analyzer`         | String          | Analyzer meant to cut the query into terms. It is recommended to NOT use this parameter.                                       | The actual field tokenizer. |


### `match_bool_prefix`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html)

#### Example

```json
{
  "query": {
    "match_bool_prefix": {
      "payload.commits.message": {
        "query": "automated comm" // This will match "automated commit" for instance.
      }
    }
  }
}
```

Contrary to ES/Opensearch, in Quickwit, at most 50 terms will be considered when searching the last term of the query as a prefix `match_bool_prefix`.

#### Supported Parameters

| Variable           | Type              | Description                                                                                                                    | Default |
| ------------------ | ----------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `query`            | String            | Full-text search query. The last token will be prefix-matched                                                                  | -       |
| `operator`         | `"AND"` or `"OR"` | Defines whether all terms should be present (`AND`) or if at least one term is sufficient to match (`OR`).                     | OR      |
| `zero_terms_query` | `all` or `none`   | Defines if all (`all`) or no documents (`none`) should be returned if the query does not contain any terms after tokenization. | `none`  |



### `Multi-match`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-multi-match-query.html)

#### Example
```json
{
  "query": {
    "multi_match": {
      "query": "search keywords",
      "fields": [
        "title",
        "body"
      ]
    }
  }
}
```

```json
{
  "query": {
    "multi_match": {
      "query": "search keywords",
      "type": "most_fields",
      "fields": [
        "title",
        "body"
      ]
    }
  }
}
```

```json
{
  "query": {
    "multi_match": {
      "query": "search keywords",
      "type": "phrase",
      "fields": [
        "title",
        "body"
      ]
    }
  }
}
```

```json
{
  "query": {
    "multi_match" : {
      "query":      "search key",
      "type":       "phrase_prefix",
      "fields":     [ "title", "body" ]
    }
  }
}
```

#### Supported parameters

| Variable           | Type                  | Description                                  | Default value |
| ------------------ | --------------------- | ---------------------------------------------| ------------- |
| `type`             | `String`              | See supported types below                    | `most_fields` |
| `fields`           | `String[]` (Optional) | Default search target fields.                | -             |
| `lenient`          | `Boolean`             | [See note](#about-the-lenient-argument).     | false         |

Supported types:

| `type` value    | Description                                                                                 |
| --------------- | ------------------------------------------------------------------------------------------- |
| `most_fields`   | Finds documents matching any field and combines the `_score` from each field (default).  |
| `phrase`        | Runs a `match_phrase` query on each field.       |
| `phrase_prefix` | Runs a `match_phrase_prefix` query on each field. |
| `bool_prefix`   | Runs a `match_bool_prefix` query on each field. |

:::warning

In `phrase`, `phrase_prefix` and `bool_prefix` modes, Quickwit sums the score of the different fields instead of returning their max.

Moreover, while Quickwit does not support `best_fields` or `cross_fields`, it will not return an error when presented a `best_fields` or `cross_fields` type. For compatibilility reasons, Quickwit silently accepts these parameters and interprets them as a `most_fields` type.

:::

### `term`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-term-query.html)

#### Example

```json
{
  "query": {
    "term": {
      "payload.commits.message": {
        "value": "automated",
        "boost": 2.0
      }
    }
  }
}
```

#### Supported Parameters

| Variable | Type     | Description                                                                  | Default |
| -------- | -------- | ---------------------------------------------------------------------------- | ------- |
| `value`  | String   | Term value. This is the string representation of a token after tokenization. | -       |
| `boost`  | `Number` | Multiplier boost for score computation                                       | 1.0     |




### `match_all` / `match_none`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html)

#### Example

```json
{"match_all": {}}
```
```json
{"match_none": {}}
```


### `exists`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-exists-query.html)

Query matching only documents containing a non-null value for a given field.

#### Example

```json
{
  "query": {
    "exists": {
      "field": "author.login"
    }
  }
}
```

#### Supported Parameters

| Variable | Type   | Description                                             | Default |
| -------- | ------ | ------------------------------------------------------- | ------- |
| `field`  | String | Only documents with a value for field will be returned. | -       |


### About the `lenient` argument

Quickwit and Elasticsearch have different interpretations of the `lenient` setting:
- In Quickwit, lenient mode allows ignoring parts of the query that reference non-existing columns. This is a behavior that Elasticsearch supports by default.
- In Elasticsearch, lenient mode primarily addresses type errors (such as searching for text in an integer field). Quickwit always supports this behavior, regardless of the `lenient` setting.

## Search multiple indices

Search APIs that accept <index_id> requests path parameter also support multi-target syntax.

### Multi-target syntax

In multi-target syntax, you can use a comma or its URL encoded version '%2C' separated list to run a request on multiple indices: test1,test2,test3. You can also sue [glob-like](https://en.wikipedia.org/wiki/Glob_(programming)) wildcard ( \* ) expressions to target indices that match a pattern: test\* or \*test or te\*t or \*test\*.

The multi-target expression has the following constraints:

    - It must follow the regex `^[a-zA-Z\*][a-zA-Z0-9-_\.\*]{0,254}$`.
    - It cannot contain consecutive asterisks (`*`).
    - If it does not contain an asterisk (`*`), the length must be greater than or equal to 3 characters.

### Examples
```
GET api/v1/_elastic/stackoverflow-000001,stackoverflow-000002/_search
{
  "query": {
    "query_string": {
      "query": "search AND engine",
      "fields": [
        "title",
        "body"
      ]
    }
  }
}
```

```
GET api/v1/_elastic/stackoverflow*/_search
{
  "query": {
    "query_string": {
      "query": "search AND engine",
      "fields": [
        "title",
        "body"
      ]
    }
  }
}
```
