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

The _bulk ingestion API makes it possible to index a batch of documents, possibly targetting several indices in the same request.

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

| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `refresh`     | `String`   | The commit behavior: blank string, `true`, `wait_for` or `false` | `false`       |

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field                       | Description                                                                                                                                                              |   Type   |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------:|
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
            {"query_string": {"query": "bitpacking"}},
            {"term":
                {"actor.login":
                    {"value": "fulmicoton"}
                }
            }
        ]
    }
  },
  "sort": [{"actor.id": {"order": null}}],
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


| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `default_operator`     | `AND` or `OR`  | The default operator used to combine search terms. It should be `AND` or `OR`. | `OR`       |
| `from`     | `Integer`   |  The rank of the first hit to return. This is useful for pagination.  |  0  |
| `q` | `String` | The search query. | (Optional) |
| `size` | `Integer` | Number of hits to return. |  10 |
| `sort` | `String` | (Optional) |

#### Supported Request Body parameters

| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `default_operator`     | `"AND"` or `"OR"` | The default operator used to combine search terms. It should be `AND` or `OR`. | `OR`       |
| `from`     | `Integer`   |  The rank of the first hit to return. This is useful for pagination.  |  0  |
| `query` | `Json object` | Describe the search query. See [Query DSL](#query-dsl) | (Optional) |
| `size` | `Integer` | Number of hits to return. |  10 |
| `sort` | `JsonObject[]` | Describes how documents should be ranked. | `[]` |
| `aggs` | `Json object` | Aggregation definition. See [Aggregations](aggregation.md). | `{}` | `


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
- a `header` json object, containing the targetted index id.
- a `search request body` as defined in the [`_search` endpoint section].

## Query DSL

[Elasticsearch Query DSL reference](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl.html).

The following query types are supported.

### `query_string`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-query-string-query.html)


#### Example

```json
{
    "query_string": {
        "query": "bitpacking AND author.login:fulmicoton",
        "fields": ["payload.description"]
    }
}
```

#### Supported parameters

| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `query`     | `String`   |  Query meant to be parsed. | -       |
| `fields`     | `String[]` (Optional)   | Default search target fields.  | -       |
| `default_operator`     | `"AND"` or `"OR"`   | In the absence of boolean operator defines whether terms should be combined as a conjunction (`AND`) or disjunction (`OR`). | `OR`|
| `boost`     | `Number`   | Multiplier boost for score computation. | 1.0       |


### `bool`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-term-query.html)

#### Example

```json
{
    "bool": {
        "must": [
            {"query_string": {"query": "bitpacking"}},
        ],
        "must_not": {"term":
            {"type":
                {"value": "CommitEvent"}
            }
        }
    }
}
```

#### Supported parameters

| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `must`     | `JsonObject[]` (Optional)  |  Sub-queries required to match the document. | [] |
| `must_not`     | `JsonObject[]` (Optional)   | Sub-queries required to not match the document.  | []       |
| `should`     | `JsonObject[]` (Optional)   | Sub-queries that should match the documents. | [] |
| `filter`     | `JsonObject[]` | Like must queries, but the match does not influence the `_score`.  | [] |
| `boost`     | `Number`   | Multiplier boost for score computation. | 1.0       |

### `range`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-range-query.html)

#### Example

```json
{
    "range": {
      "my_date_field": {
        "lt": "2015-02-01T00:00:13Z",
        "gte": "2015-02-01T00:00:10Z"
      }
    }
}

```

#### Supported parameters

| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `gt`     | bool, string, Number (Optional)  |  Greater than | None |
| `gte`     | bool, string, Number (Optional)  | Greater than or equal  | None  |
| `lt`     | bool, string, Number (Optional) | Less than | None |
| `lte`     | bool, string, Number (Optional) | Less than or equal  | None |
| `boost`     |  `Number`   | Multiplier boost for score computation | 1.0       |


### `match`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-match-query.html)

#### Example

```json
{
    "match": {
        "type": {
            "query": "CommitEvent",
            "zero_terms_query": "all"
        }
    }
}
```

#### Supported Parameters

| Variable      | Type       | Description                                                      | Default |
|---------------|------------|------------------------------------------------------------------|---------------|
| `query`     | String  |  Full-text search query. | - |
| `operator`  | `"AND"` or `"OR"` | Defines whether all terms should be present (`AND`) or if at least one term is sufficient to match (`OR`).  | OR  |
| `zero_terms_query`  |  `all` or `none` | Defines if all (`all`) or no documents (`none`) should be returned if the query does not contain any terms after tokenization. | `none` |
| `boost`     |  `Number`   | Multiplier boost for score computation | 1.0       |





### `match_phrase_prefix`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html)

#### Example

```json
{
    "match_phrase_prefix": {
      "payload.commits.message": {
        "query" : "automated comm" // This will match "automated commit" for instance.
      }
    }
}
```

#### Supported Parameters

| Variable          | Type       | Description                                                      | Default |
|-------------------|------------|------------------------------------------------------------------|---------|
| `query`           | String     |  Full-text search query. The last token will be prefix-matched   | -       |
| `zero_terms_query`|  `all` or `none` | Defines if all (`all`) or no documents (`none`) should be returned if the query does not contain any terms after tokenization. | `none` |
| `max_expansions`  |  `Integer` | Number of terms to be match by the prefix matching.               |  50     |
| `slop`            | `Integer`  | Allows extra tokens between the query tokens.                    |  0      |
| `analyzer`        | String     | Analyzer meant to cut the query into terms. It is recommended to NOT use this parameter. |  The actual field tokenizer.  |
| `zero_terms_query`  |  `all` or `none` | Defines if all (`all`) or no documents (`none`) should be returned if the query does not contain any terms after tokenization. | `none` |


### `term`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-term-query.html)

#### Example

```json
{
    "term": {
      "payload.commits.message": {
        "value": "automated",
        "boost": 2.0
      }
    }
}
```

#### Supported Parameters

| Variable          | Type       | Description                                                      | Default |
|-------------------|------------|------------------------------------------------------------------|---------|
| `value`           | String     |  Term value. This is the string representation of a token after tokenization.    | -    |
| `boost`     |  `Number`   | Multiplier boost for score computation | 1.0       |

### `match_all` / `match_none`

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html)

#### Example

```json
{"match_all": {}}
```
```json
{"match_none": {}}
```



