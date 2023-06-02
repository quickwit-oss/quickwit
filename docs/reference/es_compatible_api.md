---
title: Elasticsearch/Opensearch compatible API
sidebar_position: 2
---

In order to facilitate migrations and integrations with existing tools,
Quickwit offers a Elasticsearch/Opensearch compatible API.
This API is incomplete. This page lists the available features and endpoints.

## Supported endpoints

All the API endpoints start with the `api/v1/_elastic/` prefix.

### `_bulk`: Batch ingestion endpoint

```
POST api/v1/_elastic/_bulk
```
or
```
POST api/v1/_elastic/<index>/_bulk
```

The _bulk ingestion API makes it possible to index a batch of documents, possibly targetting several indices in the same request.

#### POST payload example

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

In Elasticsearch, the `create` action has a specific behavior when the ingest documents contain an identifier (the `_id` field). It only inserts such a document if it was not inserted before. This is extremely handy to achieve At-Most-Once indexing.
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




### `_search`: Index search endpoint

```
POST api/v1/_elastic/<index_id>/_search"
```
```
GET api/v1/_elastic/<index_id>/_search"
```

#### POST payload example

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

Search into a specific index using the [Elasticsearch] search API.

Some of the parameter can be passed as query string parameter, and some via JSON payload.
If a parameter appears both as a query string parameter and in the JSON payload, the query string parameter value will take priority.

#### Supported Query string parameters


| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `default_operator`     | `String`   | The default operator used to combine search terms. It should be `AND` or `OR` | `OR`       |
| `from`     | `Integer`   |  The rank of the first hit to return. This is useful for pagination.  |  0  |
| `q` | `String` | The | (Optional) |
| `size` | `Integer` | Number of hits to return. |  10 |
| `sort` | `String` | (Optional) |


#### Request body

| Variable      | Type       | Description                                                      | Default value |
|---------------|------------|------------------------------------------------------------------|---------------|
| `default_operator`     | `String`   | The default operator used to combine search terms. It should be `AND` or `OR` | `OR`       |
| `from`     | `Integer`   |  The rank of the first hit to return. This is useful for pagination.  |  0  |
| `query` | `Json object` | Describe the search query. See [Query DSL] | (Optional) |
| `size` | `Integer` | Number of hits to return. |  10 |
| `sort` | `Json Array` | Describes how documents should be ranked. | `[]` |
| `aggs` | `Json object` | Aggregation definition. See [Aggregations]. | `{}` | `


### `_msearch`: Multi search API

```
POST api/v1/_elastic/_msearch"
```

### Example POST Request Body

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

The following query types are supported:

### `query_string`: QueryString

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-query-string-query.html)

### `bool`: Composing queries into Boolean queries

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-term-query.html)

### `range`: Range queries

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-range-query.html)

### `match_phrase_queries`: Match phrase queries

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html)

### `match`: Match queries

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-match-query.html)

### `term`: Term queries

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/query-dsl-term-query.html)

### `match_all` / `match_none` Match all/no documents

[Elasticsearch reference documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html)



