---
title: REST API
sidebar_position: 1
---

## API version

All the API endpoints start with the `api/v1/` prefix. `v1` indicates that we are currently using version 1 of the API.

### Parameters

Parameters passed in the URL must be properly URL-encoded, using the UTF-8 encoding for non-ASCII characters.

```
GET [..]/search?query=barack%20obama
```

### Error handling

Successful requests return a 2xx HTTP status code.

Failed requests return a 4xx HTTP status code. The response body of failed requests holds a JSON object containing an `error_message` field that describes the error.

```json
{
 "error_message": "Failed to parse query"
}
```

## Search API

### Search in an index

Search for documents matching a query in the given index `api/v1/<index id>/search`. This endpoint is available as long as you have at least one node running a searcher service in the cluster.
The search endpoint accepts `GET` and `POST` requests. The [parameters](#get-parameters) are URL parameters in case of `GET` or JSON key value pairs in case of `POST`.

```
GET api/v1/<index id>/search?query=searchterm
```

```
POST api/v1/<index id>/search
{
  "query": searchterm
}
```

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| **index id**  | The index id  |

#### Parameters

| Variable            | Type       | Description                                                                                                                                            | Default value                                      |
|---------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| **query**           | `String`   | Query text. See the [query language doc](query-language.md) (mandatory)                                                                                |                                                    |
| **start_timestamp** | `i64`      | If set, restrict search to documents with a `timestamp >= start_timestamp`. The value must be in seconds.                                              |                                                    |
| **end_timestamp**   | `i64`      | If set, restrict search to documents with a `timestamp < end_timestamp`. The value must be in seconds.                                                 |                                                    |
| **start_offset**    | `Integer`  | Number of documents to skip                                                                                                                            | `0`                                                |
| **max_hits**        | `Integer`  | Maximum number of hits to return (by default 20)                                                                                                       | `20`                                               |
| **search_field**    | `[String]` | Fields to search on if no field name is specified in the query. Comma-separated list, e.g. "field1,field2"                                             | index_config.search_settings.default_search_fields |
| **snippet_fields**  | `[String]` | Fields to extract snippet on. Comma-separated list, e.g. "field1,field2"                                                                               |                                                    |
| **sort_by_field**   | `String`   | Field to sort query results by. You can sort by a field (must be a fast field) and by BM25 `_score`. By default, hits are sorted by their document ID. |                                                    |
| **format**          | `Enum`     | The output format. Allowed values are "json" or "prettyjson"                                                                                           | `prettyjson`                                       |
| **aggs**            | `JSON`     | The aggregations request. See the [aggregations doc](aggregation.md) for supported aggregations.                                                       |                                                    |

:::info
The `start_timestamp` and `end_timestamp` should be specified in seconds regardless of the timestamp field precision.
:::

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field                   | Description                    | Type       |
| --------------------    | ------------------------------ | :--------: |
| **hits**                | Results of the query           | `[hit]`    |
| **num_hits**            | Total number of matches        | `number`   |
| **elapsed_time_micros** | Processing time of the query   | `number`   |

### Search stream in an index

```
GET api/v1/<index id>/search/stream?query=searchterm
```

Streams field values from ALL documents matching a search query in the given index `<index id>`, in a specified output format among the following:

- [CSV](https://datatracker.ietf.org/doc/html/rfc4180)
- [ClickHouse RowBinary](https://clickhouse.tech/docs/en/interfaces/formats/#rowbinary)
 This endpoint is available as long as you have at least one node running a searcher service in the cluster.

:::note

The endpoint will return 10 million values if 10 million documents match the query. This is expected, this endpoint is made to support queries matching millions of document and return field values in a reasonable response time.

:::

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| **index id**  | The index id  |

#### Get parameters

| Variable            | Type       | Description                                                                                                      | Default value                                      |
|---------------------|------------|------------------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| **query**           | `String`   | Query text. See the [query language doc](query-language.md) (mandatory)                                          |                                                    |
| **fast_field**      | `String`   | Name of a field to retrieve from documents. This field must be marked as "fast" in the index config. (mandatory) |                                                    |
| **search_field**    | `[String]` | Fields to search on. Comma-separated list, e.g. "field1,field2"                                                  | index_config.search_settings.default_search_fields |
| **start_timestamp** | `i64`      | If set, restrict search to documents with a `timestamp >= start_timestamp`. The value must be in seconds.        |                                                    |
| **end_timestamp**   | `i64`      | If set, restrict search to documents with a `timestamp < end_timestamp`. The value must be in seconds.           |                                                    |
| **output_format**   | `String`   | Response output format. `csv` or `clickHouseRowBinary`                                                           | `csv`                                              |

:::info
The `start_timestamp` and `end_timestamp` should be specified in seconds regardless of the timestamp field precision.
:::

#### Response

The response is an HTTP stream. Depending on the client's capability, it is an HTTP1.1 [chunked transfer encoded stream](https://en.wikipedia.org/wiki/Chunked_transfer_encoding) or an HTTP2 stream.

It returns a list of all the field values from documents matching the query. The field must be marked as "fast" in the index config for this to work.
The formatting is based on the specified output format.

On error, an "X-Stream-Error" header will be sent via the trailers channel with information about the error, and the stream will be closed via [`sender.abort()`](https://docs.rs/hyper/0.14.16/hyper/body/struct.Sender.html#method.abort).
Depending on the client, the trailer header with error details may not be shown. The error will also be logged in quickwit ("Error when streaming search results").

### Ingest data into an index

```
POST api/v1/<index id>/ingest -d \
'{"url":"https://en.wikipedia.org/wiki?id=1","title":"foo","body":"foo"}
{"url":"https://en.wikipedia.org/wiki?id=2","title":"bar","body":"bar"}
{"url":"https://en.wikipedia.org/wiki?id=3","title":"baz","body":"baz"}'
```

Ingest a batch of documents to make them searchable in a given `<index id>`. Currently, NDJSON is the only accepted payload format.This endpoint is only available on a node that is running an indexer service.

:::info
The payload size is limited to 10MB as this endpoint is intended to receive documents in batch.
:::

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| **index id**  | The index id  |

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field                       | Description                                                                                                                                                              |   Type   |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------:|
| **num_docs_for_processing** | Total number of documents ingested for processing. The documents may not have been processed. The API will not return indexing errors, check the server logs for errors. | `number` |

### Ingest data with Elasticsearch compatible API

```
POST api/v1/_bulk -d \
'{ "create" : { "_index" : "wikipedia", "_id" : "1" } }
{"url":"https://en.wikipedia.org/wiki?id=1","title":"foo","body":"foo"}
{ "create" : { "_index" : "wikipedia", "_id" : "2" } }
{"url":"https://en.wikipedia.org/wiki?id=2","title":"bar","body":"bar"}
{ "create" : { "_index" : "wikipedia", "_id" : "3" } }
{"url":"https://en.wikipedia.org/wiki?id=3","title":"baz","body":"baz"}'
```

Ingest a batch of documents to make them searchable using the [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) bulk API. This endpoint provides compatibility with tools or systems that already send data to Elasticsearch for indexing. Currently, only the `create` action of the bulk API is supported, all other actions such as `delete` or `update` are ignored.
:::caution
The quickwit API will not report errors, you need to check the server logs.

In Elasticsearch, the `create` action has a specific behavior when the ingest documents contain an identifier (the `_id` field). It only inserts such a document if it was not inserted before. This is extremely handy to achieve At-Most-Once indexing.
Quickwit does not have any notion of document id and does not support this feature.
:::

:::info
The payload size is limited to 10MB as this endpoint is intended to receive documents in batch.
:::

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field                       | Description                                                                                                                                                              |   Type   |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------:|
| **num_docs_for_processing** | Total number of documents ingested for processing. The documents may not have been processed. The API will not return indexing errors, check the server logs for errors. | `number` |


## Index management API

### Create an index

```
POST api/v1/indexes
```

Create an index by posting an `IndexConfig` JSON payload.

#### POST payload

| Variable              | Type               | Description                                                                                                           | Default value                         |
|-----------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| **version**           | `String`           | Config format version, use the same as your Quickwit version. (mandatory)                                             |                                       |
| **index_id**          | `String`           | Index ID, see its [validation rules](../configuration/index-config.md#index-id) on identifiers. (mandatory)           |                                       |
| **index_uri**         | `String`           | Defines where the index files are stored. This parameter expects a [storage URI](../reference/storage-uri).           | `{default_index_root_uri}/{index_id}` |
| **doc_mapping**       | `DocMapping`       | Doc mapping object as specified in the [index config docs](../configuration/index-config.md#doc-mapping) (mandatory)  |                                       |
| **indexing_settings** | `IndexingSettings` | Indexing settings object as specified in the [index config docs](../configuration/index-config.md#indexing-settings). |                                       |
| **search_settings**   | `SearchSettings`   | Search settings object as specified in the [index config docs](../configuration/index-config.md#search-settings).     |                                       |
| **retention**         | `Retention`        | Retention policy object as specified in the [index config docs](../configuration/index-config.md#retention-policy).   |                                       |


**Payload Example**

curl -XPOST http://0.0.0.0:8080/api/v1/indexes --data @index_config.json -H "Content-Type: application/json"

```json title="index_config.json
{
    "version": "0.4",
    "index_id": "hdfs-logs",
    "doc_mapping": {
        "field_mappings": [
            {
                "name": "tenant_id",
                "type": "u64",
                "fast": true
            },
            {
                "name": "app_id",
                "type": "u64",
                "fast": true
            },
            {
                "name": "timestamp",
                "type": "datetime",
                "input_formats": ["unix_timestamp"],
                "precision": "seconds",
                "fast": true
            },
            {
                "name": "body",
                "type": "text",
                "record": "position"
            }
        ],
        "partition_key": "tenant_id",
        "max_num_partitions": 200,
        "tag_fields": ["tenant_id"],
        "timestamp_field": "timestamp"
    },
    "search_settings": {
        "default_search_fields": ["body"]
    },
    "indexing_settings": {
        "merge_policy": {
            "type": "limit_merge",
            "max_merge_ops": 3,
            "merge_factor": 10,
            "max_merge_factor": 12
        },
        "resources": {
            "max_merge_write_throughput": "80mb"
        }
    },
    "retention": {
        "period": "7 days",
        "schedule": "@daily"
    }
}
```

#### Response

The response is the index metadata of the created index, and the content type is `application/json; charset=UTF-8.`

| Field                | Description                               |         Type          |
|----------------------|-------------------------------------------|:---------------------:|
| **index_config**     | The posted index config.                  |     `IndexConfig`     |
| **checkpoint**       | Map of checkpoints by source.             |   `IndexCheckpoint`   |
| **create_timestamp** | Index creation timestamp                  |       `number`        |
| **sources**          | List of the index sources configurations. | `Array<SourceConfig>` |


### Get an index metadata

```
GET api/v1/indexes/<index id>
```

Get the index metadata of ID `index id`.

#### Response

The response is the index metadata of the requested index, and the content type is `application/json; charset=UTF-8.`

| Field                | Description                               |         Type          |
|----------------------|-------------------------------------------|:---------------------:|
| **index_config**     | The posted index config.                  |     `IndexConfig`     |
| **checkpoint**       | Map of checkpoints by source.             |   `IndexCheckpoint`   |
| **create_timestamp** | Index creation timestamp.                 |       `number`        |
| **sources**          | List of the index sources configurations. | `Array<SourceConfig>` |


### Delete an index

```
DELETE api/v1/indexes/<index id>
```

Delete index of ID `index id`.

#### Response

The response is the list of delete split files, and the content type is `application/json; charset=UTF-8.`

```json
[
    {
        "file_name": "01GK1XNAECH7P14850S9VV6P94.split",
        "file_size_in_bytes": 2991676
    }
]
```

### Get all indexes metadatas

```
GET api/v1/indexes
```

Get the indexes metadatas of all indexes present in the metastore.

#### Response

The response is an array of `IndexMetadata`, and the content type is `application/json; charset=UTF-8.`


### Create a source

```
POST api/v1/indexes/<index id>/sources
```

Create source by posting a source config JSON payload.

#### POST payload

| Variable          | Type     | Description                                                                                          | Default value |
|-------------------|----------|------------------------------------------------------------------------------------------------------|---------------|
| **version**       | `String` | Config format version, put your current Quickwit version. (mandatory)                                |               |
| **source_id**     | `String` | Source ID. See ID [validation rules](../configuration/source-config.md)(mandatory)                   |               |
| **source_type**   | `String` | Source type: `kafka`, `kinesis`, `file`. (mandatory)                                                 |               |
| **num_pipelines** | `usize`  | Number of running indexing pipelines per node for this source.                                       | 1             |
| **params**        | `object` | Source parameters as defined in [source config docs](../configuration/source-config.md). (mandatory) |               |


**Payload Example**

curl -XPOST http://0.0.0.0:8080/api/v1/indexes/my-index/sources --data @source_config.json -H "Content-Type: application/json"

```json title="source_config.json
{
    "version": "0.4",
    "source_id": "kafka-source",
    "source_type": "kafka",
    "params": {
        "topic": "quickwit-fts-staging",
        "client_params": {
            "bootstrap.servers": "kafka-quickwit-server:9092"
        }
    }
}
```

#### Response

The response is the created source config, and the content type is `application/json; charset=UTF-8.`


### Delete a source

```
DELETE api/v1/indexes/<index id>/sources/<source id>
```

Delete source of ID `<source id>`.


## Cluster API

This endpoint lets you check the state of the cluster from the point of view of the node handling the request.

```
GET api/v1/cluster?format=prettyjson
```

#### Parameters

Name | Type | Description | Default value
--- | --- | --- | ---
**format** | `String` | The output format requested for the response: `json` or `prettyjson` | `prettyjson`


## Delete API

The delete API enables to delete documents matching a query.

### Create a delete task

```
POST api/v1/<index id>/delete-tasks
```

Create a delete task that will delete all documents matching the provided query in the given index `<index id>`.
The endpoint simply appends your delete task to the delete task queue in the metastore. The deletion will eventually be executed.

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| **index id**  | The index id  |


#### POST payload `DeleteQuery`


| Variable            | Type       | Description                                                                                               | Default value                                      |
|---------------------|------------|-----------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| **query**           | `String`   | Query text. See the [query language doc](query-language.md) (mandatory)                                   |                                                    |
| **search_field**    | `[String]` | Fields to search on. Comma-separated list, e.g. "field1,field2"                                           | index_config.search_settings.default_search_fields |
| **start_timestamp** | `i64`      | If set, restrict search to documents with a `timestamp >= start_timestamp`. The value must be in seconds. |                                                    |
| **end_timestamp**   | `i64`      | If set, restrict search to documents with a `timestamp < end_timestamp`. The value must be in seconds.    |                                                    |


**Example**

```json
{
    "query": "body:trash",
    "start_timestamp": "1669738645",
    "end_timestamp": "1669825046",
}
```

#### Response

The response is the created delete task represented in JSON, `DeleteTask`, the content type is `application/json; charset=UTF-8.`

| Field                | Description                                            |     Type      |
|----------------------|--------------------------------------------------------|:-------------:|
| **create_timestamp** | Create timestamp of the delete query in seconds        |     `i64`     |
| **opstamp**          | Unique operation stamp associated with the delete task |     `u64`     |
| **delete_query**     | The posted delete query                                | `DeleteQuery` |


### GET a delete query

```
GET api/v1/<index id>/delete-tasks/<opstamp>
```

Get the delete task of operation stamp `opstamp` for a given `index_id`.


#### Response

The response is a `DeleteTask`.
