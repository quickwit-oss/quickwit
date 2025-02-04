---
title: REST API
sidebar_position: 10
---

## API version

All the API endpoints start with the `api/v1/` prefix. `v1` indicates that we are currently using version 1 of the API.


## OpenAPI specification

The OpenAPI specification of the REST API is available at `/openapi.json` and a Swagger UI version is available at `/ui/api-playground`.

## Parameters

Parameters passed in the URL must be properly URL-encoded, using the UTF-8 encoding for non-ASCII characters.

```
GET [..]/search?query=barack%20obama
```

## Error handling

Successful requests return a 2xx HTTP status code.

Failed requests return a 4xx HTTP status code. The response body of failed requests holds a JSON object containing a `message` field that describes the error.

```json
{
 "message": "Failed to parse query"
}
```

## Search API

### Search in an index

Search for documents matching a query in the given index `api/v1/<index id>/search`. This endpoint is available as long as you have at least one node running a searcher service in the cluster.
The search endpoint accepts `GET` and `POST` requests. The [parameters](#get-parameters) are URL parameters for `GET` requests or JSON key-value pairs for `POST` requests.

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
| `index id`  | The index id  |

#### Parameters

| Variable            | Type       | Description     | Default value   |
|---------------------|------------|-----------------|-----------------|
| `query`           | `String`   | Query text. See the [query language doc](query-language.md) | _required_ |
| `start_timestamp` | `i64`      | If set, restrict search to documents with a `timestamp >= start_timestamp`, taking advantage of potential time pruning opportunities. The value must be in seconds. | |
| `end_timestamp`   | `i64`      | If set, restrict search to documents with a `timestamp < end_timestamp`, taking advantage of potential time pruning opportunities. The value must be in seconds.    | |
| `start_offset`    | `Integer`  | Number of documents to skip | `0` |
| `max_hits`        | `Integer`  | Maximum number of hits to return (by default 20) | `20` |
| `search_field`    | `[String]` | Fields to search on if no field name is specified in the query. Comma-separated list, e.g. "field1,field2"  | index_config.search_settings.default_search_fields |
| `snippet_fields`  | `[String]` | Fields to extract snippet on. Comma-separated list, e.g. "field1,field2"  | |
| `sort_by`         | `[String]` | Fields to sort the query results on. You can sort by one or two fast fields or by BM25 `_score` (requires fieldnorms). By default, hits are sorted in reverse order of their [document ID](/docs/overview/concepts/querying.md#document-id) (to show recent events first). | |
| `format`          | `Enum`     | The output format. Allowed values are "json" or "pretty_json" | `pretty_json` |
| `aggs`            | `JSON`     | The aggregations request. See the [aggregations doc](aggregation.md) for supported aggregations. | |

:::info
The `start_timestamp` and `end_timestamp` should be specified in seconds regardless of the timestamp field precision.
:::

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field                   | Description                    | Type       |
| --------------------    | ------------------------------ | :--------: |
| `hits`                | Results of the query           | `[hit]`    |
| `num_hits`            | Total number of matches        | `number`   |
| `elapsed_time_micros` | Processing time of the query   | `number`   |

### Search multiple indices
Search APIs that accept `index id` requests path parameter also support multi-target syntax.

#### Multi-target syntax

In multi-target syntax, you can use a comma or its URL encoded version '%2C' separated list to run a request on multiple indices: test1,test2,test3. You can also use [glob-like](https://en.wikipedia.org/wiki/Glob_(programming)) wildcard ( \* ) expressions to target indices that match a pattern: test\* or \*test or te\*t or \*test\*.

The following are some constrains about the multi-target expression.

    - It must follow the regex `^[a-zA-Z\*][a-zA-Z0-9-_\.\*]{0,254}$`.
    - It cannot contain consecutive asterisks (`*`).
    - If it contains an asterisk (`*`), the length must be greater than or equal to 3 characters.

#### Examples
```
GET api/v1/stackoverflow-000001,stackoverflow-000002/search
{
    "query": "search AND engine",
}
```

```
GET api/v1/stackoverflow*/search
{
    "query": "search AND engine",
}
```

### Search stream in an index

```
GET api/v1/<index id>/search/stream?query=searchterm&fast_field=my_id
```

Streams field values from ALL documents matching a search query in the target index `<index id>`, in a specified output format among the following:

- [CSV](https://datatracker.ietf.org/doc/html/rfc4180)
- [ClickHouse RowBinary](https://clickhouse.tech/docs/en/interfaces/formats/#rowbinary). If `partition_by_field` is set, Quickwit returns chunks of data for each partition field value. Each chunk starts with 16 bytes being partition value and content length and then the `fast_field` values in `RowBinary` format.

`fast_field` and `partition_by_field` must be fast fields of type `i64` or `u64`.

This endpoint is available as long as you have at least one node running a searcher service in the cluster.



:::note

The endpoint will return 10 million values if 10 million documents match the query. This is expected, this endpoint is made to support queries matching millions of documents and return field values in a reasonable response time.

:::

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| `index id`  | The index id  |

#### Get parameters

| Variable            | Type       | Description                                                                                              | Default value                                      |
|---------------------|------------|----------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| `query`           | `String`   | Query text. See the [query language doc](query-language.md)                                                | _required_                                         |
| `fast_field`      | `String`   | Name of a field to retrieve from documents. This field must be a fast field of type `i64` or `u64`.        | _required_                                         |
| `search_field`    | `[String]` | Fields to search on. Comma-separated list, e.g. "field1,field2"                                            | index_config.search_settings.default_search_fields |
| `start_timestamp` | `i64`      | If set, restrict search to documents with a `timestamp >= start_timestamp`. The value must be in seconds.  |                                                    |
| `end_timestamp`   | `i64`      | If set, restrict search to documents with a `timestamp < end_timestamp`. The value must be in seconds.     |                                                    |
| `partition_by_field` | `String`      | If set, the endpoint returns chunks of data for each partition field value. This field must be a fast field of type `i64` or `u64`.           |                                                    |
| `output_format`   | `String`   | Response output format. `csv` or `clickHouseRowBinary`  | `csv` |

:::info
The `start_timestamp` and `end_timestamp` should be specified in seconds regardless of the timestamp field precision.
:::

#### Response

The response is an HTTP stream. Depending on the client's capability, it is an HTTP1.1 [chunked transfer encoded stream](https://en.wikipedia.org/wiki/Chunked_transfer_encoding) or an HTTP2 stream.

It returns a list of all the field values from documents matching the query. The field must be marked as "fast" in the index config for this to work.
The formatting is based on the specified output format.

On error, an "X-Stream-Error" header will be sent via the trailers channel with information about the error, and the stream will be closed via [`sender.abort()`](https://docs.rs/hyper/0.14.16/hyper/body/struct.Sender.html#method.abort).
Depending on the client, the trailer header with error details may not be shown. The error will also be logged in quickwit ("Error when streaming search results").

## Ingest API

### Ingest data into an index

```
POST api/v1/<index id>/ingest -d \
'{"url":"https://en.wikipedia.org/wiki?id=1","title":"foo","body":"foo"}
{"url":"https://en.wikipedia.org/wiki?id=2","title":"bar","body":"bar"}
{"url":"https://en.wikipedia.org/wiki?id=3","title":"baz","body":"baz"}'
```

Ingest a batch of documents to make them searchable in a given `<index id>`. Currently, NDJSON is the only accepted payload format. This endpoint is only available on a node that is running an indexer service.

#### Controlling when the indexed documents will be available for search

Newly added documents will not appear in the search results until they are added to a split and that split is committed. This process is automatic and is controlled by `split_num_docs_target` and `commit_timeout_secs` parameters. By default, the ingest command exits as soon as the records are added to the indexing queue, which means that the new documents will not appear in the search results at this moment. This behavior can be changed by adding `commit=wait_for` or `commit=force` parameters to the query. The `wait_for` parameter will cause the command to wait for the documents to be committed according to the standard time or number of documents rules. The `force` parameter will trigger a commit after all documents in the request are processed. It will also wait for this commit to finish before returning. Please note that the `force` option may have a significant performance cost especially if it is used on small batches.

```
POST api/v1/<index id>/ingest?commit=wait_for -d \
'{"url":"https://en.wikipedia.org/wiki?id=1","title":"foo","body":"foo"}
{"url":"https://en.wikipedia.org/wiki?id=2","title":"bar","body":"bar"}
{"url":"https://en.wikipedia.org/wiki?id=3","title":"baz","body":"baz"}'
```

:::info

The payload size is limited to 10MB [by default](../configuration/node-config.md#ingest-api-configuration) since this endpoint is intended to receive documents in batches.

:::

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| `index id`  | The index id  |

#### Query parameters

| Variable            | Type       | Description                                        | Default value |
|---------------------|------------|----------------------------------------------------|---------------|
| `commit`            | `String`   | The commit behavior: `auto`, `wait_for` or `force` | `auto`        |
| `detailed_response` | `bool`     | Enable `parse_failures` in the response. Setting to `true` might impact performances negatively. | `false`        |

#### Response

The response is a JSON object, and the content type is `application/json; charset=UTF-8.`

| Field                       | Description                                                                                                                                                              |   Type   |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------:|
| `num_docs_for_processing` | Total number of documents submitted for processing. The documents may not have been processed. | `number` |
| `num_ingested_docs`       | Number of documents successfully persisted in the write ahead log | `number` |
| `num_rejected_docs`       | Number of documents that couldn't be parsed (invalid json, bad schema...) | `number` |
| `parse_failures`          | List detailing parsing failures. Only available if `detailed_response` is set to `true`. | `list(object)` |

The parse failure objects contain the following fields:
- `message`: a detailed message explaining the error
- `reason`: one of `invalid_json`, `invalid_schema` or `unspecified`
- `document`: the utf-8 decoded string of the document byte chunk that generated the error


## Index API

### Create an index

```
POST api/v1/indexes
```

Create an index by posting an `IndexConfig` payload. The API accepts JSON with `content-type: application/json` and YAML with `content-type: application/yaml`.

#### POST payload

| Variable            | Type               | Description                                                                                                           | Default value                         |
|---------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| `version`           | `String`           | Config format version, use the same as your Quickwit version.                                                         | _required_                            |
| `index_id`          | `String`           | Index ID, see its [validation rules](../configuration/index-config.md#index-id) on identifiers.                       | _required_                            |
| `index_uri`         | `String`           | Defines where the index files are stored. This parameter expects a [storage URI](../configuration/storage-config.md#storage-uris).           | `{default_index_root_uri}/{index_id}` |
| `doc_mapping`       | `DocMapping`       | Doc mapping object as specified in the [index config docs](../configuration/index-config.md#doc-mapping).             | _required_                            |
| `indexing_settings` | `IndexingSettings` | Indexing settings object as specified in the [index config docs](../configuration/index-config.md#indexing-settings). |                                       |
| `search_settings`   | `SearchSettings`   | Search settings object as specified in the [index config docs](../configuration/index-config.md#search-settings).     |                                       |
| `retention`         | `Retention`        | Retention policy object as specified in the [index config docs](../configuration/index-config.md#retention-policy).   |                                       |


**Payload Example**

curl -XPOST http://localhost:7280/api/v1/indexes --data @index_config.json -H "Content-Type: application/json"

```json title="index_config.json
{
    "version": "0.9",
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
                "fast_precision": "seconds",
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

| Field                | Description                                   |         Type          |
|----------------------|-----------------------------------------------|:---------------------:|
| `version`          | The current index configuration format version. |       `string`        |
| `index_uid`        | The server-generated index UID.                 |       `string`        |
| `index_config`     | The posted index config.                        |     `IndexConfig`     |
| `checkpoint`       | Map of checkpoints by source.                   |   `IndexCheckpoint`   |
| `create_timestamp` | Index creation timestamp                        |       `number`        |
| `sources`          | List of the index sources configurations.       | `Array<SourceConfig>` |


### Update an index

```
PUT api/v1/indexes/<index id>
```

Updates the configurations of an index. This endpoint follows PUT semantics, which means that all the fields of the current configuration are replaced by the values specified in this request or the associated defaults. In particular, if the field is optional (e.g. `retention_policy`), omitting it will delete the associated configuration. If the new configuration file contains updates that cannot be applied, the request fails, and none of the updates are applied. The API accepts JSON with `content-type: application/json` and YAML with `content-type: application/yaml`.

- The retention policy update is automatically picked up by the janitor service on its next state refresh.
- The search settings update is automatically picked up by searcher nodes when the next query is executed.
- The indexing settings update is automatically picked up by the indexer nodes once the control plane emits a new indexing plan.
- The doc mapping update is automatically picked up by the indexer nodes once the control plane emit a new indexing plan.

Updating the doc mapping doesn't reindex existing data. Queries and results are mapped on a best-effort basis when querying older splits. For more details, check [the reference](updating-mapper.md) out.

#### PUT payload

| Variable            | Type               | Description                                                                                                           | Default value                         |
|---------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| `version`           | `String`           | Config format version, use the same as your Quickwit version.                                                         | _required_                            |
| `index_id`          | `String`           | Index ID, must be the same index as in the request URI.                                                               | _required_                            |
| `index_uri`         | `String`           | Defines where the index files are stored. Cannot be updated.                                                          | `{current_index_uri}`                 |
| `doc_mapping`       | `DocMapping`       | Doc mapping object as specified in the [index config docs](../configuration/index-config.md#doc-mapping).             | _required_                            |
| `indexing_settings` | `IndexingSettings` | Indexing settings object as specified in the [index config docs](../configuration/index-config.md#indexing-settings). |                                       |
| `search_settings`   | `SearchSettings`   | Search settings object as specified in the [index config docs](../configuration/index-config.md#search-settings).     |                                       |
| `retention`         | `Retention`        | Retention policy object as specified in the [index config docs](../configuration/index-config.md#retention-policy).   |                                       |


**Payload Example**

curl -XPUT http://localhost:7280/api/v1/indexes/hdfs-logs --data @updated_index_update.json -H "Content-Type: application/json"

```json title="updated_index_update.json
{
    "version": "0.9",
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
                "fast_precision": "seconds",
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
        }
    },
    "retention": {
        "period": "30 days",
        "schedule": "@daily"
    }
}
```

#### Response

The response is the index metadata of the updated index, and the content type is `application/json; charset=UTF-8.`

| Field                | Description                             |         Type          |
|----------------------|-----------------------------------------|:---------------------:|
| `version`          | The current server configuration version. |       `string`        |
| `index_uid`        | The server-generated index UID.            |       `string`        |
| `index_config`     | The posted index config.                  |     `IndexConfig`     |
| `checkpoint`       | Map of checkpoints by source.             |   `IndexCheckpoint`   |
| `create_timestamp` | Index creation timestamp                  |       `number`        |
| `sources`          | List of the index sources configurations. | `Array<SourceConfig>` |


### Get an index metadata

```
GET api/v1/indexes/<index id>
```

Get the index metadata of ID `index id`.

#### Response

The response is the index metadata of the requested index, and the content type is `application/json; charset=UTF-8.`

| Field                | Description                               |         Type          |
|----------------------|-------------------------------------------|:---------------------:|
| `version`          | The current server configuration version. |       `string`        |
| `index_uid`        | The server-generated index UID.            |       `string`        |
| `index_config`     | The posted index config.                  |     `IndexConfig`     |
| `checkpoint`       | Map of checkpoints by source.             |   `IndexCheckpoint`   |
| `create_timestamp` | Index creation timestamp.                 |       `number`        |
| `sources`          | List of the index sources configurations. | `Array<SourceConfig>` |


### Describe an index

```
GET api/v1/indexes/<index id>/describe
```
Describes an index of ID `index id`.

#### Response

The response is the stats about the requested index, and the content type is `application/json; charset=UTF-8.`

| Field                               | Description                                              |         Type          |
|-------------------------------------|----------------------------------------------------------|:---------------------:|
| `index_id`                          | Index ID of index.                                       |       `String`        |
| `index_uri`                         | Uri of index                                             |       `String`        |
| `num_published_splits`              | Number of published splits.                              |       `number`        |
| `size_published_splits`             | Size of published splits.                                |       `number`        |
| `num_published_docs`                | Number of published documents.                           |       `number`        |
| `size_published_docs_uncompressed`  | Size of the published documents in bytes (uncompressed). |       `number`        |
| `timestamp_field_name`              | Name of timestamp field.                                       |       `String`        |
| `min_timestamp`                     | Starting time of timestamp.                              |       `number`        |
| `max_timestamp`                     | Ending time of timestamp.                                |       `number`        |


### Get splits

```
GET api/v1/indexes/<index id>/splits
```
Get splits belongs to an index of ID `index id`.

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| `index id`  | The index id  |

#### Get parameters

| Variable            | Type       | Description                                                                                                      |
|---------------------|------------|------------------------------------------------------------------------------------------------------------------|
| `offset`           | `number`   | If set, restrict the number of splits to skip|
| `limit `           | `number`   | If set, restrict maximum number of splits to retrieve|
| `split_states`           | `usize`   | If set, specific split state(s) to filter by|
| `start_timestamp`           | `number`   | If set, restrict splits to documents with a `timestamp >= start_timestamp|
| `end_timestamp`           | `number`   | If set, restrict splits to documents with a `timestamp < end_timestamp|
| `end_create_timestamp`           | `number`   | If set, restrict splits whose creation dates are before this date|


#### Response

The response is the stats about the requested index, and the content type is `application/json; charset=UTF-8.`

| Field                               | Description                                              |         Type          |
|-------------------------------------|----------------------------------------------------------|:---------------------:|
| `offset`                          | Index ID of index.                                       |       `String`        |
| `size`                         | Uri of index                                             |       `String`        |
| `splits`              | Number of published splits.                              |       `List`        |

#### Examples
```
GET /api/v1/indexes/stackoverflow/splits?offset=0&limit=10
```
```json
{
  "offset": 0,
  "size": 1,
  "splits": [
    {
      "split_state": "Published",
      "update_timestamp": 1695642901,
      "publish_timestamp": 1695642901,
      "version": "0.9",
      "split_id": "01HB632HD8W6WHNM7CZFH3KG1X",
      "index_uid": "stackoverflow:01HB6321TDT3SP58D4EZP14KSX",
      "partition_id": 0,
      "source_id": "_ingest-api-source",
      "node_id": "jerry",
      "num_docs": 10000,
      "uncompressed_docs_size_in_bytes": 6674940,
      "time_range": {
        "start": 1217540572,
        "end": 1219335682
      },
      "create_timestamp": 1695642900,
      "maturity": {
        "type": "immature",
        "maturation_period_millis": 172800000
      },
      "tags": [],
      "footer_offsets": {
        "start": 4714989,
        "end": 4719999
      },
      "delete_opstamp": 0,
      "num_merge_ops": 0
    }
  ]
}
```


### Clears an index

```
PUT api/v1/indexes/<index id>/clear
```

Clears index of ID `index id`: all splits will be deleted (metastore + storage) and all source checkpoints will be reset.

It returns an empty body.


### Delete an index

```
DELETE api/v1/indexes/<index id>
```

Delete index of ID `index id`.

#### Response

The response is the list of deleted split files; the content type is `application/json; charset=UTF-8.`

```json
[
    {
        "split_id": "01GK1XNAECH7P14850S9VV6P94",
        "num_docs": 1337,
        "uncompressed_docs_size_bytes": 23933408,
        "file_name": "01GK1XNAECH7P14850S9VV6P94.split",
        "file_size_bytes": 2991676
    }
]
```

### Get all indexes metadata

```
GET api/v1/indexes
```

Retrieve the metadata of all indexes present in the metastore.

#### Response

The response is an array of `IndexMetadata`, and the content type is `application/json; charset=UTF-8.`


### Create a source

```
POST api/v1/indexes/<index id>/sources
```

Create source by posting a source config JSON payload.

#### POST payload

| Variable          | Type     | Description                                                                            | Default value |
|-------------------|----------|----------------------------------------------------------------------------------------|---------------|
| `version**       | `String` | Config format version, put your current Quickwit version.                               | _required_    |
| `source_id`     | `String` | Source ID. See ID [validation rules](../configuration/source-config.md).                 | _required_    |
| `source_type`   | `String` | Source type: `kafka`, `kinesis` or `pulsar`.                                             | _required_    |
| `num_pipelines` | `usize`  | Number of running indexing pipelines per node for this source.                           | `1`           |
| `transform`     | `object` | A [VRL](https://vector.dev/docs/reference/vrl/) transformation applied to incoming documents, as defined in [source config docs](../configuration/source-config.md#transform-parameters).                          | `null`         |
| `params`        | `object` | Source parameters as defined in [source config docs](../configuration/source-config.md). | _required_    |


**Payload Example**

curl -XPOST http://localhost:7280/api/v1/indexes/my-index/sources --data @source_config.json -H "Content-Type: application/json"

```json title="source_config.json
{
    "version": "0.9",
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

### Update a source

```
PUT api/v1/indexes/<index id>/sources/<source id>
```

Update a source by posting a source config JSON payload.

#### PUT payload

| Variable          | Type     | Description                                                                            | Default value |
|-------------------|----------|----------------------------------------------------------------------------------------|---------------|
| `version**       | `String` | Config format version, put your current Quickwit version.                               | _required_    |
| `source_id`     | `String` | Source ID, must be the same source as in the request URL.                                | _required_    |
| `source_type`   | `String` | Source type: `kafka`, `kinesis` or `pulsar`. Cannot be updated.                          | _required_    |
| `num_pipelines` | `usize`  | Number of running indexing pipelines per node for this source.                           | `1`           |
| `transform`     | `object` | A [VRL](https://vector.dev/docs/reference/vrl/) transformation applied to incoming documents, as defined in [source config docs](../configuration/source-config.md#transform-parameters).                          | `null`         |
| `params`        | `object` | Source parameters as defined in [source config docs](../configuration/source-config.md). | _required_    |

:::warning

While updating `num_pipelines` and `transform` is generally safe and reversible, updating `params` has consequences specific to the source type and might have side effects such as loosing the source's checkpoints. Perform such updates with great care. 

:::

**Payload Example**

curl -XPOST http://localhost:7280/api/v1/indexes/my-index/sources --data @source_config.json -H "Content-Type: application/json"

```json title="source_config.json
{
    "version": "0.9",
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

### Toggle source

```
PUT api/v1/indexes/<index id>/sources/<source id>/toggle
```

Toggle (enable/disable) source `source id` of index ID `index id`.

It returns an empty body.

#### PUT payload

| Variable          | Type     | Description                                                                                          |
|-------------------|----------|------------------------------------------------------------------------------------------------------|
| `enable`       | `bool` | If `true` enable the source, else disable it.                                |

### Reset source checkpoint

```
PUT api/v1/indexes/<index id>/sources/<source id>/reset-checkpoint
```

Resets checkpoints of source `source id` of index ID `index id`.

It returns an empty body.

### Delete a source

```
DELETE api/v1/indexes/<index id>/sources/<source id>
```

Delete source of ID `<source id>`.


## Cluster API

This endpoint lets you check the state of the cluster from the point of view of the node handling the request.

```
GET api/v1/cluster?format=pretty_json
```

#### Parameters

Name | Type | Description | Default value
--- | --- | --- | ---
`format` | `String` | The output format requested for the response: `json` or `pretty_json` | `pretty_json`


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
| `index id`  | The index id  |


#### POST payload `DeleteQuery`


| Variable            | Type       | Description                                                                                             | Default value                                      |
|---------------------|------------|---------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| `query`           | `String`   | Query text. See the [query language doc](query-language.md)                                               | _required_                                         |
| `search_field`    | `[String]` | Fields to search on. Comma-separated list, e.g. "field1,field2"                                           | index_config.search_settings.default_search_fields |
| `start_timestamp` | `i64`      | If set, restrict search to documents with a `timestamp >= start_timestamp`. The value must be in seconds. |                                                    |
| `end_timestamp`   | `i64`      | If set, restrict search to documents with a `timestamp < end_timestamp`. The value must be in seconds.    |                                                    |


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
| `create_timestamp` | Create timestamp of the delete query in seconds        |     `i64`     |
| `opstamp`          | Unique operation stamp associated with the delete task |     `u64`     |
| `delete_query`     | The posted delete query                                | `DeleteQuery` |


### List delete queries

```
GET api/v1/<index id>/delete-tasks
```

Get the list of delete tasks for a given `index_id`.


#### Response

The response is an array of `DeleteTask`.
