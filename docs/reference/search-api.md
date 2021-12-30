---
title: Search REST API
position: 3
---

## API version

All the API endpoints start with the `api/v1/` prefix. `v1` indicates that we are currently using version 1 of the API.


## Format

The API uses **JSON** encoded as **UTF-8**. The body of POST and PUT requests must be a JSON object and their `Content-Type` header should be set to `application/json; charset=UTF-8`.

The body of responses is always a JSON object, and their content type is always `application/json; charset=UTF-8.`

## Parameters

Parameters passed in the URL must be properly URL-encoded, using the UTF-8 encoding for non-ASCII characters.

```
GET [..]/search?query=barack%20obama
```

## Error handling

Successful requests return a 2xx HTTP status code.

Failed requests return a 4xx HTTP status code. The response body of failed requests holds a JSON object containing an `error_message` field that describes the error.

```json
{
	"error_message": "Failed to parse query"
}
```

## Endpoints

### Search in an index

```
GET api/v1/indexes/<index name>/search
```

Search for documents matching a query in the given index `<index name>`.

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| **index name** | The index name |


#### Get parameters

| Variable                  | Type                 | Description                                                                                       | Default value                                                                                   |
| ------------------------- | -------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| **query**                  | `String`           | Query text. See the [query language doc](query-language.md) (mandatory)                                          |                                                                                                |
| **startTimestamp**         | `i64`    		 	    | If set, restrict search to documents with a `timestamp >= start_timestamp`                                                            |                                                                                |
| **endTimestamp**           | `i64`       		    | If set, restrict search to documents with a `timestamp < end_timestamp`                                                            |                                                                                     |
| **startOffset**            | `Integer`     	    | Number of documents to skip                                                                | `0`                                                                                             |
| **maxHits**                | `Integer`          | Maximum number of hits to return (by default 20)                                                            | `20`                                                                                            |
| **searchFields**           | `String`      		  | Fields to search on. Comma-separated list, e.g. "field1,field2" | index_config.search_settings.default_search_fields                                                                                             |
| **format**                 | `Enum`           	| The output format. Allowed values are "json" or "prettyjson" 						 | `prettyjson`                                                                                            |


### Response

| Field                | Description                    |    Type    |
| -------------------- | ------------------------------ | :--------: |
| **hits**             | Results of the query           | `[hit]` |
| **numHits**         | Total number of matches        |  `number`  |
| **elapsedTimeMicros**    | Processing time of the query   |  `number`  |

### Search stream in an index

```
GET api/v1/indexes/<index name>/search/stream
```

Streams field values from ALL documents matching a search query in the given index `<index name>`, in a specified output format among the following:
 -  [CSV](https://datatracker.ietf.org/doc/html/rfc4180)
 -  [ClickHouse RowBinary](https://clickhouse.tech/docs/en/interfaces/formats/#rowbinary)

:::note

The endpoint will return 10 million values if 10 million documents match the query. This is expected, this endpoint is made to support queries matching millions of document and return field values in a reasonable response time.

:::


#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| **index name** | The index name |


#### Get parameters

| Variable | Type | Description | Default value |
|----------|------|-------------|---------------|
| **query** | `String` | Query text. See the [query language doc](query-language.md) (mandatory) | |
| **fastField** | `String` | Name of a field to retrieve from documents. This field must be marked as "fast" in the index config. (mandatory)| |
| **searchFields** | `[String]` | Fields to search on. Comma-separated list, e.g. "field1,field2" | index_config.search_settings.default_search_fields    |
| **startTimestamp** | `i64` | If set, restrict search to documents with a `timestamp >= start_timestamp` | |
| **endTimestamp** | `i64` | If set, restrict search to documents with a `timestamp < end_timestamp`` | |
| **outputFormat** | `String` | Response output format. `csv` or `clickHouseRowBinary`  | `csv` |


### Response
The response is a chunked transfer encoded stream.
It returns a list of all the field values from documents matching the query. The field must be marked as "fast" in the index config for this to work. 
The formatting is based on the specified output format. 

On error a "X-Stream-Error" header will be sent via the the trailers channel with information about the error and the stream will be closed. 
Depending on the client, the trailer header with error details may not be shown. 

