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

| Variable | Type | Description | Default value |
|----------|------|-------------|---------------|
| **query** | `String` | Query text. See the [query language doc](query-language.md) (mandatory) | |
| **searchFields** | `[String]` | If set, specify the set of fields the search will performed on | |
| **startTimestamp** | `i64` | If set, restrict search to documents with a `timestamp >= start_timestamp` | |
| **endTimestamp** | `i64` | If set, restrict search to documents with a `timestamp < end_timestamp`` | |
| **startOffset** | `Integer` | Number of documents to skip | `0` |
| **maxHits** | `Integer` | Maximum number of hits to return (by default 20) | `20` |


### Response

| field                | Description                    |    type    |
| -------------------- | ------------------------------ | :--------: |
| **hits**             | Results of the query           | `[hit]` |
| **numHits**         | Total number of matches        |  `number`  |
| **numMicrosecs**    | Processing time of the query   |  `number`  |



### Export search from an index

```
GET api/v1/indexes/<index name>/search/stream
```

Export documents matching a search query in the given index `<index name>`, in a specified output format.

The output format can be one of the following:
 -  [Comma Separated Values](https://datatracker.ietf.org/doc/html/rfc4180)
 -  [ClickHouse Row Binary](https://clickhouse.tech/docs/en/interfaces/formats/#rowbinary)

#### Path variable

| Variable      | Description   |
| ------------- | ------------- |
| **index name** | The index name |


#### Get parameters

| Variable | Type | Description | Default value |
|----------|------|-------------|---------------|
| **query** | `String` | Query text. See the [query language doc](query-language.md) (mandatory) | |
| **fastField** | `String` | Name of fast field to retrieve from documents (mandatory)| |
| **searchFields** | `[String]` | If set, specify the set of fields the search will performed on | |
| **startTimestamp** | `i64` | If set, restrict search to documents with a `timestamp >= start_timestamp` | |
| **endTimestamp** | `i64` | If set, restrict search to documents with a `timestamp < end_timestamp`` | |
| **outputFormat** | `String` | Response output format. `csv` or `clickHouseRowBinary`  | `csv` | |


### Response

The response is a list of fast field values. The formatting is based on the specified output format. 
