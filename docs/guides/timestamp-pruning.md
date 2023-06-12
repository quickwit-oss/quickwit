---
title: Timestamp pruning
sidebar_position: 4
---

Quickwit indexes documents in batching fashion by outputting self-contained index files called [splits](../architecture/#splits). Over time, the number of splits created in a Quickwit index grows as long as new documents are being indexed. This number of splits linearly hurts query performance because Quickwit needs to examine all the splits in an index during query execution.

Quickwit performs some maintenance techniques to keep the number of splits low within an index: applying merge policy and deleting data based on the retention policy.
Time pruning is another technique that can also greatly reduce the number splits needed during query execution. The only catch being that: it's only applicable to time series datasets.

## How it works

When ingesting a time series dataset, splits have a `start_timestamp` and `end_timestamp` attributes computed and saved in their metadata:
- `start_timestamp`: lowest timestamp seen on indexed documents in the split.
- `end_timestamp`: highest timestamp seen on indexed documents in the split.

Quickwit's technique consists of taking advantage of the fact that most time series queries filter on time range. By specifying a `start_timestamp` and/or `end_timestamp` for a query, Quickwit can use the split metadata to prune splits not satisfying this time range, therefore skipping the need to examine unnecessarily more splits.

## How to use it

To take advantage of timestamp pruning feature, you will first need to configure a `timestamp_field` on your index [doc-mapper configuration](https://quickwit.io/docs/configuration/index-config#doc-mapping). Needless to say that the configured field should have the correct type (`datetime`).

```yaml
version: 0.5 # File format version.

index_id: "hdfs"

index_uri: "s3://my-bucket/hdfs"

doc_mapping:
  mode: lenient
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      precision: seconds
      fast: true
    - name: severity_text
      type: text
      tokenizer: raw
      fast: 
        - tokenizer: lowercase
    - name: body
      type: text
      tokenizer: default
      record: position
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]
```

Once that's done, timestamp prunning will kick in whenever you reference the timestamp field in a query. It is also possible to explicitly triger timestamp prunning by setting the corresponding [query parameters](https://quickwit.io/docs/reference/rest-api#parameters-1) when searching your index.

### Referencing the timestamp field in a query

Please note that when referencing the timestamp field in a query, only the folowing datetime formats are supported: `Rfc3339`, `Rfc2822`, `Timestamp`, `%Y-%m-%d %H:%M:%S.%f`, `%Y-%m-%d %H:%M:%S`, `%Y-%m-%d`, and `%Y/%m/%d`.

```bash
curl -X POST -H 'Content-Type: application/json' \
-d '{"query": "severity_text:ERROR AND timestamp:[2002-10-02T15:00:00Z TO 2002-10-02T18:00:00Z]"}' \
http://127.0.0.1:7280/api/v1/hdfs/search
 ```

### Using the search query parameters

- `start_timestamp`: A timestamp in seconds that restricts search to documents with a `timestamp` >= `start_timestamp`.
- `end_timestamp` : A timestamp in seconds that restricts search to documents with a `timestamp` < `end_timestamp`.

```bash
curl -X POST -H 'Content-Type: application/json' \
-d '{"query": "severity_text:ERROR", "start_timestamp":1680879600 "end_timestamp":1680890400}' \
http://127.0.0.1:7280/api/v1/hdfs/search
 ```
