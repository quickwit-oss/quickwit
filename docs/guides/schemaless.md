---
title: Schemaless
sidebar_position: 1
---

# Strict schema or schemaless?

Quickwit lets you place the cursor on how strict you would like your schema to be. In other words, it is possible to operate Quickwit with a very strict mapping, in an entirely schemaless manner, and anywhere in between. Let's see how this works!

:::note

To execute the CLI commands throughout this guide, [install](/docs/get-started/installation.md) Quickwit and start a server in a terminal with the following command:

```bash
./quickwit run
```

:::

## A strict mapping

That's the most straightforward approach.
As a user, you need to precisely define the list of fields to be ingested by Quickwit.

For instance, a reasonable mapping for an application log could be:

```yaml title=my_strict_index.yaml
version: 0.5

index_id: my_strict_index

doc_mapping:
  mode: strict # <--- The mode attribute
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      precision: seconds
      fast: true
    - name: server
      type: text
      tokenizer: raw
    - name: message
      type: text
      record: position
    - name: severity
      tokenizer: raw
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity, message]

indexing_settings:
  commit_timeout_secs: 5
```

The `mode` attribute controls what should be done if an ingested document
contains a field that is not defined in the document mapping. By default, it takes the `lenient` value. In `lenient` mode, the fields that do not appear in the document mapping will simply be ignored.

If `mode` is set to `strict` on the other hand, documents containing fields
that are not defined in the mapping will be entirely discarded.

## Schemaless with a partial schema

`mode` can take another value: `dynamic`.
When set to dynamic, all extra fields will actually be mapped using a catch-all configuration.
By default, this catch-all configuration indexes and stores all of these fields, but this can be configured by setting the [`dynamic_mapping` attribute](../configuration/index-config#mode).
A minimalist, yet perfectly valid and useful index configuration is then:

```yaml title=my_dynamic_index.yaml
version: 0.5
index_id: my_dynamic_index
doc_mapping:
  mode: dynamic
```

This configuration makes it possible to ingest any JSON object and search them.

However, the dynamic mode can also be used in conjunction with field mappings.
This combination is especially powerful for event logs which cannot be mapped to a single schema.

For instance, let's consider the following user event log:

```json file title=my_logs.json
{
    "timestamp": 1653021741,
    "user_id": "8705a7fak",
    "event_type": "login",
    "ab_groups": ["phoenix-red-ux"]
}
{
    "timestamp": 1653021746,
    "user_id": "7618fe06",
    "event_type": "order",
    "ab_groups": ["phoenix-red-ux", "new-ranker"],
    "cart": [
        {
            "product_id": 120391,
            "product_description": "Cherry Pi: A single-board computer that is compatible..."
        }
    ]
}
{
    "timestamp": 1653021748,
    "user_id": "8705a7fak",
    "event_type": "login",
    "ab_groups": ["phoenix-red-ux"]
}
```

Each event type comes with its own set of attributes. Declaring our mapping as the union of all of these event-specific mappings would be a tedious exercise.

Instead, we can cherry-pick the fields that are common to all of the logs, and rely on dynamic mode to handle the rest.

```yaml title=my_dynamic_index.yaml
version: 0.5
index_id: my_dynamic_index
doc_mapping:
  mode: dynamic
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      precision: seconds
      fast: true
    - name: user_id
      type: text
      tokenizer: raw
    - name: event_type
      type: text
      tokenizer: raw
  timestamp_field: timestamp

indexing_settings:
  commit_timeout_secs: 5  # <--- Your document will be searchable ~5 seconds after you ingest them.
```

Our index is now ready to handle queries like this:

```
event_type:order AND cart.product_id:120391
```

Execute the following commands to create the index, ingest a few documents and search through them:

```bash
cat << EOF > my_dynamic_index.yaml
version: 0.5
index_id: my_dynamic_index
doc_mapping:
  mode: dynamic
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      precision: seconds
      fast: true
    - name: user_id
      type: text
      tokenizer: raw
    - name: event_type
      type: text
      tokenizer: raw
  timestamp_field: timestamp

indexing_settings:
  commit_timeout_secs: 5
EOF

# Create index.
./quickwit index create --index-config ./my_dynamic_index.yaml --overwrite --yes

cat << EOF > my_logs.json
{"timestamp":1653021741,"user_id":"8705a7fak","event_type":"login","ab_groups":["phoenix-red-ux"]}
{"timestamp":1653021746,"user_id":"7618fe06","event_type":"order","ab_groups":["phoenix-red-ux","new-ranker"],"cart":[{"product_id":120391,"product_description":"Cherry Pi: A single-board computer that is compatible..."}]}
{"timestamp":1653021748,"user_id":"8705a7fak","event_type":"login","ab_groups":["phoenix-red-ux"]}
EOF

# Ingest documents.
./quickwit index ingest --index my_dynamic_index --input-path my_logs.json 

# Execute search query.
./quickwit index search --index my_dynamic_index --query "event_type:order AND cart.product_id:120391

```

## A schema with schemaless pockets

Some logs are isolating these event-specific attributes in a
sub-field. For instance, let's have a look at an OpenTelemetry JSON log.

```json title=otel_logs.json
{
  "Timestamp": 1653028151,
  "Attributes": {
    "split_id": "28f897f2-0419-4d88-8abc-ada72b4b5256"
  },
  "Resource": {
    "service": "donut_shop",
    "k8s_pod_uid": "27413708-876b-4652-8ca4-50e8b4a5caa2"
  },
  "TraceId": "f4dbb3edd765f620",
  "SpanId": "43222c2d51a7abe3",
  "SeverityText": "INFO",
  "SeverityNumber": 9,
  "Body": "merge ended"
}
```

In this log, the `Attributes` and the `Resource` fields contain arbitrary key-values.

Quickwit 0.3 introduced a JSON field type to handle this use case.
A good index configuration here could be:

```yaml title=otel_logs.yaml
version: 0.5
index_id: otel_logs
doc_mapping:
  mode: dynamic
  field_mappings:
    - name: Timestamp
      type: datetime
      fast: true
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      precision: seconds
      fast: true
    - name: Attributes
      type: json
      tokenizer: raw
    - name: Resource
      type: json
      tokenizer: raw
    - name: TraceId
      type: text
      tokenizer: raw
    - name: SpanId
      type: text
      tokenizer: raw
    - name: SeverityText
      type: text
      tokenizer: raw
      fast: true
    - name: Body
      type: text
  timestamp_field: Timestamp
  
search_settings:
  default_search_fields: [SeverityText, Body, Attributes, Resource]

indexing_settings:
  commit_timeout_secs: 5
```

We can now naturally search our logs with the following query:

```
merge AND service:donuts_shop
```

Let's execute the following commands to create the index, ingest a document and execute a search query:

```bash
# Create index.
./quickwit index create --index-config ./otel_logs.yaml --overwrite --yes

cat << EOF > otel_logs.json
{"Timestamp":1653028151,"Attributes":{"split_id":"28f897f2-0419-4d88-8abc-ada72b4b5256"},"Resource":{"service":"donut_shop","k8s_pod_uid":"27413708-876b-4652-8ca4-50e8b4a5caa2"},"TraceId":"f4dbb3edd765f620","SpanId":"43222c2d51a7abe3","SeverityText":"INFO","SeverityNumber":9,"Body":"merge ended"}
EOF

# Ingest documents.
./quickwit index ingest --index otel_logs --input-path otel_logs.json

# Execute search query.
./quickwit index search --index otel_logs --query "merge AND service:donut_shop"

```
