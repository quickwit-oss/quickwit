---
title: Index configuration
sidebar_position: 3
toc_max_heading_level: 4
---

This page describes how to configure an index.

In addition to the `index_id`, the index configuration lets you define five items:

- The **index-uri**: it defines where the index files should be stored.
- The **doc mapping**: it defines how a document and the fields it contains are stored and indexed for a given index.
- The **indexing settings**: it defines the timestamp field used for sharding, and some more advanced parameters like the merge policy.
- The **search settings**: it defines the default search fields `default_search_fields`, a list of fields that Quickwit will search into if the user query does not explicitly target a field.
- The **retention policy**: it defines how long Quickwit should keep the indexed data. If not specified, the data is stored forever.

Configuration is set at index creation and can be changed using the [update endpoint](../reference/rest-api.md) or the [CLI](../reference/cli.md).

## Config file format

The index configuration format is YAML. When a key is absent from the configuration file, the default value is used.
Here is a complete example suited for the HDFS logs dataset:

```yaml
version: 0.7 # File format version.

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
      fast_precision: seconds
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
    - name: resource
      type: object
      field_mappings:
        - name: service
          type: text
          tokenizer: raw
  tag_fields: ["resource.service"]
  timestamp_field: timestamp
  index_field_presence: true

search_settings:
  default_search_fields: [severity_text, body]

retention:
  period: 90 days
  schedule: daily
```

## Index ID

The index ID is a string that uniquely identifies the index within the metastore. It may only contain uppercase or lowercase ASCII letters, digits, hyphens (`-`), and underscores (`_`). Finally, it must start with a letter and contain at least 3 characters but no more than 255.

## Index uri

The index-uri defines where the index files (also called splits) should be stored.
This parameter expects a [storage uri](storage-config#storage-uris).

The `index-uri` parameter is optional.
By default, the `index-uri` will be computed by concatenating the `index-id` with the
`default_index_root_uri` defined in the [Quickwit's config](node-config).

:::caution
The file storage will not work when running quickwit in distributed mode. Instead, AWS S3, Azure Blob Storage, Google Cloud Storage (in s3 interoperability mode) or other S3-compatible storage systems including Scaleway Object Storage and Garage should be used as storage when running several searcher nodes.
:::

## Doc mapping

The doc mapping defines how a document and the fields it contains are stored and indexed for a given index. A document is a collection of named fields, each having its own data type (text, bytes, datetime, bool, i64, u64, f64, ip, json).

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `field_mappings` | Collection of field mapping, each having its own data type (text, binary, datetime, bool, i64, u64, f64, ip, json).   | `[]` |
| `mode`        | Defines how quickwit should handle document fields that are not present in the `field_mappings`. In particular, the "dynamic" mode makes it possible to use quickwit in a schemaless manner. (See [mode](#mode)) | `dynamic`
| `dynamic_mapping` | This parameter is only allowed when `mode` is set to `dynamic`. It then defines whether dynamically mapped fields should be indexed, stored, etc.  | (See [mode](#mode))
| `tag_fields` | Collection of fields* already defined in `field_mappings` whose values will be stored as part of the `tags` metadata. [Learn more about tags](../overview/concepts/querying.md#tag-pruning). | `[]` |
| `store_source` | Whether or not the original JSON document is stored or not in the index.   | `false` |
| `timestamp_field`      | Timestamp field* used for sharding documents in splits. The field has to be of type `datetime`. [Learn more about time sharding](./../overview/architecture.md).  | `None` |
| `partition_key`   |  If set, quickwit will route documents into different splits depending on the field name declared as the `partition_key`. | `null` |
| `max_num_partitions`  | Limits the number of splits created through partitioning. (See [Partitioning](../overview/concepts/querying.md#partitioning))  |    `200` |
| `index_field_presence` | `exists` queries are enabled automatically for fast fields. To enable it for all other fields set this parameter to `true`. Enabling it can have a significant CPU-cost on indexing.  |  false |

*: tags fields and timestamp field are expressed as a path from the root of the JSON object to the given field. If a field name contains a `.` character, it needs to be escaped with a `\` character.

### Field types

Each field[^1] has a type that indicates the kind of data it contains, such as integer on 64 bits or text.
Quickwit supports the following raw types [`text`](#text-type), [`i64`](#numeric-types-i64-u64-and-f64-type), [`u64`](#numeric-types-i64-u64-and-f64-type), [`f64`](#numeric-types-i64-u64-and-f64-type), [`datetime`](#datetime-type), [`bool`](#bool-type), [`ip`](#ip-type), [`bytes`](#bytes-type), and [`json`](#json-type), and also supports composite types such as array and object. Behind the scenes, Quickwit is using tantivy field types, don't hesitate to look at [tantivy documentation](https://github.com/tantivy-search/tantivy) if you want to go into the details.

### Raw types

#### Text type

This field is a text field that will be analyzed and split into tokens before indexing.
This kind of field is tailored for full-text search.

Example of a mapping for a text field:

```yaml
name: body
description: Body of the document
type: text
tokenizer: default
record: position
fieldnorms: true
fast:
  normalizer: lowercase
```

**Parameters for text field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value should be indexed so it can be searched | `true` |
| `tokenizer` | Name of the `Tokenizer`. ([See tokenizers](#description-of-available-tokenizers)) for a list of available tokenizers.  | `default` |
| `record`    | Describes the amount of information indexed, choices between `basic`, `freq` and `position` | `basic` |
| `fieldnorms` | Whether to store fieldnorms for the field. Fieldnorms are required to calculate the BM25 Score of the document. | `false` |
| `fast`     | Whether value is stored in a fast field. The fast field will contain the term ids and the dictionary. The default behaviour for `true` is to store the original text unchanged. The normalizers on the fast field is separately configured. It can be configured via `normalizer: lowercase`. ([See normalizers](#description-of-available-normalizers)) for a list of available normalizers. | `false` |

##### Description of available tokenizers

| Tokenizer     | Description   |
| ------------- | ------------- |
| `raw`         | Does not process nor tokenize text. Filters out tokens larger than 255 bytes.  |
| `raw_lowercase` | Does not tokenize text, but lowercase it. Filters out tokens larger than 255 bytes.  |
| `default`     | Chops the text on according to whitespace and punctuation, removes tokens that are too long, and converts to lowercase. Filters out tokens larger than 255 bytes. |
| `en_stem`     | Like `default`, but also applies stemming on the resulting tokens. Filters out tokens larger than 255 bytes.  |
| `whitespace`  | Chops the text on according to whitespace only. Doesn't remove long tokens or converts to lowercase. |
| `chinese_compatible` |  Chop between each CJK character in addition to what `default` does. Should be used with `record: position` to be able to properly search |
| `lowercase`   | Applies a lowercase transformation on the text. It does not tokenize the text. |

##### Description of available normalizers

| Normalizer     | Description   |
| ------------- | ------------- |
| `raw`         | Does not process nor tokenize text. Filters token larger than 255 bytes.  |
| `lowercase` |  Applies a lowercase transformation on the text. Filters token larger than 255 bytes. |

**Description of record options**

| Record option | Description   |
| ------------- | ------------- |
| `basic`       |  Records only the `DocId`s |
| `freq`        |  Records the document ids as well as the term frequency  |
| `position`    |  Records the document id, the term frequency and the positions of occurrences.  |

Indexing with position is required to run phrase queries.

#### Numeric types: `i64`, `u64` and `f64` type

Quickwit handles three numeric types: `i64`, `u64`, and `f64`.

Numeric values can be stored in a fast field (the equivalent of Lucene's `DocValues`), which is a column-oriented storage used for range queries and aggregations.

When querying negative numbers without precising a field (using `default_search_fields`), you should single-quote the number (for instance '-5'), otherwise it will be interpreted as wanting to match anything but that number.

Example of a mapping for an u64 field:

```yaml
name: rating
description: Score between 0 and 5
type: u64
stored: true
indexed: true
fast: true
```

**Parameters for i64, u64 and f64 field**

| Variable        | Description   | Default value |
| --------------- | ------------- | ------------- |
| `description`   | Optional description for the field. | `None` |
| `stored`        | Whether the field values are stored in the document store. | `true` |
| `indexed`       | Whether the field values are indexed. | `true` |
| `fast`          | Whether the field values are stored in a fast field. | `false` |
| `coerce`        | Whether to convert numbers passed as strings to integers or floats. | `true` |
| `output_format` | JSON type used to return numbers in search results. Possible values are `number` or `string`. | `number` |

#### `datetime` type

The `datetime` type handles dates and datetimes. Since JSON doesn’t have a date type, the `datetime` field support multiple input types and formats. The supported input types are:
- floating-point or integer numbers representing a Unix timestamp
- strings containing a formatted date, datetime, or Unix timestamp

The `input_formats` field parameter specifies the accepted date formats. The following input formats are natively supported:
- `iso8601`
- `rfc2822`
- `rfc3339`
- `strptime`
- `unix_timestamp`

**Input formats**

When specifying multiple input formats, the corresponding parsers are attempted in the order they are declared. The following formats are natively supported:
- `iso8601`, `rfc2822`, `rfc3339`: parse dates using standard ISO and RFC formats.
- `strptime`: parse dates using the Unix [strptime](https://man7.org/linux/man-pages/man3/strptime.3.html) format with some variations:
  - `strptime` format specifiers: `%C`, `%d`, `%D`, `%e`, `%F`, `%g`, `%G`, `%h`, `%H`, `%I`, `%j`, `%k`, `%l`, `%m`, `%M`, `%n`, `%R`, `%S`, `%t`, `%T`, `%u`, `%U`, `%V`, `%w`, `%W`, `%y`, `%Y`, `%%`.
  - `%f` for milliseconds precision support.
  - `%z` timezone offsets can be specified as `(+|-)hhmm` or `(+|-)hh:mm`.

:::warning
The timezone name format specifier (`%Z`) is not supported currently.
:::

- `unix_timestamp`: parse float and integer numbers to Unix timestamps. Floating-point values are converted to timestamps expressed in seconds. Integer values are converted to Unix timestamps whose precision, determined in `seconds`, `milliseconds`, `microseconds`, or `nanoseconds`, is inferred from the number of input digits. Internally, datetimes are converted to UTC (if the time zone is specified) and stored as *i64* integers. As a result, Quickwit only supports timestamp values ranging from `Apr 13, 1972 23:59:55` to `Mar 16, 2242 12:56:31`.

:::warning
Converting timestamps from float to integer values may occurs with a loss of precision.
:::

When a `datetime` field is stored as a fast field, the `fast_precision` parameter indicates the precision used to truncate the values before encoding, which improves compression (truncation here means zeroing). The `fast_precision` parameter can take the following values: `seconds`, `milliseconds`, `microseconds`, or `nanoseconds`. It only affects what is stored in fast fields when a `datetime` field is marked as "fast". Finally, operations on `datetime` fast fields, e.g. via aggregations, need to be done at the nanosecond level.

:::info
Internally `datetime` is stored in `nanoseconds` in fast fields and in the docstore, and in `seconds` in the term dictionary.
:::

In addition, Quickwit supports the `output_format` field parameter to specify with which precision datetimes are deserialized. This parameter supports the same value as input formats except for `unix_timestamp` which is replaced by the following formats:
- `unix_timestamp_secs`: displays timestamps in seconds.
- `unix_timestamp_millis`: displays timestamps in milliseconds.
- `unix_timestamp_micros`: displays timestamps in microseconds.
- `unix_timestamp_nanos`: displays timestamps in nanoseconds.

Example of a mapping for a datetime field:

```yaml
name: timestamp
type: datetime
description: Time at which the event was emitted
input_formats:
  - rfc3339
  - unix_timestamp
  - "%Y %m %d %H:%M:%S.%f %z"
output_format: unix_timestamp_secs
stored: true
indexed: true
fast: true
fast_precision: milliseconds
```

**Parameters for datetime field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `input_formats` | Formats used to parse input dates | [`rfc3339`, `unix_timestamp`] |
| `output_format` | Format used to display dates in search results | `rfc3339` |
| `stored`        | Whether the field values are stored in the document store | `true` |
| `indexed`       | Whether the field values are indexed | `true` |
| `fast`          | Whether the field values are stored in a fast field | `false` |
| `fast_precision`     | The precision (`seconds`, `milliseconds`, `microseconds`, or `nanoseconds`) used to store the fast values. | `seconds` |

#### `bool` type

The `bool` type accepts boolean values.

Example of a mapping for a boolean field:

```yaml
name: is_active
description: Activation status
type: bool
stored: true
indexed: true
fast: true
```

**Parameters for bool field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`      | Whether value is stored in a fast field | `false` |

#### `ip` type

The `ip` type accepts IP address values, both IpV4 and IpV6 are supported. Internally IpV4 are converted to IpV6.

Example of a mapping for an IP field:

```yaml
name: host_ip
description: Host IP address
type: ip
fast: true
```

**Parameters for IP field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`      | Whether value is stored in a fast field | `false` |


#### `bytes` type
The `bytes` type accepts a binary value as a `Base64` encoded string.

Example of a mapping for a bytes field:

```yaml
name: binary
type: bytes
stored: true
indexed: true
fast: true
input_format: hex
output_format: hex
```

**Parameters for bytes field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`     | Whether value is stored in a fast field. Only on 1:1 cardinality, not supported on `array<bytes>` fields | `false` |
| `input_format`   | Encoding used to represent input bytes, either `hex` or `base64` | `base64` |
| `output_format`   |  Encoding used to represent bytes in search results, either `hex` or `base64` | `base64` |

#### `json` type

The `json` type accepts a JSON object.

Example of a mapping for a JSON field:

```yaml
name: parameters
type: json
stored: true
indexed: true
tokenizer: raw
expand_dots: false
fast:
  normalizer: lowercase
```

**Parameters for JSON field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`     | Whether value is stored in a fast field. The default behaviour for text in the JSON is to store the text unchanged. An normalizer can be configured via `normalizer: lowercase`. ([See normalizers](#description-of-available-normalizers)) for a list of available normalizers. | `true` |
| `tokenizer` | **Only affects strings in the json object**. Name of the `Tokenizer`, choices between `raw`, `default`, `en_stem` and `chinese_compatible` | `raw` |
| `record`    | **Only affects strings in the json object**. Describes the amount of information indexed, choices between `basic`, `freq` and `position` | `basic` |
| `expand_dots`    | If true, json keys containing a `.` should be expanded. For instance, if `expand_dots` is set to true, `{"k8s.node.id": "node-2"}` will be indexed as if it was `{"k8s": {"node": {"id": "node2"}}}`. The benefit is that escaping the `.` will not be required at query time. In other words, `k8s.node.id:node2` will match the document. This does not impact the way the document is stored.  | `true` |

Note that the `tokenizer` and the `record` have the same definition and the same effect as for the text field.

To search into a json object, one then needs to extend the field name with the path that will lead to the target value.

For instance, when indexing the following object:
```json
{
    "product_name": "droopy t-shirt",
    "attributes": {
        "color": ["red", "green", "white"],
        "size:": "L"
    }
}
```

Assuming `attributes` as been defined as a field mapping as follows:
```yaml
- type: json
  name: attributes
```

`attributes.color:red` is then a valid query.

If, in addition, `attributes` is set as a default search field, then `color:red` is a valid query.

### Composite types

#### array

Quickwit supports arrays for all raw types except for `object` types.

To declare an array type of `i64` in the index config, you just have to set the type to `array<i64>`.

#### object

Quickwit supports nested objects as long as it does not contain arrays of objects.

```yaml
name: resource
type: object
field_mappings:
  - name: service
    type: text
```

#### concatenate

Quickwit supports mapping the content of multiple fields to a single one. This can be more efficient at query time than
searching through dozens of `default_search_fields`. It also allow querying inside a json field without knowing the path
to the field being searched.

```yaml
name: my_default_field
type: concatenate
concatenate_fields:
  - text # things inside text, tokenized with the `default` tokenizer
  - resource.author # all fields in resource.author, assuming resource is an `object` field.
include_dynamic_fields: true
tokenizer: default
record: basic
```

Concatenate fields don't support fast fields, and are never stored. They uses their own tokenizer, independently of the
tokenizer configured on the individual fields.
At query time, concatenate fields don't support range queries.
Only the following types are supported inside a concatenate field: text, bool, i64, u64, f64, json. Other types are rejected
at index creation, or silently discarded during indexation if they are found inside a json field.
Adding an object field to a concatenate field doesn't automatically add its subfields (yet).
<!-- typing is made so it wouldn't be too hard do add, as well as things like params_* matching all fields which starts name with params_ , but the feature isn't implemented yet -->
It isn't possible to add subfields from a json field to a concatenate field. For instance if `attributes` is a json field, it's not possible to add only `attributes.color` to a concatenate field.

For json fields and dynamic fields, the path is not indexed, only values are. For instance, given the following document:
```json
{
  "421312": {
    "my-key": "my-value"
  }
}
```
It is possible to search for `my-value` despite not knowing the full path, but it isn't possible to search for all documents containing a key `my-key`.

<!--
when the features are supported, add these:
  - params_* # shortcut for all fields starting with `params_`
  - resource.author # all fields in resource.author, assuming resource is either of type `object` or `json`
---
Only the following types are supported inside a concatenate field: text, datetime, bool, i64, u64, ip, json. Other types are rejected
---
Datetime can only be queried in their RFC-3339 form, possibly omitting later components. # todo! will have to confirm this is achievable
---
plan:
- implement text/bool/i64/u64 (nothing to do on search side for it to work). all gets converted to strings
- add json
- add object
- add dynamic
-- you are here
- add wildcard
- add json sub-fields?
- add datetime (at index time, generate multiple tokens for yyyy, yyyy-MM... to yyyy-MM-ddThh:mm:ss; at search time, emit both tokenized and "raw" version of what may look like a datetime)
- check negative i64 works as intended for non-raw tokenizer, and leverage datetime code if it doesn't
- add ip (at index time, convert to single token; at search time, emit both tokenized and "raw" version of the ip)
- allow optionally indexing json path (how do we tokenize it? split at each dot, or not?)
-->

### Mode

The `mode` describes how Quickwit should behave when it receives a field that is not defined in the field mapping.

Quickwit offers you three different modes:
- `dynamic` (default value): unmapped fields are gathered by Quickwit and handled as defined in the `dynamic_mapping` parameter.
- `lenient`: unmapped fields are dismissed by Quickwit.
- `strict`: if a document contains a field that is not mapped, quickwit will dismiss it, and count it as an error.

#### Dynamic Mapping

`dynamic` mode makes it possible to operate Quickwit in a schemaless manner, or with a partial schema.
The configuration of `dynamic` mode can be set via the `dynamic_mapping` parameter.
`dynamic_mapping` offers the same configuration options as when configuring a `json` field. It defaults to:

```yaml
version: 0.7
index_id: my-dynamic-index
doc_mapping:
  mode: dynamic
  dynamic_mapping:
    indexed: true
    stored: true
    tokenizer: default
    record: basic
    expand_dots: true
    fast: true
```

When the `dynamic_mapping` is set as indexed (default), fields mapped through
dynamic mode can be searched by targeting the path needed to access them from
the root of the JSON object.

For instance, in a entirely schemaless settings, a minimal index configuration could be:

```yaml
version: 0.7
index_id: my-dynamic-index
doc_mapping:
    # If you have a timestamp field, it is important to tell quickwit about it.
    timestamp_field: unix_timestamp
    # mode: dynamic #< Commented out, as dynamic is the default mode.
```

With such a simple configuration, we can index a complex document like the following:

```json
{
  "endpoint": "/admin",
  "query_params": {
    "ctk": "e42bb897d",
    "page": "eeb"
  },
  "src": {
    "ip": "8.8.8.8",
    "port": 53,
  },
  //...
}
```

The following queries are then valid, and match the document above.

```bash
// Fields can be searched simply.
endpoint:/admin

// Nested object can be queried by specifying a `.` separated
// path from the root of the json object to the given field.
query_params.ctk:e42bb897d

// numbers are searchable too
src.port:53

// and of course we can combine them with boolean operators.
src.port:53 AND query_params.ctk:e42bb897d
```

### Field name validation rules

Currently Quickwit only accepts field name that matches the following regular expression:
`^[@$_\-a-zA-Z][@$_/\.\-a-zA-Z0-9]{0,254}$`

In plain language:
- it needs to have at least one character.
- it can only contain uppercase and lowercase ASCII letters `[a-zA-Z]`, digits `[0-9]`, `.`, hyphens `-`, underscores `_`, slash `/`, at `@` and dollar `$` signs.
- it must not start with a dot or a digit.
- it must be different from Quickwit's reserved field mapping names `_source`, `_dynamic`, `_field_presence`.

:::caution
For field names containing the `.` character, you will need to escape it when referencing them. Otherwise the `.` character will be interpreted as a JSON object property access. Because of this, it is recommended to avoid using field names containing the `.` character.
:::

### Behavior with null values or missing fields

Fields with `null` or missing fields in your JSON document will be silently ignored when indexing.

## Indexing settings

This section describes indexing settings for a given index.

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `commit_timeout_secs`      | Maximum number of seconds before committing a split since its creation.   | `60` |
| `split_num_docs_target` | Target number of docs per split.   | `10000000` |
| `merge_policy` | Describes the strategy used to trigger split merge operations (see [Merge policies](#merge-policies) section below). |
| `resources.heap_size`      | Indexer heap size per source per index.   | `2000000000` |
| `docstore_compression_level` | Level of compression used by zstd for the docstore. Lower values may increase ingest speed, at the cost of index size | `8` |
| `docstore_blocksize` | Size of blocks in the docstore, in bytes. Lower values may improve doc retrieval speed, at the cost of index size | `1000000` |

:::note

Choosing an appropriate commit timeout is critical. With a shorter commit timeout, ingested data is more quickly queryable. But the published splits will be smaller, increasing the overhead associated with [merges](#merge-policies). 

When decommissioning definitively a indexer node that received data through the ingest API (including the [Elastic bulk API](/docs/reference/es_compatible_api) and the OTEL [log](/docs/log-management/otel-service.md) and [trace](/docs/distributed-tracing/otel-service.md) services), we need to make sure that all the data that was staged locally (Write Ahead Log) is indexed. After receiving the termination signal, the Quickwit process waits for the local indexing pipelines to complete. This can take as long as the longest commit timeout of all indexes. Make sure that the termination grace period of the infrastructure supporting the Quickwit indexer nodes is long enough (e.g [`terminationGracePeriodSeconds`](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/) in Kubernetes or [`stopTimeout`](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html) on AWS ECS).

:::

### Merge policies

Quickwit makes it possible to define the strategy used to decide which splits should be merged together and when.

Quickwit offers three different merge policies, each with their
own set of parameters.

#### "Stable log" merge policy

The stable log merge policy attempts to minimize write amplification AND keep time-pruning power as high as possible, by merging splits with a similar size, and with a close time span.

Quickwit's default merge policy is the `stable_log` merge policy
with the following parameters:

```yaml
version: 0.7
index_id: "hdfs"
# ...
indexing_settings:
  merge_policy:
    type: "stable_log"
    min_level_num_docs: 100000
    merge_factor: 10
    max_merge_factor: 12
    maturation_period: 48h
```


| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `merge_factor`      | *(advanced)* Number of splits to merge together in a single merge operation.   | `10` |
| `max_merge_factor` | *(advanced)* Maximum number of splits that can be merged together in a single merge operation.  | `12` |
| `min_level_num_docs` |  *(advanced)* Number of docs below which all splits are considered as belonging to the same level.   | `100000` |
| `maturation_period` | Duration after which a split is considered mature, and won't be considered for merges anymore. May impact the completion time of pending delete tasks. | `48h` |

#### "Limit Merge" merge policy

*The limit merge policy is considered advanced*.

The limit merge policy simply limits write amplification by setting an upperbound
of the number of merge operation a split should undergo.


```yaml
version: 0.7
index_id: "hdfs"
# ...
indexing_settings:
  merge_policy:
    type: "limit_merge"
    max_merge_ops: 5
    merge_factor: 10
    max_merge_factor: 12
    maturation_period: 48h
```


| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `max_merge_ops`   |  Maximum number of merges that a given split should undergo. | `4` |
| `merge_factor`      | *(advanced)* Number of splits to merge together in a single merge operation.   | `10` |
| `max_merge_factor` | *(advanced)* Maximum number of splits that can be merged together in a single merge operation.  | `12` |
| `maturation_period` | Duration after which a split is considered mature, and won't be considered for merges anymore. May impact the completion time of pending delete tasks. | `48h` |

#### No merge

The `no_merge` merge policy entirely disables merging.

:::caution
This setting is not recommended. Merges are necessary to reduce the number of splits, and hence improve search performances.
:::

```yaml
version: 0.7
index_id: "hdfs"
indexing_settings:
    merge_policy:
        type: "no_merge"
```



### Indexer memory usage

Indexer works with a default heap of 2 GiB of memory. This does not directly reflect the overall memory usage, but doubling this value should give a fair approximation.


## Search settings

This section describes search settings for a given index.

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `default_search_fields` | Default list of fields that will be used for search. The field names in this list may be declared explicitly in the schema, or may refer to a field captured by the dynamic mode. | `None` |

## Retention policy

This section describes how Quickwit manages data retention. In Quickwit, the retention policy manager drops data on a split basis as opposed to individually dropping documents. Splits are evaluated based on their `time_range` which is derived from the index timestamp field specified in the (`doc_mapping.timestamp_field`) settings. Using this setting, the retention policy will delete a split when `now() - split.time_range.end >= retention_policy.period`

```yaml
version: 0.7
index_id: hdfs
# ...
retention:
  period: 90 days
  schedule: daily
```

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `period`      | Duration after which splits are dropped, expressed in a human-readable way (`1 day`, `2 hours`, `a week`, ...). | required |
| `schedule`    | Frequency at which the retention policy is evaluated and applied, expressed as a cron expression (`0 0 * * * *`) or human-readable form (`hourly`, `daily`, `weekly`, `monthly`, `yearly`). | `hourly` |


`period` is specified as set of time spans. Each time span is an integer followed by a unit suffix like: `2 days 3h 24min`. The supported units are:
  - `nsec`, `ns` -- nanoseconds
  - `usec`, `us` -- microseconds
  - `msec`, `ms` -- milliseconds
  - `seconds`, `second`, `sec`, `s`
  - `minutes`, `minute`, `min`, `m`
  - `hours`, `hour`, `hr`, `h`
  - `days`, `day`, `d`
  - `weeks`, `week`, `w`
  - `months`, `month`, `M` -- a month is defined as `30.44 days`
  - `years`, `year`, `y` -- a year is defined as `365.25 days`
