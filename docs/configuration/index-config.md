---
title: Index configuration
sidebar_position: 2
---

This page describes how to configure an index.

In addition to the `index_id`, the index configuration lets you define five items:

- The **index-uri**: it defines where the index files should be stored.
- The **doc mapping**: it defines how a document and the fields it contains are stored and indexed for a given index.
- The **indexing settings**: it defines the timestamp field used for sharding, and some more advanced parameters like the merge policy.
- The **search settings**: it defines the default search fields `default_search_fields`, a list of fields that Quickwit will search into if the user query does not explicitly target a field.
- The (data) **sources**: it defines a list of sources of types like file or Kafka source.

Configuration is set at index creation and cannot be modified except for the sources using the CLI ``quickwit source``  commands.

## Config file format

The index configuration format is YAML. When a key is absent from the configuration file, the default value is used.
Here is a complete example suited for the HDFS logs dataset:

```yaml
version: 0 # File format version.

index_id: "hdfs"

index_uri: "s3://my-bucket/hdfs"

doc_mapping:
  mode: lenient
  field_mappings:
    - name: timestamp
      type: i64
      fast: true
    - name: severity_text
      type: text
      tokenizer: raw
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

indexing_settings:
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]

sources:
 - hdfs: hdfs-log-kafka
   source_type: kafka
   params:
     topic: hdfs-logs
     client_params:
       bootstrap.servers: localhost:9092
       group.id: quickwit-consumer-group
       security.protocol: SSL
```

## Index uri

The index-uri defines where the index files (also called splits) should be stored.
This parameter expects a [storage uri](../reference/storage-uri).


The `index-uri` parameter is optional.
By default, the `index-uri` will be computed by concatenating the `index-id` with the
`default_index_root_uri` defined in the [Quickwit's config](node-config).


:::caution
The file storage will not work when running quickwit in distributed mode.
Today, only the s3 storage is available when running several searcher nodes.
:::


## Doc mapping

The doc mapping defines how a document and the fields it contains are stored and indexed for a given index. A document is a collection of named fields, each having its own data type (text, binary, datetime, bool, i64, u64, f64).

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `field_mappings` | Collection of field mapping, each having its own data type (text, binary, datetime, bool, i64, u64, f64).   | [] |
| `mode`        | Defines how quickwit should handle document fields that are not present in the `field_mappings`. In particular, the "dynamic" mode makes it possible to use quickwit in a schemaless manner. (See [mode](#mode)) | `lenient`
| `dynamic_mapping` | This parameter is only allowed when `mode` is set to `dynamic`. It then defines whether dynamically mapped fields should be indexed, stored, etc.  | (See [mode](#mode))
| `tag_fields` | Collection of fields already defined in `field_mappings` whose values will be stored in a dedicated `tags` (1) | [] |
| `store_source` | Whether or not the original JSON document is stored or not in the index.   | false |

(1) [Learn more on the tags usage](../concepts/querying.md).

### Field types

Each field has a type that indicates the kind of data it contains, such as integer on 64 bits or text.
Quickwit supports the following raw types `text`, `i64`, `u64`, `f64`, `datetime`, `bool`, and `bytes`, and also supports composite types such as array and object. Behind the scenes, Quickwit is using tantivy field types, don't hesitate to look at [tantivy documentation](https://github.com/tantivy-search/tantivy) if you want to go into the details.

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
```

**Parameters for text field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `tokenizer` | Name of the `Tokenizer`, choices between `raw`, `default`, `en_stem` and `chinese_compatible` | `default` |
| `record`    | Describes the amount of information indexed, choices between `basic`, `freq` and `position` | `basic` |
| `fieldnorms` | Whether to store fieldnorms for the field. Fieldnorms are required to calculate the BM25 Score of the document. | `false` |
| `fast`     | Whether value is stored in a fast field. The fast field will contain the term ids. The effective cardinality depends on the tokenizer. When creating fast fields on text fields it is recommended to use the "raw" tokenizer, since it will store the original text unchanged. The "default" tokenizer will store the terms as lower case and this will be reflected in the dictionary ([see tokenizers](#description-of-available-tokenizers)). | `false` |

#### **Description of available tokenizers**

| Tokenizer     | Description   |
| ------------- | ------------- |
| `raw`         | Does not process nor tokenize text  |
| `default`     | Chops the text on according to whitespace and punctuation, removes tokens that are too long, and converts to lowercase |
| `en_stem`     |  Like `default`, but also applies stemming on the resulting tokens  |
| `chinese_compatible` |  Chop between each CJK character in addition to what `default` does. Should be used with `record: position` to be able to properly search |

**Description of record options**

| Record option | Description   |
| ------------- | ------------- |
| `basic`       |  Records only the `DocId`s |
| `freq`        |  Records the document ids as well as the term frequency  |
| `position`    |  Records the document id, the term frequency and the positions of occurrences.  |

Indexing with position is required to run phrase queries.

#### Numeric types: `i64`, `u64` and `f64` type

Quickwit handles three numeric types: `i64`, `u64`, and `f64`.

Numeric values can be stored in a fast field (the equivalent of Lucene's `DocValues`) which is a column-oriented storage.

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

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether the field values are stored in the document store | `true` |
| `indexed`   | Whether the field values are indexed | `true` |
| `fast`      | Whether the field values are stored in a fast field | `false` |

#### `datetime` type

The `datetime` type handles dates and datetimes. Quickwit supports multiple input formats configured independently for each field. The following formats are natively supported:
- `iso8601`, `rfc2822`, `rfc3339`: parse dates using standard ISO and RFC formats.
- `strptime`: parse dates using the Unix [strptime](https://man7.org/linux/man-pages/man3/strptime.3.html) format with few changes:
  - `strptime` format specifiers: `%C`, `%d`, `%D`, `%e`, `%F`, `%g`, `%G`, `%h`, `%H`, `%I`, `%j`, `%k`, `%l`, `%m`, `%M`, `%n`, `%R`, `%S`, `%t`, `%T`, `%u`, `%U`, `%V`, `%w`, `%W`, `%y`, `%Y`, `%%`.
  - `%f` for milliseconds precision support.
  - `%z` timezone offsets can be specified as `(+|-)hhmm` or `(+|-)hh:mm`.

- `unix_ts_secs`, `unix_ts_millis`, `unix_ts_micros`, `unix_ts_nanos`: parse Unix timestamps. At most one Unix timestamp format must be specified.

When a `datetime` field is stored as a fast field, the `precision` parameter indicates the precision used to truncate the values before encoding and compressing them. The `precision` parameter can take the following values: `seconds`, `milliseconds`, `microseconds`.

:::info
When specifying multiple input formats, the corresponding parsers are attempted in the order they are declared.
:::

:::warning
The timezone name format specifier (`%Z`) is not currently supported in `strptime`.
:::

Example of a mapping for a datetime field:

```yaml
name: timestamp
type: datetime
description: Time at which the event was emitted
input_formats:
  - rfc3339
  - unix_ts_millis
  - "%Y %m %d %H:%M:%S.%f %z"
stored: true
indexed: true
fast: true
precision: milliseconds
```

**Parameters for datetime field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `input_formats` | Formats used to parse input dates | [`rfc3339`] |
| `stored`        | Whether the field values are stored in the document store | `true` |
| `indexed`       | Whether the field values are indexed | `true` |
| `fast`          | Whether the field values are stored in a fast field | `false` |
| `precision`     | The precision (`seconds`, `milliseconds`, or `microseconds`) used to store the fast values | `seconds` |

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

#### `bytes` type
The `bytes` type accepts a binary value as a `Base64` encoded string.

Example of a mapping for a bytes field:

```yaml
name: binary
type: bytes
stored: true
indexed: true
fast: true
```

**Parameters for bytes field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`     | Whether value is stored in a fast field. Only on 1:1 cardinality, not supported on `array<bytes>` fields | `false` |

#### `json` type

The `json` type accepts a JSON object.

Example of a mapping for a JSON field:

```yaml
name: parameters
type: json
stored: true
indexed: true
tokenizer: "default"
```

**Parameters for JSON field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `description` | Optional description for the field. | `None` |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `tokenizer` | **Only affects strings in the json object**. Name of the `Tokenizer`, choices between `raw`, `default`, `en_stem` and `chinese_compatible` | `default` |
| `record`    | **Only affects strings in the json object**. Describes the amount of information indexed, choices between `basic`, `freq` and `position` | `basic` |

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

#### **array**

Quickwit supports arrays for all raw types except for `object` types.

To declare an array type of `i64` in the index config, you just have to set the type to `array<i64>`.

#### **object**

Quickwit supports nested objects as long as it does not contain arrays of objects.

```yaml
name: resource
type: object
field_mappings:
  - name: service
    type: text
```

### Mode

The `mode` describes how Quickwit should behave when it receives a field that is not defined in the field mapping.

Quickwit offers you three different modes:
- `lenient`: unmapped fields are dismissed by Quickwit.
- `strict`: if a document contains a field that is not mapped, quickwit will dismiss it, and count it as an error.
- `dynamic`: unmapped fields are gathered by Quickwit and handled as defined in the `dynamic_mapping` parameter.

`dynamic_mapping` offers the same configuration options as when configuring a `json` field. It defaults to:

```yaml
- indexed: true
- stored: true
- tokenizer: raw
- record: basic
```

The `dynamic` mode makes it possible to operate Quickwit in a schemaless manner, or with a partial schema.

If the `dynamic_mapping` has been set as indexed (this is the default),
fields that were mapped thanks to the dynamic mode can be searched, by
targeting the path required to reach them from the root of the json object.

For instance, in a entirely schemaless settings, a minimal index configuration could be:

```yaml
version: 0
index_id: my-dynamic-index
# note we did not map anything.
doc_mapping:
  mode: dynamic
```

We could then index a complex document like the following:

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
`[a-zA-Z][_\.\-a-zA-Z0-9]*$`

In plain language:
- it needs to have at least one character.
- it should only contain latin letter `[a-zA-Z]` digits `[0-9]` or (`.`, `-`, `_`).
- the first character needs to be a letter.

:::caution
For field names containing the `.` character, you will need to escape it when referencing them. Otherwise the `.` character will be interpreted as a JSON object property access. Because of this, it is recommended to avoid using field names containing the `.` character.
:::

### Behavior with fields not defined in the config

Fields in your JSON document that are not defined in the `index config` will be ignored.

### Behavior with null values or missing fields

Fields with `null` or missing fields in your JSON document will be silently ignored when indexing.

## Indexing settings

This section describes indexing settings for a given index.

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `timestamp_field`      | Timestamp field used for sharding documents in splits (1).   | None |
| `commit_timeout_secs`      | Maximum number of seconds before committing a split since its creation.   | 60 |
| `split_num_docs_target` | Target number of docs per split.   | 10_000_000 |
| `merge_policy` | Describes the strategy used to trigger split merge operations (see [Merge policies](#merge-policies) section below). |
| `resources.heap_size`      | Indexer heap size per source per index.   | 2_000_000_000 |

(1) Both `datetime` and `i64` can be referenced. `i64` fields are interpreted as Unix timestamp (seconds). You can learn more about time sharding [here](./../concepts/architecture.md).

### Merge policies

Quickwit makes it possible to define the strategy used to decide which splits should be merged together and when.

Quickwit offers three different merge policies, each with their
own set of parameters.

#### "Stable log" merge policy

The stable log merge policy attempts to minimize write amplification AND keep time-pruning power as high as possible, by merging splits with a similar size, and with a close time span.

Quickwit's default merge policy is the `stable_log` merge policy
with the following parameters:

```yaml
version: 0
index_id: "hdfs"
# ...
indexing_settings:
  merge_policy:
    type: "stable_log"
    min_level_num_docs: 100_000
    merge_factor: 10
    max_merge_factor: 12
```


| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `merge_factor`      | *(advanced)* Number of splits to merge together in a single merge operation.   | 10 |
| `max_merge_factor` | *(advanced)* Maximum number of splits that can be merged together in a single merge operation.  | 12 |
| `min_level_num_docs` |  *(advanced)* Number of docs below which all splits are considered as belonging to the same level.   | 100_000 |


#### "Limit Merge" merge policy

*The limit merge policy is considered advanced*.

The limit merge policy simply limits write amplification by setting an upperbound
of the number of merge operation a split should undergo.


```yaml
version: 0
index_id: "hdfs"
# ...
indexing_settings:
  merge_policy:
    type: "limit_merge"
    max_merge_ops: 5
    merge_factor: 10
    max_merge_factor: 12

```


| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `max_merge_ops`   |  Maximum number of merges that a given split should undergo. | 4 |
| `merge_factor`      | *(advanced)* Number of splits to merge together in a single merge operation.   | 10 |
| `max_merge_factor` | *(advanced)* Maximum number of splits that can be merged together in a single merge operation.  | 12 |

#### No merge

The `no_merge` merge policy entirely disables merging.

:::caution
This setting is not recommended. Merges are necessary to reduce the number of splits, and hence improve search performances.
:::

```yaml
version: 0
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
| `search_default_fields`      | Default list of fields that will be used for search.   | None |

## Sources

An index can have one or several data sources. [Learn how to configure them](source-config.md).
