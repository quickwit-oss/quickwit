---
title: Index configuration
position: 4
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
This parameter expects a [storage uri](storage-uri).


The `index-uri` parameter is optional.
By default, the `index-uri` will be computed by concatenating the `index-id` with the
`default_index_root_uri` defined in the [Quickwit's config](quickwit-config).

<aside>
⚠️ The file storage will not work when running quickwit in distributed mode.
Today, only the s3 storage is available when running several searcher nodes.
</aside>

## Doc mapping

The doc mapping defines how a document and the fields it contains are stored and indexed for a given index. A document is a collection of named fields, each having its own data type (text, binary, date, i64, u64, f64).

```markdown
| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `field_mappings` | Collection of field mapping, each having its own data type (text, binary, date, i64, u64, f64).   | [] |
| `tag_fields` | Collection of fields already defined in `field_mappings` whose values will be stored in a dedicated `tags` (1) | [] |
| `store_source` | Whether or not the original JSON document is stored or not in the index.   | false |
```

(1) [Learn more on the tags usage](../design/querying.md).


### Field types

Each field has a type that indicates the kind of data it contains, such as integer on 64 bits or text.
Quickwit supports the following raw types `text`, `i64`, `u64`, `f64`, `date`, and `bytes`, and also supports composite types such as array and object. Behind the scenes, Quickwit is using tantivy field types, don't hesitate to look at [tantivy documentation](https://github.com/tantivy-search/tantivy) if you want to go into the details.

### Raw types

#### text type

This field is a text field that will be analyzed and split into tokens before indexing.
This kind of field is tailored for full-text search.

Example of a mapping for a text field:

```yaml
name: body
type: text
tokenizer: default
record: position
```

**Parameters for text field**

```markdown
| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `stored`    | Whether value is stored in the document store | `true` |
| `tokenizer` | Name of the `Tokenizer`, choices between `raw`, `default` and `stem_en` | `default` |
| `record`    | Describes the amount of information indexed, choices between `basic`, `freq` and `position` | `basic` |
```

**Description of available tokenizers**

```markdown
| Tokenizer     | Description   |
| ------------- | ------------- |
| `raw`         | Does not process nor tokenize text  |
| `default`     | Chops the text on according to whitespace and punctuation, removes tokens that are too long, and lowercases tokens |
| `stem_en`     |  Like `default`, but also applies stemming on the resulting tokens  |
```

**Description of record options**

```markdown
| Record option | Description   |
| ------------- | ------------- |
| `basic`       |  Records only the `DocId`s |
| `freq`        |  Records the document ids as well as the term frequency  |
| `position`    |  Records the document id, the term frequency and the positions of occurences.  |
```

Indexing with position is required to run phrase queries.

#### Numeric types: `i64`, `u64` and `f64` type

Quickwit handles three numeric types: `i64`, `u64`, and `f64`.

Numeric values can be stored in a fast field (the equivalent of Lucene's `DocValues`) which is a column-oriented storage.

Example of a mapping for an i64 field:

```yaml
name: timestamp
type: i64
stored: true
indexed: true
fast: true
```

**Parameters for i64, u64 and f64 field**

```markdown
| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`      | Whether value is stored in a fast field | `false` |
```

#### `date` type

The `date` type accepts one strict format `RFC 3339`.

Example of a mapping for a date field:

```yaml
name: timestamp
type: date
stored: true
indexed: true
fast: true
```

**Parameters for date field**

```markdown
| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`      | Whether value is stored in a fast field | `false` |
```

#### `bytes` type
The `bytes` type accepts a binary value as a `Base64` encoded string.

Example of a mapping for a bytes field:

```yaml
name: binary,
type: bytes,
stored: true,
indexed: true,
fast: true
```

**Parameters for bytes field**

```markdown
| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `stored`    | Whether value is stored in the document store | `true` |
| `indexed`   | Whether value is indexed | `true` |
| `fast`     | Whether value is stored in a fast field | `false` |
```

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

### Field name validation rules

Currently Quickwit only accepts field name that matches the following rules:

- do not start with character `-`
- matches regex `[_a-zA-Z][_\\.\\-a-zA-Z0-9]*$`

### Behavior with fields not defined in the config

Fields in your JSON document that are not defined in the `index config` will be ignored.

### Behavior with null values or missing fields

Fields with `null` or missing fields in your JSON document will be silently ignored when indexing.

## Indexing settings

This section describes indexing settings for a given index.

```markdown
| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `timestamp_field`      | Timestamp field used for sharding documents in splits (1).   | None |
| `commit_timeout_secs`      | Maximum number of seconds before committing a split since its creation.   | 60 |
| `split_num_docs_target`      | Maximum number of documents in a split. Note that this is not a hard limit.   | 10_000_000 |
| `merge_policy.merge_factor`      | Number of splits to merge.   | 10 |
| `merge_policy.max_merge_factor`      | Maximum number of splits to merge.   | 12 |
| `resources.num_threads`      | Number of threads per source.   | 1 |
| `resources.heap_size`      | Indexer heap size per source per index.   | 2_000_000_000 |
```

(1) [Learn more on time sharding]( ../design/architecture.md)


### Indexer memory usage

Indexer works with a default heap of 2 GiB of memory. This does not directly reflect the overall memory usage, but doubling this value should give a fair approximation.


## Search settings

This section describes search settings for a given index.

```markdown
| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| `search_default_fields`      | Default list of fields that will be used for search.   | None |
```

## Sources

An index can have one or several data sources. [Learn how to configure them](source-config.md).
