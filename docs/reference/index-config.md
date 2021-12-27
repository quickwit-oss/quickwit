---
title: Index configuration
position: 3
---

WIP on Notion.

This page describes how to configure an index.

The index config lets you define four things:
- The document model version, only version `0` is available.
- The doc mapping, that is how a document and the fields it contains are stored and indexed for a given index.
- The indexing settings that defines the merge policy, timestamp field used for sharding, and more.
- The search settings that defines the default search fields `default_search_fields`, a list of field that will be used by default for search.
- The (data) sources that defines a list of source of types among Kafka or simple files.  

Configuration is set at index creation or can be modified later by using CLI commands or by directly changing settings in the metastore except for the doc mapping (or schema), which is today immutable.

## Config file format

The index configuration format is YAML. When a key is absent from the configuration file, the default value is used.
Here is a complete example suited for the hdfs logs dataset:

```YAML title="index-config.yaml"
version: 0

doc_mapping:
  field_mappings:
    - name: tenant_id
      type: u64
      fast: true
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
  tag_fields: [tenant_id]
  store_source: true

indexing_settings:
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]

```

## Doc mapping
The doc mapping defines how a document, and the fields it contains are stored and indexed for a given index. A document is a collection of named fields, each having its own data type (text, binary, date, i64, u64, f64).


| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| field_mappings | Collection of field mapping, each having its own data type (text, binary, date, i64, f64). | [] |
| tag_fields | Tag fields. | [] |
| store_source | Whether or not the original JSON document is stored or not in the index. | false |


### Field types
Each field has a type that indicates the kind of data it contains, such as integer on 64 bits or text.
Quickwit supports the following raw types `text`, `i64`, `u64`, `f64`, `date`, and `bytes` and also supports composite types such as array and object. Behind the scenes, Quickwit is using tantivy field types, don't hesitate to look at [tantivy documentation](https://github.com/tantivy-search/tantivy) if you want to go into the details.


### Raw types
#### `text` type

This field is a text field that will be analyzed and split into tokens before indexing.
This kind of field is tailored for full text search.

Example of a mapping for a text field:
```YAML
name: body
type: text
tokenizer: default
record: position
```

**Parameters for text field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| **stored**    | Whether value is stored in the document store | `true` |
| **tokenizer** | Name of the `Tokenizer`, choices between `raw`, `default` and `stem_en` | `default` |
| **record**    | Describes the amount of information indexed, choices between `basic`, `freq` and `position` | `basic` |

**Description of available tokenizers**

| Tokenizer     | Description   |
| ------------- | ------------- |
| `raw`         | Does not process nor tokenize text  |
| `default`     | Chops the text on according to whitespace and punctuation, removes tokens that are too long, and lowercases tokens |
| `stem_en`     |  Like `default`, but also applies stemming on the resulting tokens  |

**Description of record options**

| Record option | Description   |
| ------------- | ------------- |
| `basic`       |  Records only the `DocId`s |
| `freq`        |  Records the document ids as well as the term frequency  |
| `position`    |  Records the document id, the term frequency and the positions of occurences  |


#### Numeric types: `i64`, `u64` and `f64` type
Quickwit handles three numeric types: `i64`, `u64` and `f64`.
Numeric values can be stored in a fast field (equivalent of `Lucene`'s `DocValues`) which is a column-oriented storage.

Example of a mapping for a i64 field:
```json
{
    "name": "timestamp",
    "type": "i64",
    "stored": true,
    "indexed": true,
    "fast": true
}
```

**Parameters for i64, u64 and f64 field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| **stored**    | Whether value is stored in the document store | `true` |
| **indexed**   | Whether value is indexed | `true` |
| **fast**      | Whether value is stored in a fast field | `false` |


#### `date` type
The `date` type accepts one strict format `RFC 3339`.

Example of a mapping for a i64 field:
```json
{
    "name": "start_date",
    "type": "date",
    "stored": true,
    "indexed": true,
    "fast": true
}
```

**Parameters for date field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| **stored**    | Whether value is stored in the document store | `true` |
| **indexed**   | Whether value is indexed | `true` |
| **fast**      | Whether value is stored in a fast field | `false` |


#### `bytes` type
The `bytes` type accepts a binary value as a `Base64` encoded string.

Example of a mapping for a i64 field:
```json
{
    "name": "binary",
    "type": "bytes",
    "stored": true,
    "indexed": true,
    "fast": true
}
```

**Parameters for bytes field**

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| **stored**    | Whether value is stored in the document store | `true` |
| **indexed**   | Whether value is indexed | `true` |
| **fast**      | Whether value is stored in a fast field | `false` |

### Composite types
#### `array`
Quickwit supports array for all raw types but not for `object` type.
To declare an array type of `i64` in the `index config`, you just have to set the type to `array<i64>`.

#### `object`
Quickwit supports nested object as long as it does not contain arrays of object.

```json
{
    "name": "resource",
    "type": "object",
    "field_mappings": [
        {
            "name": "service",
            "type": "text"
        }
    ]
}
```

### Field name validation rules
Currently Quickwit only accepts field name that matches the following rules:
- do not start with character `-`
- matches regex `[_a-zA-Z][_\.\-a-zA-Z0-9]*$`


### Behaviour with fields are not defined in the config
Fields in your json document that are not defined in the `index config` will be ignored.


### Behaviour with null values or missing fields
Fields with `Null` or missing field in your json document will be silently ignored when indexing.



## Indexing settings

This section describes indexing settings.

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| timestamp_field    | timestamp field `timestamp_field` used for [sharding documents in splits](../overview/architecture.md#splits). | None |
| commit_timeout_secs | Maximum number of seconds before committing a split since its creation. | 60 |
| split_num_docs_target | Maximum number of documents in a split. Please note that this is not a hard limit. | false |
| merge_enabled | Merging splits is enabled. | true |
| merge_policy.merge_factor | Number of splits to merge. | 10 |
| merge_policy.max_merge_factor | Maximum number of splits to merge. | 12 |
| resources.num_threads | Number of threads per source. | 1 |
| resources.heap_size | Heap size per source | 2_000_000_000 (2GB) |


## Search settings

| Variable      | Description   | Default value |
| ------------- | ------------- | ------------- |
| search_default_fields | Default list of fields that will be used for search. | None |


## Sources

One index can define a list of sources, go to [sources] page to learn how to configure them.
