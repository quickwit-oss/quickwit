---
title: Index configuration
position: 2
---

The index config let you define four things:
- the doc mapping, that is how a document, and the fields it contains are stored and indexed for a given index. A document is a collection of named fields, each having its own data type (text, binary, date, i64, f64)
- the timestamp field `timestamp_field` used for [sharding documents in splits](../overview/architecture.md#the-splits). This is very useful when querying as Quickwit will be able to prune splits based on time range and make search way faster. The timestamp field must be an `i64`. When you define a timestamp field, note that documents will be ordered by default in descending order related to this field 
- the default search fields `default_search_fields`: if no field name is specified in your query, these fields will be used for search
- whether or not the original JSON document is stored or not in the index by setting `store_source` to true or false.

This config can be expressed as a json file given to the `new` Quickwit command. Here is a example of a json config for a logging dataset:

```json title="hdfslogs_index_config.json"
{
    "store_source": true,
    "default_search_fields": ["body", "severity_text"],
    "timestamp_field": "timestamp", // by default, Quickwit will sort documents on this field by a descending order.
    "field_mappings": [
        {
            "name": "timestamp",
            "type": "i64", // i64 is mandatory for a timestamp field.
            "fast": true // fast field must be true for a timestamp field.
        },
        {
            "name": "severity_text",
            "type": "text"
        },
        {
            "name": "body",
            "type": "text"
        },
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
    ]
}
```


## Field types
Each field has a type which indicates the kind of data it contains such as integer on 64 bits or text.
Quickwit supports the following raw types `text`, `i64`, `f64`, `date` and `bytes` and also supports composite types such as array and object. Behind the scenes, Quickwit is using tantivy field types, don't hesitate to have a look at [tantivy documentation](https://github.com/tantivy-search/tantivy) if you want to go into the details.


### Raw types
#### `text` type

This field is a text field that will be analyzed and split into tokens before indexing. 
This kind of field is tailored for full text search.

Example of a mapping for a text field:
```json
{
    "name": "body",
    "type": "text",
    "tokenizer": "default",
    "record": "position"
}
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


#### Numeric types: `i64` and `f64` type
Quickwit handles two numeric types: `i64` and `f64`. 
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

**Parameters for i64 and f64 field**

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
Quickwit supports nested object as long as it does not contains arrays of object.

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

## Field name validation rules
Currently Quickwit only accepts field name that matches the following rules:
- do not start with character `-`
- matches regex `[_a-zA-Z][_\.\-a-zA-Z0-9]*$`


## Behaviour with fields are not defined in the config
Fields in your json document that are not defined in the `index config` will be ignored.


## Behaviour with null values or missing fields
Fields with `Null` or missing field in your json document will be silently ignored when indexing.
