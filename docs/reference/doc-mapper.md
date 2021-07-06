---
title: Doc mapper
position: 3
---

## What is it?

The `doc mapper` is in charge of defining how a document, and the fields it contains, are stored and indexed for a given index.
A document is a collection of named fields, each having its own data type (text, binary, date, i64, f64).

Besides that, the doc mapper let you define 3 more information:
- the timestamp field `timestamp_field` used for [sharding documents in splits](link). This is very useful when querying as Quickwit will be able to prune splits based on time range and make search way faster. The timestamp field must be a `i64`
- the default search fields `default_search_fields`: if no field name is specified in your query, these fields will be used for search
- whether or not the original JSON document is stored or not in the index by setting `store_source` to true or false.


The `doc mapper` is defined by a json file given to Quickwit CLI at the creation of the index. Here is a example of a doc mapper for a logging dataset: 

```
{
    "store_source": true,
    "default_search_fields": ["body", "severity_text"],
    "timestamp_field": "timestamp",
    "field_mappings": [
        {
            "name": "timestamp",
            "type": "i64",
            "fast": true
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
Quickwit supports the following raw types `text`, `i64`, `f64`, `date` and `bytes` and also supports composite types such as array and object.

### Raw types

#### `text` type

This field is a text field that will be analyzed and split into tokens before indexing. 
This kind of field is tailored for full text search.

Example of a mapping for a text field:
```
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
| **record**    | Describes the amount information indexed, choices between `basic`, `freq` and `position` | `basic` |

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
```
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
```
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
```
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
To declare an array type of `i64` in the `doc mapper`, you just have to set the type `array<i64>`.

#### `object`

Quickwit supports nested object as long as it does not contains arrays of object.

```
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


## Behaviour with fields not defined in the mapper
Fields in your json document that are not defined in the `doc mapper` will be ignored.


## Behaviour with null values or missing fields
Fields with `Null` or missing field in your json document will be silently ignored when indexing.