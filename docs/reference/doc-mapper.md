---
title: Doc mapper
position: 3
---

## What is it?
The `doc mapper` is in charge of defining how a document, and the fields it contains, are stored and indexed.
A document is a collection of named fields, each having its own data type (text, binary, date, i64, f64).

The doc mapper also defines 3 more informations:
- the `i64` timestamp field used to shard documents into splits. This is very useful when querying as Quickwit will be able to prune splits based on time range and make search way faster. If your documents have a timestamp field, set it as the timestamp field (`timestamp_field`)
- the default search fields: when querying, these fields will be used to search documents if no field name is specified in the query (`default_search_fields`)
- if the original JSON document is stored or not in the index (`store_source`)

The `doc mapper` is defined by a json file given to Quickwit CLI at the creation of the index. Here is a example of a doc mapper for a logging dataset: 
```
{
    "store_source": true,
    "default_search_fields": ["body", "severity_text"],
    "timestamp_field": "timestamp",
    "field_mappings": [
        {
            "name": "timestamp",
            "type": "i64"
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

Each field has a type which indicates the kind of data it contains such as integer on 64bits.
Quickwit supports the following raw types `text`, `i64`, `f64`, `date` and `bytes` and also supports composite
types such as array and object.

### Raw types

#### `text` type

```
{
    "name": "body",
    "type": "text",
    "tokenizer": "english"
}
```

#### `i64` type

```
{
    "name": "timestamp",
    "type": "i64",
    "stored": boolean,
    "fast": boolean
}
```

#### `f64` type

#### `date` type

#### `bytes` type


### Composite types

#### `array`

#### `object`
