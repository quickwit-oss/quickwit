---
title: Query language
sidebar_position: 3
---

Quickwit uses a query mini-language which is used by providing a `query` parameter to the search endpoints.

### Terms

The `query` is parsed into a series of terms and operators. There are two types of terms: single terms such as “tantivy” and phrases which is a group of words surrounded by double quotes such as “hello world”.

Multiple terms can be combined together with Boolean operators `AND, OR` to form a more complex query. By default, terms will be combined with the `AND` operator.

### Fields

You can specify fields to search in the query by following the syntax `field_name:term`.

For example, let's assume an index that contains two fields, `title`, and `body` with `body` the default field. To search for the phrase “Barack Obama” in the title AND “president” in the body, you can enter:

```
title:"barack obama" AND president
```

Note that a query like `title:barack obama` will find only `barack` in the title and `obama` in the default fields. If no default field has been set on the index, this will result in an error.

### Boolean Operators

Quickwit supports `AND`, `+`, `OR`, `NOT` and `-` as Boolean operators (case sensitive). By default, the `AND` is chosen, this means that if you omit it in a query like `title:"barack obama" president` Quickwit will interpret the query as `title:"barack obama" AND president`.

### Grouping boolean operators

Quickwit supports parenthesis to group multiple clauses:

```
(color:red OR color:green) AND size:large
```

### Slop Operator

Quickwit also supports phrase queries with a slop parameter using the slop operator `~` followed by the value of the slop. For instance, the query `body:"small bike"~2` will match documents containing the word `small`, followed by one or two words immediately followed by the word `bike`.

:::caution
Slop queries can only be used on field indexed with the [record option](./../configuration/index-config.md#text-type) set to `position` value.
:::

#### Examples:

With the following corpus:
```json 
[
    {"id": 1, "body": "a red bike"},
    {"id": 2, "body": "a small blue bike"},
    {"id": 3, "body": "a small, rusty, and yellow bike"},
    {"id": 4, "body": "fred's small bike"},
    {"id": 5, "body": "a tiny shelter"}
]
```
The following queries will output: 

- `body:"small bird"~2`: no match []
- `body:"red bike"~2`: matches [1]
- `body:"small blue bike"~3`: matches [2]
- `body:"small bike"`: matches [4]
- `body:"small bike"~1`: matches [2, 4]
- `body:"small bike"~2`: matches [2, 4] 
- `body:"small bike"~3`: matches [2, 3, 4]

### Escaping Special Characters

Special reserved characters are: `+` , `^`, `` ` ``, `:`, `{`, `}`, `"`, `[`, `]`, `(`, `)`, `~`, `!`, `\\`, `*`, `SPACE`. Such characters can still appear in query terms, but they need to be escaped by an antislash `\` .
