---
title: Query language
position: 7
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

Quickwit supports parenthesis to group multiple clauses with or without specifying a field:

```
title:(obama OR president)

(obama OR president)
```

### Escaping Special Characters

Special reserved characters are: `+` , `^`, ```, `:`, `{`, `}`, `"`, `[`, `]`, `(`, `)`, `~`, `!`, `\\`, `*`, `SPACE`. Such characters can still appear in query terms, but they need to be escaped by an antislash `\` .
