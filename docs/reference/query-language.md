---
title: Query language
position: 4
---


The query language is used by the `search` CLI command and by the `query` parameter in the REST API search request. 
It supports currently a very simple simple syntax:
- simple terms: `Barack Obama` will match documents containing both "barack" and "obama" terms
- multiple terms: you can chain term with an `OR` to match document containing at least one of the terms
- negative terms: a term can be excluded by prepending it with a `-`
- must terms: a term can be made required by prepending it with a `+`.

## Examples with field names

You can specify fields to search in the query:
- `title:barack`
- `title:(barack OR obama)`
- `title:barack resource.body:barack` will search for `barack` in both fields
- `title:"barack obama"` will search for the exact phrase 

You can also omit field names to search into default search fields defined in the `index config`:
- `barack OR obama` will search for `barack` or `obama` in the default search fields.

## Example of a search query on the REST API

```
GET /api/v1/indexes/<index name>/search?query=barack OR obama
```

