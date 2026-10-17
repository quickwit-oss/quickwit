---
title: Introduction to Quickwit's query language
sidebar_position: 3
---

Quickwit allows you to search on your indexed documents using a simple query language. Here's a quick overview.

## Clauses

The main concept of this language is a clause, which represents a simple condition that can be tested against documents. 

### Querying fields

A clause operates on fields of your document. It has the following syntax :
```
field:condition
```

For example, when searching documents where the field `app_name` contains the token `tantivy`, you would write the following clause:
```
app_name:tantivy
```

In many cases the field name can be omitted, quickwit will then use the `default_search_fields` configured for the index.

### Clauses Cheat Sheet

Quickwit support various types of clauses to express different kinds of conditions. Here's a quick overview of them:

| type | syntax | examples | description| `default_search_field`|
|-------------|--------|----------|------------|-----------------------|
| term | `field:token` | `app_name:tantivy` <br/> `process_id:1234` <br/> `word` | A term clause tests the existence of avalue in the field's tokens | yes |
| term prefix | `field:prefix*` | `app_name:tant*` <br/> `quick*` | A term clause tests the existence of a token starting with the provided value | yes |
| term set | `field:IN [token token ..]` |`severity:IN [error warn]` | A term set clause tests the existence of any of the provided value in the field's tokens| yes |
| phrase | `field:"sequence of tokens"` | `full_name:"john doe"` | A phrase clause tests the existence of the provided sequence of tokens | yes |
| phrase prefix | `field:"sequence of tokens"*` | `title:"how to m"*` | A phrase prefix clause tests the existence of a sequence of tokens, the last one used like in a prefix clause | yes |
| all | `*` | `*` | A match-all clause will match every document | no |
| exist | `field:*` | `error:*` | An exist clause tests the existence of any value for the field, it will match only if the field exists | no |
| range | `field:bounds` |`duration:[0 TO 1000]` <br/> `last_name:[banner TO miller]` | A term clause tests the existence of a token between the provided bounds | no |

## Queries

### Combining queries

Clauses can be combined using boolean operators `AND` and  `OR` to create more complex search expressions
An `AND` query will match only if conditions on both sides of the operator are met
```
type:rose AND color:red
```

An `OR` query will match if either or both conditions on each side of the operator are met
```
weekday:6 OR weekday:7
```

If no operator is provided, `AND` is implicitly assumed.

```
type:violet color:blue
```

### Grouping queries
You can build complex expressions by grouping clauses using parentheses.
```
(type:rose AND color:red) OR (type:violet AND color:blue)
```

When no parentheses are used, `AND` takes precedence over `OR`, meaning that the following query is equivalent to the one above.

```
type:rose AND color:red OR type:violet AND color:blue
```

### Negating queries

An expression can be negated either with the operator `NOT` or by prefixing the query with a dash `-`.

`NOT` and `-` take precedence over everything, such that `-a AND b` means `(-a) AND b`, not `-(a AND B)`.

```
NOT severity:debug
```

or

```
type:proposal -(status:rejected OR status:pending)
```


## Dive deeper

If you want to know more about the query language, head to the [Query Language Reference](../reference/query-language.md)
