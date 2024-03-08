---
title: Query language
sidebar_position: 40
---

Quickwit uses a query mini-language which is used by providing a `query` parameter to the search endpoints.
<!-- todo also used in some place in ES: where? -->>

<!-- that first part is more an introduction than a reference.I think it is useful to get started, but it should probably be moved elsewhere -->
### Simple queries

Quickwit queries are composed of basic clauses, with which it filter out documents. By default, it returns only documents matching the intersection (logical AND) of the provided clauses.
For instance, `log_level:WARN body:DNS` will match only documents which have a field `log_level` containing `WARN`, and a field `body` containing `DNS`. You can also make the AND explicit:
`log_level:WARN AND body:DNS`. If instead you want to get documents matching one of two clauses, you can use the `OR` keyword: `log_level:WARN OR log_level:ERROR`.

If you configured default fiels on your index, you can omit the field name, and directly search for the value you want. For instance, if body is a default field, you could write
just `DNS` to get all documents where body contains `DNS`, or `log_level:WARN DNS` to get the same result as `log_level:WARN AND body:DNS`.

### Grouping and negating

It is possible to group clauses to make more complexe expressions. For instance, if you want documents where log\_level is either `WARN` or `ERROR`, and the body contains `DNS`, you could
do `(log_level:WARN OR log_level:ERROR) AND body:DNS`. You can go arbitrarily deep to make more complexe expression.

You can also negate a clause, as to get documents which did not match it. To do this, either use the NOT operator, or prefix the close with a `-`. For instance
`log_level:ERROR AND NOT body:DNS` or `log_level:ERROR AND -body:DNS` returns documents where log\_level is `ERROR` and body doesn't contain `DNS`.

### More clauses

Up to now, we have seen only seen queries searching for specific keywords. There are other basic clauses you can use in your queries:
- term query: we already used, matches a precise token.
- prefix query: matches if the field contains a token which starts with the value provided. E.g. `body:tant*` will match documents containing 'tantivy' or 'tantal', but not 'quickwit'
- phrase query: matches if the field contains the provided tokens, and they follow each other. E.g. `body:"blue boat"` will match a document containing "blue boat and white car", but not "white boat and blue car".
- phrase prefix query: this is a mix of a prefix query and a phrase prefix query. E.g. `body:"blue boa"*`.
- range query: matches only if the field contains a value in the provided range. E.g. `year:[2020 TO 2022]` will match only if year is 2020, 2021 or 2022. See the [[reference]] for other usage of range query.
- exist query: matches only if the field exist in the document. `body:*`
- term set query: matches if the field contains one of the provided values. It can be more efficient than ORing many term queries. E.g. `id: IN [5 7 8 12 13 15 17 21]` will match a document where id is any of those
values. When there is only a handful of terms to match, ORing multing term query is more efficient.

<!-- we don't talk about slop and boost here, trinity thinks they are better suited for a full reference than a short introduction -->

## Reference

Queries are built of basic blocks called clauses. They target a specific field, and take the form `field:<something>`. The field is only used for the query directly adjacent. In `field:<something> <somethingelse>`, only '\<something\>' will be using that field.

In many case it is possible to omit the field you search if it was configured as a `default_search_fields`. Not all kind of query support this, but they do unless noted otherwise in their description below. These clauses can be used together by using operators such as 'AND' or 'OR'.

Note that the result of a query can depend on the tokenizer used for the field getting searched. Hence this document always speaks of tokens, which may be the exact value the document contain (in case of the raw tokenizer), or a subset of it (for instance any tokenizer cutting on spaces).

### Term Query

The simplest kind of query, it matches the value that's given. `field:value` will match any document where the field 'field' has a token 'value'.

### Prefix Query

A query which matches if the field of a document contains a token which starts with the provided value. `field:valu*` will match any document where the field 'field' has a token 'value', but also a token 'valuf', but not 'val' or 'abcd'.

### Phrase Query

A query which matches if the field contains the sequence of token provided. `field:"looks good to me"` will match any document containing that sequence of tokens.

Is is also possible to add a slop, which allow matching a sequence with some distance. For instance `"looks to me"~1` will match "looks good to me", but not "looks very good to me".
Transposition costs 2, e.g. `"A B"~1` will not match `"B A"` but it would with `"A B"~2`.
Transposition is not a special case, in the example above A is moved 1 position and B is moved 1 position, so the slop is 2.

For phrase query, the field must have been configured with `record: position` when indexing.

### Phrase Prefix Query

A query which match if the field contains the sequence of token provided, where the last token in the query may be only a prefix of the token in the document. `field:"thanks for your contrib"*` will match 'thanks for your contribution'.

There is no slop for phrase prefix queries.
Like with phrase query, the field must have been configured with `record: position` when indexing.

Caveat: this query may not match some document it should in some cases. If you search for `"thanks for your co"*`. Quickwit
will first enumerate the first 50 tokens which start with "co", and search for any documents where "thanks for you" is followed
by any of these tokens. If there are many tokens starting with "co", "contribution" might not be one of the 50 selected tokens,
and the query won't match a document containing "thanks for your contribution". Normal prefix queries don't suffer from this issue.

### Exist Query

A query which match if the document contain some value for that field. `field:*` will match any document where 'field' is set to something, but no documents where 'field' is unset.

You have to specify a field for this query. Quickwit won't use `default_search_fields` automatically. Using `*` alone will create a match-all query, which match every single document.

### Match all Query

A query which match every single document. You can't put a field in front. It is simply written as `*`.

### Range Query

A query which matches if the document contains a token between the provided bound for that field.
`field:[abc TO zz]` will match a document where field is 'abc', 'def', or 'z', but not 'aab' or 'zzz'.
Instead of square bracket, you can use curly brackets `{}` to make a range that's exclusive, on one side or on both.
`field:{abc TO zz]` matches the same as the previous query, except for 'abc' which is excluded.
For text fields, the ranges are defined by lexicographic order. It means for a text field, 100 is between 1 and 2.
When using ranges on integers, it behaves naturally.

You can make an half open range by using `*` as one of the bounds. `field:[b TO *]` will match 'bb' and 'zz', but not 'ab'.
You can also use a comparhison based syntax:`field:<b`, `field:>b`, `field:<=b` or `field:>=b`.

For range queries, you must provide a field. Quickwit won't use `default_search_fuelds` automatically.

### Term Set Query

A query which matches if the document contains any of the tokens provided. `field: IN [ab cd]` will match 'ab' or 'cd', but nothing else.

This is a lot like writting `field:ab OR field:cd`. When there are only a handful of terms to search for, using ORs is usually faster. When there are many values to match, a term set query can become more efficient.

<!-- previously a field was required. It looks like it may no longer be the case -->

## Other concepts

### Operators

Most queries are composed of more than one clause. When doing so, you may add operators between clauses. Implicitly if no operator is provided, 'AND' is assumed.

The supported operators are 'AND', which matches only if both side match, and 'OR' which matches if any of the side match.
`field1:val1 AND field2:val2` (or equivalently `field1:val1 field2:val2`) matches only a document field1 contains val1 AND field2 contains val2.
`field1:val1 OR field2:val2` matches if either (or both) of the condition are met.

It is also possible to negate an expression, either with the operator 'NOT' or by prefixing the query with a dash '-': `NOT field:val` or `-field:val`.

For priorities, 'AND' takes precedence over 'OR'. That is, `a AND b OR c` is interpreted as `(a AND b) or c`. 'NOT' and '-' takes precedence over everything, such that `-a AND b` means `(-a) AND b`, not `-(a AND B)`.

### Grouping

It is possible to group clauses to make more complexe expression, using parenthesis. For instance, if a query should match if 'field1' is 'one' or 'two', and 'field2' is 'three', you can use `(field1:one OR field1:two) AND field2:three`.

### Searching inside structures

When using `object` or `json` fields, or on a field caught through the dynamic mode, you can query your documents in the same way.
The field you provide will have to contains dots between each path part. For instance, the document
`{"product": {"attributes": {color": "red"}}}` is matched by `product.attributes.color:red`

If a dot `.` exists in one of the key of your object, the above syntax has some ambiguity.
For instance, by default, `{"k8s.component.name": "quickwit"}` will be matched by `k8s.component.name:quickwit`.

It is possible to remove the ambiguity by setting `expand_dots` in the json field configuration.
In that case, it will be necessary to escape the `.` in the query to match this document.

For instance, the above document will match the query `k8s\.component\.name:quickwit`.

### Escaping Special Characters

Special reserved characters are: `+` , `^`, `` ` ``, `:`, `{`, `}`, `"`, `[`, `]`, `(`, `)`, `~`, `!`, `\\`, `*`, `SPACE`. Such characters can still appear in query terms, but they need to be escaped by an antislash `\` .

### Searching on ip field and datetime

When searching for IP addesses, they can be provided as IPv4 or IPv6. It is recommended to search with the format used
when indexing documents. There is no support for searching for a range of IP using CIDR notation, but you can use normal
range queries.

When searching a datetime field, you must provide values in rfc3339 format, such as `1970-01-01T00:00:00Z`

## Examples:

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
- `body: IN [small tiny]`: matches [2, 3, 4, 5]

