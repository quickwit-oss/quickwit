---
title: Query Language Reference
sidebar_position: 40
---

## Pseudo-grammar

```
query = '(' query ')'
      | query operator query
      | unary_operator query
      | query query
      | clause

operator = 'AND' | 'OR'

unary_operator = 'NOT' | '-'

clause = field_name ':' field_clause
       | defaultable_clause
       | '*'

field_clause = term | term_prefix | term_set | phrase | phrase_prefix | range | '*'
defaultable_clause = term | term_prefix | term_set | phrase | phrase_prefix
```
---
## Writing Queries
### Escaping Special Characters

Some characters need to be escaped in non quoted terms because they are syntactically significant otherwise: special reserved characters are: `+` , `^`, `` ` ``, `:`, `{`, `}`, `"`, `[`, `]`, `(`, `)`, `~`, `!`, `\\`, `*`, `SPACE`. If such such characters appear in query terms, they need to be escaped by prefixing them with an anti-slash `\`.

In quoted terms, the quote character in use `'` or `"` needs to be escaped.

###### Allowed characters in field names

See the [Field name validation rules](https://quickwit.io/docs/configuration/index-config#field-name-validation-rules) in the index config documentation.

### Addressing nested structures

Data stored deep inside nested data structures like `object` or `json` fields can be addressed using dots as separators in the field name.
For instance, the document `{"product": {"attributes": {color": "red"}}}` is matched by
```
product.attributes.color:red
```

If the keys of your object contain dots, the above syntax has some ambiguity : by default `{"k8s.component.name": "quickwit"}` will be matched by 
```k8s.component.name:quickwit```

It is possible to remove the ambiguity by setting expand_dots in the json field configuration. 
In that case, it will be necessary to escape the `.` in the query to match this document like this :
```
k8s\.component\.name:quickwit
```

---

## Structured data
### Datetime
Datetime values must be provided in rfc3339 format, such as `1970-01-01T00:00:00Z`

### IP addresses
IP addresses can be provided as IPv4 or IPv6. It is recommended to search with the format used when indexing documents.
There is no support for searching for a range of IP using CIDR notation, but you can use normal range queries.

---

## Types of clauses

### Term `field:term`
```
term = term_char+
```

Matches documents if the targeted field contains a token equal to the provided term. 

`field:value` will match any document where the field 'field' has a token 'value'.

### Wildcard `field:wil?car*d`
```
wildcard = [term_char\*\?]+
```

Matches documents if the targeted field contains a token that matches the wildcard:
- `?` replaces one and only one term character
- `*` replaces any number of term characters or an empty string

Examples:
- `field:quick*` will match any document where the field 'field' has a token like `quickwit` or `quickstart`, but not `qui` or `abcd`.
- `field:h?llo` will match any document where the field 'field' has a token like `hello` or `hallo`, but not `heillo` or `hllo`.

Queries with prefixes (`field:qui*`) are much more efficient than queries starting with a wildcard (`field:*wit`)


### Term set `field:IN [a b c]`
```
term_set = 'IN' '[' term_list ']'
term_list = term_list term
          | term
```
Matches if the document contains any of the tokens provided. 

###### Examples
`field:IN [ab cd]` will match 'ab' or 'cd', but nothing else.

###### Performance Note
This is a lot like writing `field:ab OR field:cd`. When there are only a handful of terms to search for, using ORs is usually faster.
When there are many values to match, a term set query can become more efficient.

<!-- previously a field was required. It looks like it may no longer be the case -->

### Phrase `field:"sequence of words"`
```
phrase = phrase_string
       | phrase_string slop
phrase_string = '"' phrase_char '"'
slop = '~' [01-9]+

```

Matches if the field contains the sequence of token provided:
- `field:"looks good to me"` will match any document containing that sequence of tokens.
- `field:"look* good to me"` with the default tokenizer is equivalent to `field:"look good to me"`, i.e. the '*' character is pruned by the tokenizer and not interpreted as a wildcard.

:::info

The field must have been configured with `record: position` when indexing.

:::

###### Slop operator
Is is also possible to add a slop, which allow matching a sequence with some distance. For instance `"looks to me"~1` will match "looks good to me", but not "looks very good to me".
Transposition costs 2, e.g. `"A B"~1` will not match `"B A"` but it would with `"A B"~2`.
Transposition is not a special case, in the example above A is moved 1 position and B is moved 1 position, so the slop is 2.

### Phrase Prefix `field:"finish this phr"*`
```
phrase_prefix = phrase '*'
```

Matches if the field contains the sequence of token provided, where the last token in the query may be only a prefix of the token in the document.

The field must have been configured with `record: position` when indexing.

There is no slop for phrase prefix queries.

###### Examples
 `field:"thanks for your contrib"*` will match 'thanks for your contribution'.

###### Limitation

Quickwit may trim some results matched by this clause in some cases.  If you search for `"thanks for your co"*`, it will enumerate the first 50 tokens which start with "co" (in their storage order), and search for any documents where "thanks for your" is followed by any of these tokens.

If there are many tokens starting with "co", "contribution" might not be one of the 50 selected tokens, and the query won't match a document containing "thanks for your contribution". Normal prefix queries don't suffer from this issue.

### Range `field:[low_bound TO high_bound}`
```
range = explicit_range | comparison_half_range

explicit_range = left_bound_char bounds right_bound_char
left_bound_char = '[' | '{' 
right_bound_char = '}' | ']'
bounds = term TO term
       | term TO '*'
       | '*' TO term

comparison_range = comparison_operator term
comparison_operator = '<' | '>' | '<=' | '>='
```

Matches if the document contains a token between the provided bounds for that field.
For range queries, you must provide a field. Quickwit won't use `default_search_fields` automatically.

###### Order
For text fields, the ranges are defined by lexicographic order on uft-8 encoded byte arrays. It means for a text field, 100 is between 1 and 2.
<!-- TODO: Build a more comprehensive example set to showcase how wharacters are sorted -->

When using ranges on integers, it behaves naturally.

###### Inclusive and exclusive bounds
Inclusive bounds are represented by square brackets `[]`. They will match tokens equal to the bound term.
Exclusive bounds are represented by curly brackets `{}`. They will not match tokens equal to the bound term.

###### Half-Open bounds
You can make an half open range by using `*` as one of the bounds. `field:[b TO *]` will match 'bb' and 'zz', but not 'ab'.
You can also use a comparison based syntax:`field:<b`, `field:>b`, `field:<=b` or `field:>=b`.

<!-- NOTE : empty values likely not indexed -->

###### Examples
- Inclusive Range: `ip:[127.0.0.1 TO 127.0.0.50]`
- Exclusive Range: `ip:{127.0.0.1 TO 127.0.0.50}`
- Unbounded Inclusive Range: `ip:[127.0.0.1 TO *] or ip:>=127.0.0.1`
- Unbounded Exclusive Range: `ip:{127.0.0.1 TO *] or ip:>127.0.0.1`


### Exists `field:*`

Matches documents where the field is set. You have to specify a field for this query, Quickwit won't use `default_search_fields` automatically.

### Match All `*`

Matches every document. You can't put a field in front. It is simply written as `*`.

---

## Building Queries
Most queries are composed of more than one clause. When doing so, you may add operators between clauses.

Implicitly if no operator is provided, 'AND' is assumed.

### Conjunction `AND`
An `AND` query will match only if both sides match.

<!-- TODO: Formal example ?*-->

### Disjunction `OR`
An `OR` query will match if either (or both) sides match.

<!-- TODO: Formal example ?*-->

### Negation `NOT` or `-`
A `NOT` query will match if the clause it is applied to does not match.
The `-` prefix is equivalent to the `NOT` operator.

### Grouping `()`
Parentheses are used to force the order of evaluation of operators.
For instance, if a query should match if 'field1' is 'one' or 'two', and 'field2' is 'three', you can use `(field1:one OR field1:two) AND field2:three`.

### Operator Precedence
Without parentheses, `AND` takes precedence over `OR`. That is, `a AND b OR c` is interpreted as `(a AND b) or c`.

`NOT` and `-` takes precedence over everything, such that `-a AND b` means `(-a) AND b`, not `-(a AND B)`.


---

## Other considerations 

### Default Search Fields
In many case it is possible to omit the field you search if it was configured in the `default_search_fields` array of the index configuration. If more than one field is configured as default, the resulting implicit clauses are combined using a conjunction ('OR').

### Tokenization
Note that the result of a query can depend on the tokenizer used for the field getting searched. Hence this document always speaks of tokens, which may be the exact value the document contain (in case of the raw tokenizer), or a subset of it (for instance any tokenizer cutting on spaces).

<!-- NOTE : should dig deeper ? -->
