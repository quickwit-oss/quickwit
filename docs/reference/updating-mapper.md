# Updating the doc mapping of an index

Quickwit allows updating the mapping it uses to add more fields to an existing index or change how they are indexed. In doing so, it does not reindex existing data but still lets you search through older documents where possible.

## Indexing

When you update a doc mapping for an index, Quickwit will restart indexing pipelines to take the changes into account. As both this operation and the document ingestion are asynchronous, there is no strict happens-before relationship between ingestion and update. This means a document ingested just before the update may be indexed according to the newer doc mapper, and document ingested just after the update may be indexed with the older doc mapper.

:::warning

If you use the ingest or ES bulk API (V2), the old doc mapping will still be used to validate new documents that end up being persisted on existing shards (see [#5738](https://github.com/quickwit-oss/quickwit/issues/5738)).

:::

## Querying

Quickwit always validate queries against the most recent mapping.
If a query was valid under a previous mapping but is not compatible with the newer mapping, that query will be rejected.
For instance if a field which was indexed no longer is, any query that uses it will become invalid.
On the other hand, if a query was not valid for a previous doc mapping, but is valid under the new doc mapping, Quickwit will process the query.
When querying newer splits, it will behave normally, when querying older splits, it will try to execute the query as correctly as possible.
If you find yourself in a situation where older splits causes a valid request to return an error, please open a bug report.
See examples 1 and 2 below for clarification.

Change in tokenizer affect only newer splits, older splits keep using the tokenizers they were created with.

Document retrieved are mapped from Quickwit internal format to JSON based on the latest doc mapping. This means if fields are deleted,
they will stop appearing (see also Reversibility below) unless mapper mode is Dynamic. If the type of some field changed, it will be converted on a best-effort basis:
integers will get turned into text, text will get turned into string when it is possible, otherwise, the field is omited.
See example 3 for clarification.

## Reversibility

Quickwit does not modify existing data when receiving a new doc mapping. If you realize that you updated the mapping in a wrong way, you can re-update your index using the previous mapping. Documents indexed while the mapping was wrong will be impacted, but any document that was committed before the change will be queryable as if nothing happened.

## Type update reference

Conversion from a type to itself is omitted. Conversions that never succeed and always omit the field are omitted, too.

<!-- this is extracted from `quickwit_doc_mapper::::default_doc_mapper::value_to_json()` -->
| type before | type after | behavior |
|-------------|------------|
| u64/i64/f64 | text | convert to decimal string |
| date | text | convert to rfc3339 textual representation |
| ip | text | convert to IPv6 representation. For IPv4, convert to IPv4-mapped IPv6 address (`::ffff:1.2.3.4`) |
| bool | text | convert to "true" or false" |
| u64/i64/f64 | bool | convert 0/0.0 to false and 1/1.0 to true, otherwise omit |
| text | bool | convert if "true" or "false" (lowercase), otherwise omit |
| text | ip | convert if valid IPv4 or IPv6, otherwise omit |
| text | f64 | convert if valid floating point number, otherwise omit |
| u64/i64 | f64 | convert, possibly with loss of precision |
| bool | f64 | convert to 0.0 for false, and 1.0 for true |
| text | u64 | convert is valid integer in range 0..2\*\*64, otherwise omit |
| i64 | u64 | convert if in range 0..2\*\*63, otherwise omit |
| f64 | u64 | convert if in range 0..2\*\*64, possibly with loss of precision, otherwise omit |
| text | i64 | convert is valid integer in range -2\*\*63..2\*\*63, otherwise omit |
| u64 | i64 | convert if in range 0..2\*\*63, otherwise omit |
| f64 | i64 | convert if in range -2\*\*63..2\*\*63, possibly with loss of precision, otherwise omit |
| bool | i64 | convert to 0 for false, and 1 for true |
| text | datetime | parse according to current input\_format, otherwise omit |
| u64 | datetime | parse according to current input\_format, otherwise omit |
| i64 | datetime | parse according to current input\_format, otherwise omit |
| f64 | datetime | parse according to current input\_format, otherwise omit |
| array\<T\> | array\<U\> | convert individual elements, skipping over those which can't be converted |
| T | array\<U\> | convert element, emiting array of a single element, or empty array if it can't be converted |
| array\<T\> | U | convert individual elements, keeping the first which can be converted |
| json | object | try convert individual elements if they exists inside object, omit individual elements which can't be |
| object | json | convert individual elements. Previous lists of one element are converted to a single element not in an array.

## Examples

In the below examples, fields which are not relevant are removed for conciseness, you will not be able to use these index config as is.

### Example 1

before:
```yaml
doc_mapping:
  field_mappings:
    - name: field1
      type: text
      tokenizer: raw
```

after:
```yaml
doc_mapping:
  field_mappings:
    - name: field1
      type: text
      indexed: false
```

A field changed from being indexed to not being indexed.
A query such as `field1:my_value` was valid, but is now rejected.

### Example 2

before:
```yaml
doc_mapping:
  field_mappings:
    - name: field1
      type: text
      indexed: false
    - name: field2
      type: text
      tokenizer: raw

```

after:
```yaml
doc_mapping:
  field_mappings:
    - name: field1
      type: text
      tokenizer: raw
    - name: field2
      type: text
      tokenizer: raw
```

A field changed from being not indexed to being indexed.
A query such as `field1:my_value` was invalid before, and is now valid. When querying older splits, it won't return a match, but won't return an error either.
A query such as `field1:my_value OR field2:my_value` is now valid too. For old splits, it will return the same results as `field2:my_value` as field1 wasn't indexed before. For newer splits, it will return the expected results.
A query such as `NOT field1:my_value` would return all documents for old splits, and only documents where `field1` is not `my_value` for newer splits.


### Example 3

# show cast (trivial, valid and invalid)
# show array to single

before:
```yaml
doc_mapping:
  field_mappings:
    - name: field1
      type: text
    - name: field2
      type: u64
    - name: field3
      type: array<text>
```
document presents before update:
```json
{
  "field1": "123",
  "field2": 456,
  "field3": ["abc", "def"]
}
{
  "field1": "message",
  "field2": 987,
  "field3": ["ghi"]
}
```

after:
```yaml
doc_mapping:
  field_mappings:
    - name: field1
      type: u64
    - name: field2
      type: text
    - name: field3
      type: text
```

When querying this index, the documents returned would become:
```json
{
  "field1": 123,
  "field2": "456",
  "field3": "abc"
}
{
  // field1 is missing because "message" can't be converted to int
  "field2": "987",
  "field3": "ghi"
}
```
