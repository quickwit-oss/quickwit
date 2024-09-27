# Updating the doc mapping of an index

Quickwit allows updating the mapping it uses, to add more fields to an existing index, or change how they are indexed. In doing so,
it does not reindex existing data, still lets you search through older documents where possible.

## Indexing

When you update a doc mapping for an index, Quickwit will restart indexing pipelines to take the changes into account. As both this operation and the document ingestion are asynchronous, you can't assume documents sent immediately after the update will necessarily use the new mapping nor that documents sent immediately before the update won't be indexed using the new doc mapping.

## Querying

When receiving a query, Quickwit always validate it against the most recent mapping.
If a query was valid under a previous mapping but is not compatible with the newer mapping, that query will be rejected.
For instance if a field which was indexed no longer is, any query that uses it will become invalid.
On the other hand, if a query was not valid for a previous doc mapping, but is valid under the new doc mapping, Quickwit will process the query.
When querying newer splits, it will behave normally, when querying older splits, it will try ot execute the query as correctly as possible.
If you find a situation where older splits causes a valid request to return an error, please open a bug report.
See example 1 and 2 below for clarification.

Change in tokenizer affect only newer splits, older splits keep using the tokenizers they were created with.

Document retrieved are mapped from Quickwit internal format to JSON based on the latest doc mapping. This means if fields are deleted,
they will stop appearing (see also Reversibility below). If the type of some field changed, it will be converted on a best effort basis:
integers will get turned into text, text will get turned into string when it is possible, otherwise, the field is omited.
See example 3 for clarification.

## Reversibility

Quickwit doesn't store old doc mappings, it can't revert an update all by itself. However it also does not modify existing data when
receiving a new doc mapping. If you realize you updated the mapping in a way that's very wrong, you can re-update using the previous
mapping. Documents indexed while the mapping was wrong will be impacted, but any document that was commited before the change will be
back to its original state.

## Examples

In all exemples, fields which are not relevant are removed for conciseness, you will not be able to use these index config as is.

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
