# Updating the doc mapping of an index

Quickwit allows updating the mapping it uses, to add more fields to an existing index, or change how they are indexed. In doing so,
it does not reindex existing data, still lets you search through older documents where possible.

## Indexing

When receiving a new doc mapping for an index, Quickwit will restart indexing pipelines to take into account the changes. This operation
is asynchronous. You can't assume documents send immediately after the update will necessarily use the new mapping. On the other hand,
this usually happens very fast, possibly while some documents sent before the udpate haven't been commited yet. This means you can't
assume documents sent before the update won't be indexed using the new doc mapping.

## Querying

When receiving a query, Quickwit always validate that query against the most recent mapping. If a query was valid under a previous
mapping but is not compatible with the newer mapping, that query will be rejected. For instance if a field which was indexed no longer
is, or a field was removed, any query that uses it will become invalid. On the other hand, if a query was not valid for a previous doc
mapping, but is valid under the new doc mapping, Quickwit will process the query. When querying newer splits, it will behave normally,
when querying older splits, it will try ot execute the query as correctly as possible. If you find a situation where older splits
causes a valid request to return an error, please open a bug report.

For instance, if a field was added or newly marked as indexed, quickwit will behave as if no older document had the right value. This
means `field1:abc field2:def` where field2 is a new field would return no documents for older splits. However `field1:abc OR field2:def`
would return the same as `field1:abc` for those older splits, and `NOT field2:def`would return every documents.

If a field was previously not marked as fast, and it is now, aggregations using that field will assume the value is missing for older
documents, and will behave normally for newer.

If you remove stored fields, the documents returned from a query will instantly have that field hidden, like if that field never was
stored. If you change the type of a field, Quickwit will attempt to convert the old document format into the newer one. If it can't,
it will show the field as absent. Some conversions are trivially always correct, for instance converting from integer to text. Some are
dependant on your data, for instance converting text to integer. Finally some will always hide the fields, for instance converting text
to an object. If a field goes from being an array, to being single-valued, only the first value will be returned, as a direct value.
When converting an array field to be a part of a json field, Quickwit will decide per document and per field to show it as a direct
value (if it contained exactly one element for this document), or an array if it contained any other number of values.

## Reversibility

Quickwit doesn't store old doc mappings, it can't revert an update all by itself. However it also does not modify existing data when
receiving a new doc mapping. If you realize you updated the mapping in a way that's very wrong, you can re-update using the previous
mapping. Documents indexed while the mapping was wrong will be impacted, but any document that was commited before the change will be
back to its original state.
