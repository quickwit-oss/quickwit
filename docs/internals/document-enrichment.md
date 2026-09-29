# Post-routing document enrichment

`quickwit-indexing` exposes an optional Rust callback for applications embedding Quickwit that
need to add fields using indexing-time metadata. It runs after the indexer selects the destination
split and before Tantivy consumes the document. No Tantivy changes or mapping syntax are involved.

## Registration

Construct an `IndexingService` with `IndexingService::new`, then call
`with_doc_enricher_factory` before spawning the service. The factory receives each new pipeline's
identity and actual `DocMapper`. It returns either a callback for that pipeline or `None`.
This lets applications select pipelines and validate/cache schema fields at startup, rather than
performing schema lookups for every document. Factory errors prevent pipeline startup.

For example, assuming an optional, stored text field named `origin` is already in the mapping:

```rust
use std::sync::Arc;
use quickwit_indexing::{DocEnricher, DocEnricherFactory};

let factory: DocEnricherFactory = Arc::new(|pipeline, mapper| {
    if pipeline.source_id != "enriched-source" {
        return Ok(None);
    }
    let origin = mapper.schema().get_field("origin")?;
    let enricher: DocEnricher = Arc::new(move |context, doc| {
        anyhow::ensure!(doc.get_first(origin).is_none(), "origin field collision");
        doc.add_text(origin, context.split_id.as_str());
        Ok(())
    });
    Ok(Some(enricher))
});
let indexing_service = indexing_service.with_doc_enricher_factory(factory);
```

The example assumes the application has validated the field's type and cardinality. A field handle
belongs to its mapper's schema; do not reuse it across unrelated schemas. Selection is application
policy, not an implicit reservation of a field name.

Applications constructing pipelines directly can set `IndexingPipelineParams::doc_enricher_opt`.
Both `IndexingService::new` and the existing `start_indexing_service` default to no enrichment.
There is no YAML, REST, or dynamically loaded plugin configuration for this extension point.

## Context and lifecycle

The read-only `DocIndexingContext` provides:

- pipeline identity (index UID, source ID, node ID, pipeline UID);
- the actual initial split ID;
- the destination partition ID, including the overflow partition when the partition limit is reached;
- document mapping UID and destination schema.

The callback is invoked for each document, not just the first document in a split. Documents from
different input partitions may share an overflow split; a new workbench allocates new splits.
Do not infer the destination from the input partition or input-batch boundaries.

Callbacks run synchronously on the indexing worker and must not perform blocking I/O. An error
fails the indexing attempt instead of silently dropping the document. Normal pipeline retries can
invoke the callback again and can assign a different split ID. Callbacks must not depend on
exactly-once side effects. A callback is retained across restarts of the same pipeline's actors;
its state must not assume that a particular split remains open.

Enrichment is not invoked during merge or delete-and-merge execution. Added values are ordinary
mapped fields, so normal merges preserve them without knowing about the callback.

## Contract

The callback operates on an **already mapped** document. It must:

- only add schema-compatible values, without changing existing values;
- preserve routing, timestamp, clustering inputs, and Quickwit-reserved fields;
- enforce collision and cardinality rules for fields it supplies.

Required-field validation and mapping transformations have already happened. Concatenate fields,
the stored original source, and input-document byte counts are not recomputed. The indexer does
update field-presence metadata after enrichment, so `exists` queries on newly added indexed,
non-fast fields work as well as fast-field existence queries.

The no-callback path does not inspect or mutate documents for enrichment. No application-specific
field names or generation policy are built into the indexer.
