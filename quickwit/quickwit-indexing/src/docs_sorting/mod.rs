// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Fingerprint-based document sorting for better log compression.
//!
//! # Warning
//!
//! This feature is experimental. Its configuration and behavior may change, and it should be
//! validated on representative workloads before production use.
//!
//! # Motivation
//!
//! Logs normally enter Tantivy segments in arrival order, which is driven by batching, load
//! balancing, and event timing rather than message structure. A segment can therefore interleave
//! unrelated templates:
//!
//! ```text
//! api:    "server started at 8080"
//! worker: "job 123 finished in 42ms"
//! api:    "server started at 9090"
//! db:     "connection from 1.2.3.4"
//! worker: "job 456 finished in 13ms"
//! ```
//!
//! Stored log bodies and repeated fields compress better when similar documents are adjacent.
//! Reordering documents before segment finalization gives the doc-store compressor longer repeated
//! runs without changing the logical query model.
//!
//! # Design
//!
//! Document sorting is enabled process-wide when `NodeConfig::docs_sorting_config` contains a
//! policy, unless `QW_DISABLE_DOCS_SORTING=true`. Deployments should configure it only on indexers
//! whose workloads are intended to use document sorting.
//!
//! [`Fingerprinter`] computes two independent hashes:
//!
//! 1. A schema hash from the sorted set of leaf JSON paths, excluding configured paths.
//! 2. A grouping hash from configured raw string values and tokenized signatures.
//!
//! Tokenized fields hash token types rather than literal values, up to 50 tokens. This groups
//! volatile values with the same shape:
//!
//! ```text
//! "server started at 8080" -> Word Gap Word Gap Word Gap Number
//! "server started at 9090" -> Word Gap Word Gap Word Gap Number
//! ```
//!
//! Missing and non-string grouping values are encoded as absent so each configured field retains
//! its position in the grouping hash.
//!
//! ```text
//! Raw document
//!     |
//!     v
//! DocProcessor
//!     |  computes Fingerprint { schema, grouping }
//!     v
//! ProcessedDoc { doc, fingerprint_opt, ... }
//!     |
//!     v
//! Indexer
//!     |  records each split-local doc ID in DocIdSorter
//!     v
//! IndexedSplitBuilder::finalize
//!     |  builds a DocIdMapping: largest schema groups first,
//!     |  then largest grouping-hash groups within each schema
//!     v
//! Tantivy segment with similar documents stored together
//! ```
//!
//! The fingerprint exists only in the in-memory indexing pipeline:
//!
//! 1. `DocProcessor` computes it from the processed JSON document.
//! 2. `ProcessedDoc::fingerprint_opt` carries it to the `Indexer`.
//! 3. `Indexer` pushes `(fingerprint_opt, doc_id)` into [`DocIdSorter`].
//! 4. At split finalization, [`DocIdSorter`] creates the Tantivy `DocIdMapping`. Documents without
//!    fingerprints are appended in insertion order.
//!
//! Sorting is applied once to each fresh split. Later merges use Tantivy's native merge path and
//! preserve the ordering of their input splits without repeating the remapping and doc-store
//! rewrite.
//!
//! # Consequences
//!
//! Benefits:
//!
//! - Recurring schemas, message templates, and stable dimensions gain better compression locality.
//! - Search semantics, checkpoints, split metadata, and user-visible schemas remain unchanged.
//! - Dynamic log schemas do not require a fixed sort schema.
//!
//! Trade-offs and risks:
//!
//! - Manual doc ID mapping writes a temporary uncompressed doc store and then rewrites it in
//!   permuted, compressed order, increasing local disk traffic.
//! - Fingerprinting adds ingest CPU, particularly for tokenized fields.
//! - [`DocIdSorter`] retains document IDs and hash-map entries outside Tantivy's reported memory
//!   usage; high-cardinality policies and multiple open partitions require memory headroom.
//! - Fingerprints provide best-effort locality, not a total order or query-pruning structure.
//! - Policies that include volatile data can under-group, while broad exclusions can over-group.
//!   Compression gains therefore depend on workload entropy and should be measured on a
//!   representative corpus together with CPU, memory, and temporary disk writes.
//!
//! This implementation targets logs in the Tantivy indexing path. The principle generalizes to
//! other signals, but metrics and traces have different physical layouts and query patterns and
//! require signal-specific sorting policies.

mod fingerprinter;
mod sorter;
mod tokenizer;

pub use fingerprinter::{Fingerprint, Fingerprinter};
pub use sorter::DocIdSorter;
pub use tokenizer::tokenize;
