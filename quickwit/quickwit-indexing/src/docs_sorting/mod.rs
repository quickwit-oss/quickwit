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

//! Fingerprint based document sorting.
//!
//! Document sorting is enabled process-wide when `NodeConfig::docs_sorting_config` contains a
//! fingerprint policy, unless `QW_ENABLE_DOCS_SORTING=false`. Deployments should configure it only
//! on indexers whose workloads are intended to use document sorting. It is a write-time
//! optimization: documents that look structurally similar are assigned nearby Tantivy doc IDs in
//! the final segment. The fingerprint has two independent components: a schema hash built from leaf
//! JSON paths and a grouping hash built from configured raw or tokenized fields. When
//! fingerprinting is disabled, documents follow the normal indexing path and no doc ID remapping is
//! applied.
//!
//! ```text
//! Raw document
//!     |
//!     v
//! DocProcessor
//!     |  computes document fingerprint (first schema hash, then grouping hash) with Fingerprinter
//!     v
//! ProcessedDoc { doc, fingerprint_opt, ... }
//!     |
//!     v
//! Indexer
//!     |  records doc_id by fingerprint in DocIdSorter
//!     v
//! IndexedSplitBuilder::finalize
//!     |  builds DocIdMapping, largest schema groups first,
//!     |  then largest sort groups within each schema
//!     v
//! Tantivy segment with similar documents stored together
//! ```
//! The fingerprint is only carried through the in-memory indexing pipeline:
//! 1. `DocProcessor` computes it from the processed JSON document.
//! 2. `ProcessedDoc::fingerprint_opt` carries it to the `Indexer`.
//! 3. `Indexer` pushes `(fingerprint_opt, doc_id)` into `DocIdSorter` when fingerprinting is active
//!    for the split.
//! 4. During split finalization, `DocIdSorter` sorts schema groups by total document count, sorts
//!    sort groups within each schema by document count, and turns the result into a Tantivy
//!    `DocIdMapping`.

mod fingerprinter;
mod sorter;
mod tokenizer;

pub use fingerprinter::{Fingerprint, Fingerprinter};
pub use sorter::DocIdSorter;
pub use tokenizer::tokenize;
