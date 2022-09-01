// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.


use once_cell::sync::Lazy;
use quickwit_common::metrics::{new_histogram, Histogram};

use crate::actors::PackagerType;

pub struct IndexingMetrics {
    pub packager: PackagerMetrics,
    pub merge_packager: PackagerMetrics,
}

impl IndexingMetrics {
    pub fn get_packager_metrics(&self, packager_type: PackagerType) -> &PackagerMetrics {
        match packager_type {
            PackagerType::Packager => &self.packager,
            PackagerType::MergePackager => &self.merge_packager,
        }
    }
}

pub struct PackagerMetrics {
    pub duration_seconds: Histogram,
}

impl PackagerMetrics {
    pub fn new(packager_type: PackagerType) -> PackagerMetrics {
        let prefix= match packager_type {
            PackagerType::Packager => "packager",
            PackagerType::MergePackager => "merger_packager",
        };
        let description_suffix = match packager_type {
            PackagerType::Packager => "freshly indexed split batches (as opposed to merge)",
            PackagerType::MergePackager => "split resulting from a merge.",
        };
        PackagerMetrics {
            duration_seconds: new_histogram(
                &format!("{prefix}_duration_seconds"),
                &format!("Number of seconds require to package {description_suffix}."),
                "quickwit"
            ),
        }

    }
}

impl Default for IndexingMetrics {
    fn default() -> Self {
        IndexingMetrics {
            packager: PackagerMetrics::new(PackagerType::Packager),
            merge_packager: PackagerMetrics::new(PackagerType::MergePackager),
        }
    }
}

/// Serve counters exposes a bunch a set of metrics about the request received to quickwit.
pub static INDEXING_METRICS: Lazy<IndexingMetrics> = Lazy::new(IndexingMetrics::default);
