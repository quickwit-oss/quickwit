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
use quickwit_common::metrics::{new_counter, new_gauge, IntCounter, IntGauge};

pub struct IngestMetrics {
    pub ingested_num_bytes: IntCounter,
    pub ingested_num_docs: IntCounter,
    pub queue_count: IntGauge,
}


impl Default for IngestMetrics {
    fn default() -> Self {
        Self {
            ingested_num_bytes: new_counter(
                "ingested_num_bytes",
                "Total size of the docs ingested in bytes",
                "quickwit_ingest",
            ),
            ingested_num_docs: new_counter(
                "ingested_num_docs",
                "Number of docs recieved to be ingested",
                "quickwit_ingest",
            ),
            queue_count: new_gauge(
                "queue_count",
                "Number of queues currently active",
                "quickwit_ingest",
            ),
        }
    }
}

pub static INGEST_METRICS: Lazy<IngestMetrics> = Lazy::new(IngestMetrics::default);
