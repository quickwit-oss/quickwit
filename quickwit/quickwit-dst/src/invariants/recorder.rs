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

//! Invariant metrics recorder — Layer 4 of the verification stack.
//!
//! Every call to [`check_invariant!`](crate::check_invariant) evaluates the
//! condition in **all** build profiles (debug and release). The result is
//! recorded as metrics.

use quickwit_metrics::{LabelNames, LazyCounter, counter, label_names, label_values, lazy_counter};

use super::InvariantId;

const INVARIANT_LABEL_NAMES: LabelNames<1> = label_names!("invariant");

static POMSKY_INVARIANT_CHECKED_TOTAL: LazyCounter = lazy_counter!(
        name: "checked",
        description: "Number of invariant checks performed by Pomsky.",
        system: "pomsky",
        subsystem: "invariant",
        separator: ".",
);

static POMSKY_INVARIANT_VIOLATED_TOTAL: LazyCounter = lazy_counter!(
        name: "violated",
        description: "Number of invariant violations observed by Pomsky.",
        system: "pomsky",
        subsystem: "invariant",
        separator: ".",
);

/// Record an invariant check result.
///
/// Called by [`check_invariant!`](crate::check_invariant) on every invocation,
/// in both debug and release builds.
#[inline]
pub fn record_invariant_check(invariant_id: InvariantId, passed: bool) {
    let invariant_name = invariant_id.as_str();
    let labels = label_values!(INVARIANT_LABEL_NAMES => invariant_name);
    counter!(parent: POMSKY_INVARIANT_CHECKED_TOTAL, labels: [labels]).inc();
    if !passed {
        counter!(parent: POMSKY_INVARIANT_VIOLATED_TOTAL, labels: [labels]).inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_checked_and_violated_metrics() {
        let invariant_id = InvariantId::TW2;
        let labels = label_values!(INVARIANT_LABEL_NAMES => invariant_id.as_str());
        let checked_counter = counter!(parent: POMSKY_INVARIANT_CHECKED_TOTAL, labels: [labels]);
        let violated_counter = counter!(parent: POMSKY_INVARIANT_VIOLATED_TOTAL, labels: [labels]);

        assert_eq!(checked_counter.key().name(), "pomsky.invariant.checked");
        assert_eq!(violated_counter.key().name(), "pomsky.invariant.violated");

        let initial_checked = checked_counter.get();
        let initial_violated = violated_counter.get();

        record_invariant_check(invariant_id, true);
        record_invariant_check(invariant_id, false);

        assert_eq!(checked_counter.get(), initial_checked + 2);
        assert_eq!(violated_counter.get(), initial_violated + 1);
    }
}
