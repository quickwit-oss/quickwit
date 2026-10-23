#!/usr/bin/env bash
#
# Compares a generated metrics inventory with the committed one and explains
# what to do when they differ. Exits 0 if they are identical, 1 otherwise.
#
# Added metrics and description changes only require regenerating the
# inventory. Removed or renamed metrics, label changes, and type changes may
# break users of the metric and require attention.
#
# Requires `yq` (https://github.com/mikefarah/yq), preinstalled on GitHub
# Actions Ubuntu runners.
#
# Usage:
#   ./scripts/check.sh <committed-inventory.yaml> <generated-inventory.yaml>

set -euo pipefail
export LC_ALL=C

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <committed-inventory.yaml> <generated-inventory.yaml>" >&2
    exit 2
fi
COMMITTED=$1
GENERATED=$2

status=0
DIFF=$(diff -u "$COMMITTED" "$GENERATED") || status=$?
if (( status == 0 )); then
    exit 0
elif (( status > 1 )); then
    exit "$status"
fi

# One `<kind> <metric name>{<labels>}` line per metric.
metrics() {
    yq '.[] | to_entries | .[] | .value.kind + " " + .key' "$1" | sort -u
}

# A committed metric that is no longer generated with the same name, labels,
# and kind was removed, renamed, relabeled, or changed type. Added metrics and
# description changes never show up here.
BREAKING=$(comm -23 <(metrics "$COMMITTED") <(metrics "$GENERATED"))

report() {
    echo "## Metrics inventory is out of date"
    echo
    echo "The generated metrics inventory differs from the committed \`$COMMITTED\`."
    echo "Run \`make metrics-inventory\` to regenerate it, then commit the result."
    echo
    if [[ -z "$BREAKING" ]]; then
        echo "Only added metrics or description changes were detected:"
        echo "regenerating the inventory is enough."
    else
        echo "### Changes that require attention"
        echo
        echo "The following metrics were removed, renamed, or had their labels or type"
        echo "changed. This can break their users. Regenerate the inventory, but be very"
        echo "cautious: identify every user of these metrics (dashboards, monitors, alerts,"
        echo "SLOs, documentation, downstream consumers), assess the impact, and"
        echo "communicate the change before merging."
        echo
        while read -r kind key; do
            echo "- \`$key\` ($kind)"
        done <<< "$BREAKING"
    fi
    echo
    echo "### Diff"
    echo
    echo '```diff'
    echo "$DIFF"
    echo '```'
}

report

if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    if [[ -z "$BREAKING" ]]; then
        echo "::error title=Metrics inventory out of date::Run 'make metrics-inventory' and commit $COMMITTED. Only added metrics or description changes were detected: regenerating is enough."
    else
        echo "::error title=Metrics inventory out of date::Run 'make metrics-inventory' and commit $COMMITTED. Some metrics were removed, renamed, or had their labels or type changed: identify their users, assess the impact, and communicate the change. See the job summary."
        while read -r kind key; do
            echo "::error title=Metric removed or changed::$key ($kind) was removed, renamed, or had its labels or type changed."
        done <<< "$BREAKING"
    fi
fi
exit 1
