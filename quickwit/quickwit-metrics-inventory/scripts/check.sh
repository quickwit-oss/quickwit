#!/usr/bin/env bash
#
# Compares a generated metrics inventory with the committed one and explains
# what to do when they differ. Exits 0 if they are identical, 1 otherwise.
#
# Added metrics and description changes only require regenerating the
# inventory. Removed or renamed metrics, label changes, and kind (type)
# changes may break users of the metric and require attention.
#
# Usage:
#   ./scripts/check.sh <committed-inventory.yaml> <generated-inventory.yaml>
#
# When run in GitHub Actions, errors are emitted as annotations and a report
# is appended to the job summary.

set -euo pipefail
# `sort`, `comm`, and `join` must agree on ordering.
export LC_ALL=C

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <committed-inventory.yaml> <generated-inventory.yaml>" >&2
    exit 2
fi
COMMITTED=$1
GENERATED=$2

DIFF=$(diff -u "$COMMITTED" "$GENERATED") && exit 0 || status=$?
if (( status > 1 )); then
    exit "$status"
fi

# Prints `<key>\t<kind>` for every metric, where `<key>` is the metric name
# with its static labels (e.g. `quickwit_foo{a=b}`). The inventory is a
# two-level YAML mapping: `module: { key: { kind, description } }`.
metric_kinds() {
    awk '
        /^  [^ ]/ {
            key = $0
            sub(/^  /, "", key)
            sub(/:$/, "", key)
            gsub("[\"\047]", "", key)
        }
        /^    kind: / { print key "\t" $2 }
    ' "$1" | sort -u
}
base_names() {
    cut -f 1 | sed 's/{.*//' | sort -u
}

COMMITTED_KINDS=$(metric_kinds "$COMMITTED")
GENERATED_KINDS=$(metric_kinds "$GENERATED")

# Keys (name + labels) present in the committed inventory but not generated.
MISSING_KEYS=$(comm -23 <(cut -f 1 <<< "$COMMITTED_KINDS" | sort -u) <(cut -f 1 <<< "$GENERATED_KINDS" | sort -u))
GENERATED_NAMES=$(base_names <<< "$GENERATED_KINDS")

REMOVED=()
RELABELED=()
while read -r key; do
    [[ -z "$key" ]] && continue
    name=${key%%\{*}
    if grep -qxF "$name" <<< "$GENERATED_NAMES"; then
        RELABELED+=("$key")
    else
        REMOVED+=("$key")
    fi
done <<< "$MISSING_KEYS"

# Keys present in both inventories with a different kind.
KIND_CHANGED=()
while IFS=$'\t' read -r key old_kind new_kind; do
    [[ -z "$key" ]] && continue
    KIND_CHANGED+=("$key: $old_kind -> $new_kind")
done < <(join -t $'\t' <(echo "$COMMITTED_KINDS") <(echo "$GENERATED_KINDS") | awk -F '\t' '$2 != $3')

NEEDS_ATTENTION=$(( ${#REMOVED[@]} + ${#RELABELED[@]} + ${#KIND_CHANGED[@]} ))

report() {
    echo "## Metrics inventory is out of date"
    echo
    echo "The generated metrics inventory differs from the committed \`$COMMITTED\`."
    echo "Run \`make metrics-inventory\` to regenerate it, then commit the result."
    echo
    if (( NEEDS_ATTENTION == 0 )); then
        echo "Only metric additions or description changes were detected:"
        echo "regenerating the inventory is enough."
    else
        echo "### Changes that require attention"
        echo
        echo "Removing or renaming a metric, or changing its labels or type, can break"
        echo "its users. Regenerate the inventory, but be very cautious: identify every"
        echo "user of these metrics (dashboards, monitors, alerts, SLOs, documentation,"
        echo "downstream consumers), assess the impact, and communicate the change"
        echo "before merging."
        print_list "Removed or renamed" "${REMOVED[@]+"${REMOVED[@]}"}"
        print_list "Labels changed" "${RELABELED[@]+"${RELABELED[@]}"}"
        print_list "Type changed" "${KIND_CHANGED[@]+"${KIND_CHANGED[@]}"}"
    fi
    echo
    echo "### Diff"
    echo
    echo '```diff'
    echo "$DIFF"
    echo '```'
}
print_list() {
    local title=$1
    shift
    (( $# == 0 )) && return
    echo
    echo "#### $title"
    echo
    printf -- '- `%s`\n' "$@"
}

report
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    report >> "$GITHUB_STEP_SUMMARY"
fi

if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    if (( NEEDS_ATTENTION == 0 )); then
        echo "::error title=Metrics inventory out of date::Run 'make metrics-inventory' to regenerate $COMMITTED and commit it. Only additions or description changes were detected: regenerating is enough."
    else
        echo "::error title=Metrics inventory out of date::Run 'make metrics-inventory' to regenerate $COMMITTED and commit it. Some changes require attention: identify all users of the affected metrics, assess the impact, and communicate the change. See the job summary."
    fi
    for key in "${REMOVED[@]+"${REMOVED[@]}"}"; do
        echo "::error title=Metric removed or renamed::$key. Identify all users of this metric, assess the impact, and communicate the change before merging."
    done
    for key in "${RELABELED[@]+"${RELABELED[@]}"}"; do
        echo "::error title=Metric labels changed::$key. Identify all users of this metric, assess the impact, and communicate the change before merging."
    done
    for change in "${KIND_CHANGED[@]+"${KIND_CHANGED[@]}"}"; do
        echo "::error title=Metric type changed::$change. Identify all users of this metric, assess the impact, and communicate the change before merging."
    done
fi
exit 1
