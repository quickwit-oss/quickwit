#!/bin/sh

trap 'kill -- -$$' INT TERM EXIT

QUICKWIT_PORT=7280
QUICKWIT_ENDPOINT="http://localhost:$QUICKWIT_PORT"
POMSKY_INTAKE_ENDPOINT="0.0.0.0:8181"
SANDBOX=sandbox
DD_SITE=datad0g.com
export DD_ORG_ID=2

count=10
freq=1
count_next=0
freq_next=0
for arg in "$@"; do
    if [ $count_next = 1 ]; then
        count=$arg
        count_next=0
    elif [ $freq_next = 1 ]; then
        freq=$arg
        freq_next=0
    elif [ "${arg}x" = "-cx" ]; then
        count_next=1
    elif [ "${arg}x" = "-fx" ]; then
        freq_next=1
    else
        echo "Usage: $0 [-f frequency] [-c count] [-h] [-d] [-m] [-p]"
        echo "    frequency: submit signals every this many seconds, default 1"
        echo "    count: submit signals this many times before quitting, default 10"
        echo "    d: run byoc-dualship-mgr, default: false, use prefab values"
        echo "    h: run byoc-hosttags-mgr, default: false, use prefab values"
        echo "    m: run byoc-metrics-metadata, default: false, use prefab values"
        echo "    p: use local Pomsky (Quickwit) instead of sink-server.py"
        exit 0
    fi
done


# This script does a lot of path specific stuff, so make sure it knows where it is, or die if it can't figure it out.
LOC=$GOPATH/src/github.com/DataDog/pomsky/quickwit/pomsky-intake/local-test
cd "$LOC" || (echo "cannot find pomsky-intake/local-test directory, giving up" && exit 255)

# Check for API key, and die if we don't have it.
if [ "${DD_API_KEY}x" = "x" ]; then
    echo "ERROR: you do not have an API key defined in DD_API_KEY."
    exit 1
fi

rm -rf $SANDBOX
mkdir $SANDBOX

# Start the endpoint server
POMSKY_DIR=$GOPATH/src/github.com/DataDog/pomsky
echo Building Pomsky...
(cd "$POMSKY_DIR/quickwit" && cargo build -p quickwit-cli)
echo Starting Pomsky...
(cd "$POMSKY_DIR/quickwit" && cargo run -p quickwit-cli -- run --config $LOC/quickwit-local.yaml) &
POMSKY_PID=$!
echo Waiting for Pomsky to be ready...
while ! curl -sf http://localhost:7280/health/livez > /dev/null 2>&1; do
  if ! kill -0 $POMSKY_PID 2>/dev/null; then
    echo "ERROR: Pomsky process exited before becoming ready"
    exit 1
  fi
  sleep 2
done
echo Pomsky is ready
echo Building pomsky-intake...
(cd "$POMSKY_DIR/quickwit" && cargo build -p pomsky-intake)
echo Starting pomsky-intake...
(cd "$POMSKY_DIR/quickwit" && cargo run -p pomsky-intake -- --config $LOC/intake-local.yaml ) &
sleep 5

iter=0
while [ $iter -lt "$count" ]; do
    echo Pushing telemetry
    python generate-test-metrics.py
    python generate-test-logs.py
    python generate-test-traces.py
    python upload-test-data.py http://${POMSKY_INTAKE_ENDPOINT}
    sleep "$freq"
    iter=$((iter + 1))
done

# Give the indexing pipeline time to commit and publish splits
echo "Checking data arrived, this takes a bit due to 30 second flush interval..."
iter=0
while [ $iter -lt 7 ]; do
    echo "Still waiting $iter of 7"
    sleep 5
    iter=$((iter + 1))
done

failures=0
echo "Checking for metrics documents"
METRICS_METASTORE="$SANDBOX/indexes/datadog-metrics/metastore.json"
NUM_DOCS=$(python3 -c "
import json, sys
data = json.load(open('$METRICS_METASTORE'))
splits = data.get('metrics_splits', [])
published = [s for s in splits if s.get('state') == 'Published']
print(sum(s['metadata']['num_rows'] for s in published))
")
if [ "$NUM_DOCS" -gt 0 ]; then
  echo "SUCCESS: datadog-metrics has $NUM_DOCS published rows (parquet)"
else
  echo "FAIL: datadog-metrics has no published rows"
  failures=$((failures + 1))
fi

echo "Checking for logs documents"
DESCRIBE=$(curl -sf "$QUICKWIT_ENDPOINT/api/v1/indexes/datadog/describe")
NUM_DOCS=$(echo "$DESCRIBE" | python3 -c "import sys,json; print(json.load(sys.stdin)['num_published_docs'])")
if [ "$NUM_DOCS" -gt 0 ]; then
  echo "SUCCESS: datadog has $NUM_DOCS published docs"
else
  echo "FAIL: datadog has no published docs (logs)"
  echo "$DESCRIBE"
  failures=$((failures + 1))
fi

echo "Checking for spans documents"
DESCRIBE_TRACES=$(curl -sf "$QUICKWIT_ENDPOINT/api/v1/indexes/datadog-spans/describe")
NUM_TRACE_DOCS=$(echo "$DESCRIBE_TRACES" | python3 -c "import sys,json; print(json.load(sys.stdin)['num_published_docs'])")
if [ "$NUM_TRACE_DOCS" -gt 0 ]; then
  echo "SUCCESS: datadog-spans has $NUM_TRACE_DOCS published docs"
else
  echo "FAIL: datadog-spans has no published docs"
  echo "$DESCRIBE_TRACES"
  failures=$((failures + 1))
fi

if [ $failures -gt 0 ]; then
    echo FAILED
    exit 1
fi

echo SUCCEEDED