#!/usr/bin/env bash

set -eu

awslocal kinesis create-stream --stream-name quickwit-dev-stream --shard-count 3
awslocal s3 mb s3://quickwit-dev
awslocal s3 mb s3://quickwit-integration-tests && awslocal s3 rm --recursive s3://quickwit-integration-tests
