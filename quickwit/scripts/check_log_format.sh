#!/bin/bash

git grep -n -E '(warn|info|error|debug)!\("[A-Z][a-z]|(warn|info|error|debug)!.*\."\);' -- ':(glob)**/src/**/*.rs'
RESULT=$?

if [ "$RESULT" -eq 1 ]; then
    exit 0
fi
if [ "$RESULT" -eq 0 ]; then
    exit 1
fi
exit "$RESULT"
