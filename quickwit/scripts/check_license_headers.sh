#!/bin/bash

RESULT=0

for file in $(git ls-files | \
    grep "build\|src\|proto" | \
    grep -e "\.proto\|\.rs\|\.ts" | \
    grep -v "quickwit-proto/protos/third-party" | \
    grep -v "quickwit-proto/src" | \
    grep -v "/codegen/" \
)
do
    diff <(sed 's/{\\d+}/2023/' .license_header.txt) <(head -n 19 $file) > /dev/null
    DIFFRESULT=$?
    if [ $DIFFRESULT -ne 0 ]; then
        grep -q -i 'begin quickwit-codegen' $file
        GREPRESULT=$?
        if [ $GREPRESULT -ne 0 ]; then
            echo "Incomplete or missing license header in $file"
            RESULT=1
        fi
    fi
done

exit $RESULT
