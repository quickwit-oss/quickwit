#!/bin/bash

RESULT=0

for file in $(git ls-files | grep "src\|proto" | grep -e "\.proto\|\.rs\|\.ts" | grep -v "quickwit-proto/otlp" | grep -v "quickwit-proto/src")
do
    diff <(sed 's/{\\d+}/2022/' .license_header.txt) <(head -n 19 $file)
    DIFFRESULT=$?
    if [ $DIFFRESULT -ne 0 ]; then
        echo $DIFFRESULT
        echo "---"
        echo "Incomplete or missing license header in $file"
        echo "---"
        echo
        RESULT=1
    fi
done

exit $RESULT
