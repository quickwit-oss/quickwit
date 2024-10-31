#!/bin/bash

RESULT=0

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

for file in $(git ls-files | \
    grep "build\|src\|proto" | \
    grep -e "\.proto\|\.rs\|\.ts" | \
    grep -v "quickwit-proto/protos/third-party" | \
    grep -v "quickwit-proto/src" | \
    grep -v "/codegen/" \
)
do
    # echo "Checking $file";
    diff <(sed 's/{\\d+}/2024/' "${SCRIPT_DIR}/.agpl.license_header.txt") <(head -n 18 $file) > /dev/null
    HAS_AGPL_LICENSE=$?
    diff <(sed 's/{\\d+}/2024/' "${SCRIPT_DIR}/.ee.license_header.txt") <(head -n 16 $file) > /dev/null
    HAS_EE_LICENSE=$?
    HAS_LICENSE_HEADER=$(( $HAS_AGPL_LICENSE ^ $HAS_EE_LICENSE ))
    if [ $HAS_LICENSE_HEADER -ne 1 ]; then
        grep -q -i 'begin quickwit-codegen' $file
        GREPRESULT=$?
        if [ $GREPRESULT -ne 0 ]; then
            echo "Incomplete or missing license header in $file"
            RESULT=1
        fi
    fi
done

exit $RESULT
