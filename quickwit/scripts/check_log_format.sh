#!/bin/bash

RESULT=0

for file in $(git ls-files | egrep "src/.*\.rs$")
do
    LOG_STARTING_WITH_UPPERCASE=$(egrep -n "(warn|info|error|debug)\!\(\"[A-Z][a-z]" $file)
    DIFFRESULT=$?
    LOG_ENDING_WITH_PERIOD=$(egrep -n "(warn|info|error|debug)\!.*\.\"\);" $file)
    DIFFRESULT=$(($DIFFRESULT && $?))
    if [ $DIFFRESULT -eq 0 ]; then
      echo "===================="
      echo $file
      echo $LOG_STARTING_WITH_UPPERCASE
      echo $LOG_ENDING_WITH_PERIOD
      echo $FAULTY_LINES
      RESULT=1
    fi
done

exit $RESULT
