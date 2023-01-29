#!/bin/bash
set -x

LOGS=test.log

runit() {
    cmd=$*
    echo $cmd
    $cmd 2>> $LOGS
    if [ $? -ne  0 ] ; then
        echo test failed on $cmd
        tail -20 $LOGS
        exit 1
    fi
}

echo test run on `date` > test.log

runit ./houseutils.py clean_tests
runit ./houseutils.py publish
runit ./housekeeping.py run -t -v -H hop-prod -D aws-dev-db -S S3-dev
runit ./houseutils.py verify_db_to_store -s -t 'house*test*'
