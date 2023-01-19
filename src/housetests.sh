#!/bin/sh



LOG=tests.log
echo test oupute is sent to  $LOG

run () {
    echo CMD $*
    $*  2>>$LOG
    if [ $? -ne 0 ] ; then echo FAIL $* ; exit ; fi 
}

run ./houseutils.py clean_tests
run ./houseutils.py  status | grep house
run ./houseutils.py  publish
run ./houseutils.py  status | grep house
run ./housekeeping.py run -t -D aws-dev-db -H hop-prod -S S3-dev
run ./houseutils.py  status | grep house
