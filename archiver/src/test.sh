#!/bin/bash
#
# Test the primary message path of  archive_ingest
#
# This program runs a number of test progrgms. All stderr form
# all programs is accumulated in a unified log file.

# The file is left in place after the test script finished.
# test objects are preserved in the store and the database.

# Should a test program show thow an error,
# - no futher programs are run
# - the last few lines of the unified log file is shown.
# - can be run against the live replyed producition system...
#   ... uses the reserved quiet channel archive-ingest-test
#

#set -x

case "$1" in
    "devel")
        hop_flag="-H hop-prod"
        store_flag="-S S3-dev"
        db_flag="-D aws-dev-db"
        hop_mock_flag="-H mock-hop"
        
        ;;
    "prod")
        hop_flag="-H hop-prod"
        store_flag="-S S3-prod"
        db_flag="-D aws-prod-db"
        hop_mock_flag="-H mock-hop"
        ;;
    *)
        echo choose "prod" or "devel"
        exit 1
        ;;
esac

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

echo test run on `date` > $LOGS

# purge any retined sta fromo DB, Store and HOP
runit ./ingestutils.py clean_tests $hop_flag  $db_flag $store_flag

# pubish know test  known test data on the test topic
runit ./ingestutils.py publish $hop_flag 

# read that test data compare to published (-t) ...
#  and aslo read back form store and verify saved data (-v).
runit ./archive_ingest.py run -t -v $hop_flag  $db_flag $store_flag

# further check that the DB models the store for this test..
# sampel teh DB rows and see that objects exist
runit ./ingestutils.py verify_db_to_store -s -t sys.archive-ingest-test  $db_flag $store_flag

# for kicks sample the whole DB and see that objects exist in the store
runit ./ingestutils.py verify_db_to_store -s  $db_flag $store_flag

# run mocks which simulate circumstances not reproduceable by publiching
# precess these  and verify against what was stores (-v)
runit ./archive_ingest.py run  -v  $hop_mock_flag  $db_flag $store_flag

# verify stored mocks are as recieved. 
runit ./ingestutils.py verify_db_to_store -s -t mock.topic  $db_flag $store_flag


