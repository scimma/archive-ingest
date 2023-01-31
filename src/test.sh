#!/bin/bash
#
# Test the primary message path in the devel version of archive_ingest
#
# This program runs a number of test progrgms. All stderr form
# all programs is accumulated in a unified log file.

# The file is left in place after the test script finished.
# test objects are preserved in the store and the database.

# Should a test program show thow an error,
# - no futher programs are run
# - the last few lines of the unified log file is shown.
#
# n.b can be generalised  to test "prod" in situ,
#    but well you can work forever, so that's
#    an issue for a future version.
#

#set -x

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
runit ./ingestutils.py clean_tests

# pubish know test  known test data on the test topic
runit ./ingestutils.py publish

# read taht test dats compare to published (-t) ...
#  and aslo read back form store and verify saved data (-v).
runit ./archive_ingest.py run -t -v -H hop-prod -D aws-dev-db -S S3-dev
v
# further check that the DB models the store for this test..
# sampel teh DB rows and see that objects exist
runit ./ingestutils.py verify_db_to_store -s -t 'house*test*'

# for kicks sample the whole DB and see that objects exist in the store
runit ./ingestutils.py verify_db_to_store -s

# run mocks which simulate circumstances not reproduceable by publiching
# precess these  and verify against what was stores (-v)
runit ./archive_ingest.py run  -v -H mock-hop -D aws-dev-db -S S3-dev

# verify stored mocks are as recieved. 
runit ./ingestutils.py verify_db_to_store -s -t '*mock*'


