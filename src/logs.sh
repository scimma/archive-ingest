#!/bin/bash
#
# Print the most recent hopdevel-housekeeping-db logs 
#set -x
last_log=`aws rds describe-db-log-files --db-instance-identifier hopdevel-housekeeping-db | jq .DescribeDBLogFiles[].LogFileName | tail -1`
last_log=`echo $last_log | tr -d \"`
logs=`aws  rds download-db-log-file-portion   --db-instance-identifier hopdevel-housekeeping-db --log-file-name $last_log | jq .LogFileData | tr -d \"`
echo -e  $logs 
