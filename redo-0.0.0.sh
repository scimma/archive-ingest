#!/bin/bash
# Suport devleopment on teh 0.0.0  containers
# delete  the 0.0.0 images set
# use make to load a new 0.0.0

mode=""
if [ "$1" == "local" ] ; then mode="local" ; fi
if [ "$1" == "aws"   ] ; then mode="aws"   ; fi
if [ -z "$mode"       ] ; then echo argment must be local or AWS ; exit 1 ; fi
                           
                              

echo '======= Nuke Old Stuff ========'
sleep 3 

# remove image from  local docker
id=`docker image ls  | grep housekeeping | grep "0\.0\.0" | awk '{print $3}'`
if [ -n "$id" ] ; then docker image rm $id --force ; fi

if [ "$mode" = "aws"  ] ; then
# remove images from Awb
    aws ecr batch-delete-image --repository-name scimma/housekeeping --image-ids imageTag=0.0.0
    aws ecr batch-delete-image --repository-name scimma/housekeeping --image-ids imageTag=0.0
    aws ecr batch-delete-image --repository-name scimma/housekeeping --image-ids imageTag=0
fi

echo '======= Make Container ========'
sleep 3 

make container || exit $0

echo '======== Push Container ============='
sleep 3 

make  push-$mode  GITHUB_REF=housekeeping-0.0.0 || exit $0

set -x 
if [ "$mode" = "aws"  ] ; then

    echo '======== Restart pod ============='
    sleep 3 

    pod=`kubectl get pods | grep housekeeping | awk '{print $1}'`
    echo deleting $pod
    kubectl delete pods $pod  --grace-period=0 --force
    sleep 5
    status=`kubectl get pods | grep housekeeping`
    echo new pod:  $status
 fi

if [ "$mode" = "local"  ] ; then
    echo  docker run --platform linux/amd64 -it -v /Users/donaldp/.aws/boto.cfg:/etc/boto.cfg scimma/housekeeping
fi
