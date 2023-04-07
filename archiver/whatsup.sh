#!/bin/bash
# get logs from hosekeeping 
pod=`kubectl get pods  | grep house | awk '{print $1}'`
kubectl logs $pod 
