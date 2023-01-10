"""
Provide classes and API to stores  of housekeeping objects.

There are two types homeomorphic classes

One class accesses AWS S3. This class can be configured
via housekeeping.toml. It can access the production or
development S3 buckets via different configurations. 

The other class  is "mock" store useful for
development and test. Thsi sote discards the data.

The StoreFactory class supports choosing which class is
used at run-time.

All classes use a namespace object (config), such
as provided by argparse, as part of their interface.
"""


import boto3
import botocore
import bson
import uuid
import logging
import toml
import time
import datetime
import sys
import utility_api

##################################
# stores
##################################

class StoreFactory:
    """
    Factory class to create Mock, or S3 stores 
    """
    def __init__ (self, config):
        config = utility_api.merge_config(config)
        type = config["store-type"]
        #instantiate, then return db object of correct type.
        if type == "mock" : self.store  =  Mock_store(config) ; return 
        if type == "S3"   : self.store  =  S3_store  (config) ; return 
        logging.fatal(f"store {type} not supported")
        exit (1)
        
    def get_store(self):
        return self.store

class Store_info:
    "a namespace for storage data"
    pass

class Base_store:
    " base class for common methods"
    def __init__(self, config):
        self.bucket = config["store-bucket"]
        self.n_stored = 0
        self.log_every = config["store-log-every"]
    
    def connect(self):
        pass

    def log(self, storeinfo):
        "log storage informmation, but not too often"
        msg1 = f"stored {self.n_stored} objects."
        msg2 = f"This object: {storeinfo.size} bytes to {storeinfo.bucket} {storeinfo.key}"
        if self.n_stored < 5 :
            logging.info(msg1)
            logging.info(msg2)
        elif self.n_stored % self.log_every == 0:
            logging.info(msg1)
            logging.info(msg2)

    def get_key(self, metadata, text_uuid):
        'compute the "path" to the object' 
        topic = metadata["topic"]
        t = time.gmtime(metadata["timestamp"]/1000)
        key = f"{topic}/{t.tm_year}/{t.tm_mon}/{t.tm_mday}/{t.tm_hour}/{text_uuid}.bson"
        return key
    
    def get_storeinfo(self, key, size):
        storeinfo = Store_info()
        storeinfo.size = size
        storeinfo.key = key
        storeinfo.bucket = self.bucket
        return storeinfo

    def get_as_bson(self, payload, metadata):
        "return a blob of bson"
        ret = bson.dumps({"message" : payload, "metadata" : metadata})
        return ret

    def get_object(self, key):
        "if not overriden, print error and die"
        logging.fatal("This source does not implement get_object")
        exit(1)

    def get_object_summary(self, key):
        "if not overriden, print error and die"
        logging.fatal("This source does not implement get_object")
        exit(1)
        
    
class S3_store(Base_store):
    "send things to s3"
    def __init__(self, config):
        super().__init__(config)
        
    def connect(self):
        "obtain an S3 Client"
        self.client = boto3.client('s3')

    def store(self, payload, metadata, text_uuid):
        """place data, metadata as an object in S3"""
        
        bucket = self.bucket
        key = self.get_key(metadata, text_uuid)
        b = self.get_as_bson(payload, metadata)
        size = len(b)
        self.client.put_object(Body=b, Bucket=bucket, Key=key)        
        self.n_stored += 1
        storeinfo = self.get_storeinfo(key, size)
        self.log(storeinfo)
        return storeinfo

    def get_object(self, key):
        "return oject from S3"
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=key)
        data = response['Body'].read()
        return data

    def get_object_summary(self, key):
        "if not overriden, print error and die"
        summary = {"exists" : False}
        try: 
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=key)

        except  botocore.errorfactory.ClientError as err:
            # Its important to differentiate between this
            # error and any other BOTO error.
            #
            # oh my! if the key does not exists,
            # boto throws a dymanically made class
            # botocore.errorfactory.NoSuchKey. Because the
            # class is dynamically made, it can't be
            # used in the except statement, above. so I've
            # provided this hokey test against __repr__
            # to indicate the key does not  exist.
        
            if "NoSuchKey" in err.__repr__() : return summary
            raise
        summary = {"exists" : True}
        size  = response["ContentLength"]
        summary["size"] = size 
        return summary


class Mock_store(Base_store):
    """
    a mock store that does nothing -- support debug and devel.
    """
    
    def __init__(self, config):
        self.bucket = "scimma-housekeeping"
        super().__init__(config)
        logging.info(f"Mock store configured")


    def store(self, payload, metadata, text_uuid):
        "mock operation of storing in s3"    
        self.n_stored += 1
        key = self.get_key(metadata, text_uuid)
        b = self.get_as_bson(payload, metadata)
        size = len(b)
        storeinfo = self.get_storeinfo(key, size)
        self.log(storeinfo)
        return storeinfo
