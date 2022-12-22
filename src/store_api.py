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

All classes use a namespace object (args), such
as provided by argparse, as part of their interface.
"""


import boto3
import bson
import uuid
import logging
import toml
import time
import datetime

##################################
# stores
##################################

class StoreFactory:
    """
    Factory class to create Mock, or S3 stores 
    """
    def __init__ (self, args):
        toml_data = toml.load(args.toml_file)
        stanza = args.store_stanza
        logging.info(f"using dabase stanza {stanza}")
        config    = toml_data.get(stanza, None)
        if not config:
            logging.fatal("could not find {stanza} in {args.toml_file}")

        type = config["type"]
        #instantiate, then return db object of correct type.
        if type == "mock" : self.store  =  Mock_store(args, config) ; return 
        if type == "S3"   : self.store  =  S3_store  (args, config) ; return 
        logging.fatal(f"store {type} not supported")
        exit (1)
        
    def get_store(self):
        return self.store

class Store_info:
    "a namespace for storage data"
    pass

class Base_store:
    " base class for common methods"
    def __init__(self, args, config):
        self.bucket = config["bucket"]
        self.n_stored = 0
        self.log_every = 100   #move to config file.
    
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

    def get_key(self, metadata):
        'compute the "path" to the object' 
        topic = metadata["topic"]
        uuid_ = self.get_uuid(metadata)
        t = time.gmtime(metadata["timestamp"]/1000)
        key = f"{topic}/{t.tm_year}/{t.tm_mon}/{t.tm_mday}/{t.tm_hour}/{uuid.UUID(bytes=uuid_)}.bson"
        return key

    def get_uuid(self, metadata):
        for label, value in metadata["headers"]:
            if label ==  "_id" : return value
        raise Exception ("Missing UUID header in headers")
    
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
    
class S3_store(Base_store):
    "send things to s3"
    def __init__(self, args, config):
        super().__init__(args, config)
        
    def connect(self):
        "obtain an S3 Client"
        self.client = boto3.client('s3')

    def store(self, payload, metadata):
        """place data, metadata as an object in S3"""
        
        bucket = self.bucket
        key = self.get_key(metadata)
        b = self.get_as_bson(payload, metadata)
        size = len(b)
        self.client.put_object(Body=b, Bucket=bucket, Key=key)        
        self.n_stored += 1
        storeinfo = self.get_storeinfo(key, len(payload))
        self.log(storeinfo)
        return storeinfo

class Mock_store(Base_store):
    """
    a mock store that does nothing -- support debug and devel.
    """
    
    def __init__(self, args, config):
        self.bucket = "scimma-housekeeping"
        super().__init__(args, config)
        logging.info(f"Mock store configured")


    def store(self, payload, metadata):
        "mock operation of storing in s3"    
        self.n_stored += 1
        key = self.get_key(metadata)
        b = self.get_as_bson(payload, metadata)
        size = len(b)
        storeinfo = self.get_storeinfo(key, size)
        self.log(storeinfo)
        return storeinfo