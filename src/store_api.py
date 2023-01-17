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
import logging
import time
import utility_api
import zlib

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

class Base_store:
    " base class for common methods"
    def __init__(self, config):
        self.primary_bucket = config["store-primary-bucket"]
        self.backup_bucket  = config["store-backup-bucket"]
        self.n_stored = 0
        self.log_every = config["store-log-every"]

    def connect(self):
        pass

    def log(self, archiver_notes):
        "log storage informmation, but not too often"
        msg1 = f"stored {self.n_stored} objects."
        msg2 = f"This object: {archiver_notes['size']} bytes to {archiver_notes['bucket']} {archiver_notes['key']}"
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

    def set_storeinfo(self, archiver_notes, key, size, crc32):
        archiver_notes['size'] = size
        archiver_notes['key'] = key
        archiver_notes['bucket'] = self.primary_bucket
        archiver_notes['crc32'] = crc32


    def get_as_bson(self, payload, metadata, archiver_notes):
        "return a blob of bson"
        ret = bson.dumps({"message" : payload,
                          "metadata" : metadata,
                          "archiver_notes": archiver_notes
                          })
        return ret

    def get_object(self, key):
        "if not overriden, print error and die"
        logging.fatal("This source does not implement get_object")
        exit(1)

    def get_object_summary(self, key):
        "if not overriden, print error and die"
        logging.fatal("This source does not implement get_object")
        exit(1)

    def deep_delete(self, bucket, key):
        "if not overriden, print error and die"
        logging.fatal("This source does not implement deep_delete")
        exit(1)


class S3_store(Base_store):
    "send things to s3"
    def __init__(self, config):
        super().__init__(config)

    def connect(self):
        "obtain an S3 Client"
        self.client = boto3.client('s3')

    def store(self, payload, metadata, archiver_notes):
        """place data, metadata as an object in S3"""

        bucket = self.primary_bucket
        key = self.get_key(metadata, archiver_notes["con_text_uuid"])
        b = self.get_as_bson(payload, metadata, archiver_notes)
        size = len(b)
        crc32 = zlib.crc32(b)
        self.client.put_object(Body=b, Bucket=bucket, Key=key)
        self.n_stored += 1
        self.set_storeinfo(archiver_notes, key, size, crc32)
        self.log(archiver_notes)
        return

    def deep_delete_from_archive(self, key):
        """
        delete all contents from S3 archive

        right now assert that buckets contian
        'devel' as part fo their name out of paranoia
       """
        self.deep_delete(self.primary_bucket, key)
        self.deep_delete(self.backup_bucket, key)

    def deep_delete(self, bucket_name, key):
        """
        delete all contents from S3 archive

        right now assert that bucket contain the string
        'devel' as part of its name out of paranoia
       """
        assert 'devel' in bucket_name
        s3  = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.object_versions.filter(Prefix=key).delete()

    def get_object(self, key):
        "return oject from S3"
        response = self.client.get_object(
            Bucket=self.primary_bucket,
            Key=key)
        data = response['Body'].read()
        return data

    def get_object_summary(self, key):
        "if not overriden, print error and die"
        summary = {"exists" : False}
        try:
            response = self.client.get_object(
                Bucket=self.primary_bucket,
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
        self.primary_bucket = "scimma-housekeeping"
        super().__init__(config)
        logging.info(f"Mock store configured")


    def store(self, payload, metadata, archiver_notes):
        "mock operation of storing in s3"
        self.n_stored += 1
        key = self.get_key(metadata, archiver_notes["con_text_uuid"])
        b = self.get_as_bson(payload, metadata, archiver_notes)
        size = len(b)
        crc32 = zlib.crc32(b)
        self.set_storeinfo(archiver_notes, key, size, crc32)
        self.log(archiver_notes)
