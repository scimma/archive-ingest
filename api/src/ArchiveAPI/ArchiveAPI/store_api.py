"""
Provide classes and API to stores  of archive objects.

There are two types homeomorphic classes

One class accesses AWS S3. This class can be configured
via archive_ingest.toml. It can access the production or
development S3 buckets via different configurations.

The other class  is "mock" store useful for
development and test. This class  discards the data.

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
# import utility_api
import zlib
import os

##################################
# stores
##################################


class Base_store:
    " base class for common methods"
    def __init__(self, config):
        self.primary_bucket = config["store-primary-bucket"]
        self.backup_bucket  = config["store-backup-bucket"]
        ## If custom S3 endpoint is specified, assume non-AWS config
        if 'store-endpoint-url' in config:
            self.s3_provider = 'custom'
            self.s3_endpoint_url = config['store-endpoint-url']
            self.s3_region_name = config['store-region-name']
            self.aws_access_key_id=os.environ['S3_ACCESS_KEY_ID']
            self.aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY']
        else:
            self.s3_provider = 'aws'
            self.s3_endpoint_url = ''
            self.s3_region_name = ''
            self.aws_access_key_id= ''
            self.aws_secret_access_key= ''
        self.n_stored = 0
        self.log_every = config["store-log-every"]
        self.config = config

    def connect(self):
        pass

    def log(self, annotations):
        "log storage informmation, but not too often"
        msg1 = f"stored {self.n_stored} objects."
        msg2 = f"This object: {annotations['size']} bytes to {annotations['bucket']} {annotations['key']}"
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

    def set_storeinfo(self, annotations, key, size, crc32):
        annotations['size'] = size
        annotations['key'] = key
        annotations['bucket'] = self.primary_bucket
        annotations['crc32'] = crc32


    def get_as_bson(self, payload, metadata, annotations):
        "return a blob of bson"
        ret = bson.dumps({"message" : payload,
                          "metadata" : metadata,
                          "annotations": annotations
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

    def initialize_bucket(self, bucket=''):
        exists = True
        try:
            self.client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = e.response['Error']['Code']
            if error_code == '404':
                exists = False
        if not exists:
            ## Create buckets if they do not exist
            self.client.create_bucket(Bucket=bucket)
    def connect(self):
        "obtain an S3 Client"
        if self.s3_provider == 'custom':
            self.client = boto3.client('s3',
                endpoint_url=self.s3_endpoint_url,
                region_name = self.s3_region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
            self.initialize_bucket(bucket=self.primary_bucket)
            self.initialize_bucket(bucket=self.backup_bucket)
        else:
            self.client = boto3.client('s3')

    def store(self, payload, metadata, annotations):
        """place data, metadata as an object in S3"""

        bucket = self.primary_bucket
        key = self.get_key(metadata, annotations["con_text_uuid"])
        b = self.get_as_bson(payload, metadata, annotations)
        size = len(b)
        crc32 = zlib.crc32(b)
        self.client.put_object(Body=b, Bucket=bucket, Key=key)
        self.n_stored += 1
        self.set_storeinfo(annotations, key, size, crc32)
        self.log(annotations)
        return

    def deep_delete_object_from_store(self, key):
        """
        delete all corresponding objects  from all S3 archive
        including versions and delete markers.
        right now assert that buckets contian
        'devel' as part fo their name out of paranoia
       """
        self.deep_delete_object(self.primary_bucket, key)
        self.deep_delete_object(self.backup_bucket, key)

    def deep_delete_object(self, bucket_name, key):
        """
        delete all contents related  to object from the S3
        bucket.
        right now assert that bucket contain the string
        'devel' as part of its name out of paranoia
       """

        assert "mock" in key or "archive-ingest-test" or "cmb-s4-fabric-tests.housekeeping-test" in key
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

    
    def list_object_versions(self, prefix):
        """ list all onecht verision under prefix"""
        import pdb; pdb.set_trace()
        s3 = session.resource('s3')
        my_bucket = s3.Bucket('self.primary_bucket')
        for _object in my_bucket.objects.all():
            print(object.key)
        """    
        paginator = client.get_paginator('list_objects')
        result = paginator.paginate(Bucket=self.primary_bucket
                                    , Delimiter=prefix)
        import pdb; pdb.set_trace()
        for prefix in result.search('CommonPrefixes'):
            print(prefix.get('Prefix'))
        return
        objectXSXCs = list(bucket.objects.filter(Prefix=prefix))
        for object in objects:
            for result in self.client.list_object_versions(
                Bucket=self.primary_bucket,
                Prefix=path_prefix):
            yield result
        """

    
class Mock_store(Base_store):
    """
    a mock store that does nothing -- support debug and devel.
    """

    def __init__(self, config):
        self.primary_bucket = "hopdevel-scimma-archive-ingest"
        super().__init__(config)
        logging.info(f"Mock store configured")


    def store(self, payload, metadata, annotations):
        "mock operation of storing in s3"
        self.n_stored += 1
        key = self.get_key(metadata, annotations["con_text_uuid"])
        b = self.get_as_bson(payload, metadata, annotations)
        size = len(b)
        crc32 = zlib.crc32(b)
        self.set_storeinfo(annotations, key, size, crc32)
        self.log(annotations)
