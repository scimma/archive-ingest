"""
Provide classes and API to pubis data to kafaka/hop
for testing the archiver.

There are two types homeomorphic classes

One class accesses hopskotch. This class can be configured
via housekeeping.toml. It can access the production or
development versions of hop via different configurations. Hop
credentials  are stored in AWS secrets.

The other class  is "mock" writer useful for
development and test.

The WriterFactory class supports choosing which class is
used at run-time.

All classes use a namespace object (args), such
as provided by argparse, as part of their interface.

"""
##################################
#   Writers 
##################################

import boto3
from random import random
import toml
import logging
import time
import random
import uuid
import json
import os
import certifi
import hop
from hop.io import Stream, StartPosition, list_topics

class WriterFactory:
    """
    Factory class to create Mock, or HOP data writers. 
    """

    def __init__(self, args):
        toml_data = toml.load(args.toml_file)
        config    = toml_data.get(args.hop_stanza, None)

        type = config["type"]
        #instantiate, then return writer object of correct type.
        #if type == "kafka" : self.writer =  Kafka_writer(args, config) ; return
        if type == "hop"  : self.writer =  Hop_writer(args, config)  ; return
        logging.fatal(f"writer {type} not supported")
        exit (1)
        
    def get_writer(self):
        "return the srouce sppecified in the toml file"
        return self.writer

class Base_writer:
    "base class for common methods"
    
    def __init__(self, args, config):
        pass
    
    def connect(self):
        pass

    def publish(self, messages, header=()):
        logging.info(f"dropping message on the floor")
        pass

    def mark_done(self):
        pass


class Hop_writer(Base_writer):
    " A class to writer data from Hop"
    def __init__(self, args, config):
        self.args    = args
        toml_data    =   toml.load(args.toml_file)
        config       =   toml_data[args.hop_stanza]
        self.groupname     = config["groupname"]
        self.secret_name   = config["aws-secret-name"]
        self.region_name   = config["aws-secret-region"]
        self.test_topic    = config["test-topic"]
        
        self.authorize()
        self.base_url = (
                f"kafka://"   \
                f"{self.username}@" \
                f"{config['hostname']}:" \
                f"{config['port']}/"
            )

        super().__init__(args, config)
        
    def refresh_url(self):
        "initalize/refresh the list of topics to record PRN"
        #return if not not needed.
        if self.n_recieved  % self.refresh_url_every != 0: return
        if self.args.test_topic:
            #this implementation suposrt test and debug.
            topics = self.test_topic
        else: 
            # Read the available topics from the given broker
            topic_dict = list_topics(url=self.base_url, auth=self.auth)
        
            # Concatinate the avilable topics with the broker address
            # omitvetoed  topics
            topics = ','.join([t for t in topic_dict.keys() if t not in self.vetoed_topics])
        self.url = (f"{self.base_url}{topics}")
        logging.info(f"Hop Url (re)configured: {self.url} excluding {self.vetoed_topics}")
    

    def connect(self):
        "connect to write test messages to teat_topic"
        stream = Stream(auth=self.auth)
        url = self.base_url + self.test_topic
        group_id = f"{self.username}W-{self.groupname}w"
        logging.info (f"opening for write {url} group: {group_id}")
        #self.write_client = stream.open(url=url, group_id=group_id)
        self.write_client = stream.open(url, "w")

    def authorize(self):
        "authorize using AWS secrets"
        from hop.auth import Auth
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )
        resp = client.get_secret_value(
            SecretId=self.secret_name
        )['SecretString']
        resp = json.loads(resp)
        self.username = resp["username"]
        logging.info (f"hopskotch username is: {self.username}")
        self.auth  = hop.auth.Auth(resp["username"], resp["password"])
        return
        
    
    def publish(self, message, headers):
        """
        publish a message to support testnig 
        """
        self.write_client.write(message, headers)
        self.write_client.flush()

            

