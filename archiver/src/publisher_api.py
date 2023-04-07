"""
Provide classes and API to pubis data to kafaka/hop
for testing the archiver.

There are two types homeomorphic classes

One class accesses hopskotch. This class can be configured
via archive_ingest.toml. It can access the production or
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
import toml
import logging
import json
import certifi
import hop
from hop.io import Stream, StartPosition, list_topics
import utility_api

class PublisherFactory:
    """
    Factory class to create Mock, or HOP data publishers. 
    """

    def __init__(self, args):
        config = utility_api.merge_config(args)
        type = config["hop-type"]
        #instantiate, then return publisher object of correct type.
        if type == "kcat" : self.publisher =  Kcat_publisher(config) ; return
        if type == "hop"  : self.publisher =  Hop_publisher(config)  ; return
        logging.fatal(f"publisher {type} not supported")
        exit (1)
        
    def get_publisher(self):
        "return the srouce sppecified in the toml file"
        return self.publisher

class Base_publisher:
    "base class for common methods"
    
    def __init__(self, config):
        pass
    
    def connect(self):
        pass

    def authorize(self):
        pass

    def get_auth_info(self):
        "obtain AWS secrets"
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
        self.password = resp["password"]

    def publish(self, messages, header=()):
        logging.info(f"dropping message on the floor")

    def mark_done(self):
        pass


class Hop_publisher(Base_publisher):
    " A class to write data from Hop"
    def __init__(self, config):
        self.groupname     = config["hop-groupname"]
        self.secret_name   = config["hop-aws-secret-name"]
        self.region_name   = config["hop-aws-secret-region"]
        self.test_topic    = config["hop-test-topic"]
        
        self.authorize()
        self.base_url = (
                f"kafka://"   \
                f"{self.username}@" \
                f"{config['hop-hostname']}:" \
                f"{config['hop-port']}/"
            )
        super().__init__(config)

        

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

        
class Kcat_publisher(Base_publisher):
    "a class to publish data to hop"
    def __init__(self, config):
        self.groupname     = config["hop-groupname"]
        self.secret_name   = config["hop-aws-secret-name"]
        self.region_name   = config["hop-aws-secret-region"]
        self.test_topic    = config["hop-test-topic"]
        
        self.get_auth_info()
        self.base_url = (
                #f"kafka://"   \
                #f"{self.username}@" \
                f"{config['hop-hostname']}:" \
                f"{config['hop-port']}"
            )
        super().__init__(config)
    

    def publish(self, message, header=()):
        import tempfile 
        auth_template = f""" kcat -b {self.base_url} \
        -X security.protocol=sasl_ssl -X sasl.mechanisms=SCRAM-SHA-512 \
        -X sasl.username={self.username}  \
        -X sasl.password={self.password}  \
        -t {self.test_topic} -P
        """
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(message)
        msessage_file_name =  tmp.name
        print(auth_template)
