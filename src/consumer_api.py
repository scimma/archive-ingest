"""
Provide classes and API to sources of archive-ingest data.

There are two types archive-ingest classes

One class accesses hopskotch. This class can be configured
via archive_ingestx.toml. It can access the production or
development versions of hop via different configurations. Hop
credentials  are stored in AWS secrets.

The other class  is "mock" consumer useful for
development and test.

The ConsumerFactory class supports choosing which class is
used at run-time.


"""
##################################
#   Consumers
##################################

import boto3
from random import random
import toml
import logging
import time
import random
import uuid
import json
import hop
import zlib
import os
import uuid
from hop.io import Stream, StartPosition, list_topics
import utility_api
import datetime

class ConsumerFactory:
    """
    Factory class to create Mock, or HOP data consumers.
    """

    def __init__(self, config):
        config = utility_api.merge_config(config)
        type = config["hop-type"]
        #instantiate, then return consumer object of correct type.
        if type == "mock" : self.consumer =  Mock_consumer(config) ; return
        if type == "hop"  : self.consumer =  Hop_consumer(config)  ; return
        logging.fatal(f"consumer {type} not supported")
        exit (1)

    def get_consumer(self):
        "return the source sppecified in the toml file"
        return self.consumer

class Base_consumer:
    "base class for common methods"

    def __init__(self, config):
        pass


    def get_text_uuid(self, headers):
        """
        provide text uuid from binary uuid provided
        by hop client.

        The text uuis is needed for the database                                                                                                                           
        and formualation of object/file names.
        Pre v1.8.0 verisons  of the client do
        not provide a uuuid, so make one up.
        """

        def urn_to_uuid(urn):
            "Trim urn:uuid:"
            return urn.split(':')[2]

        text_uuid = ""
        _ids = [item for item in headers if item[0] == "_id"]
        #return the first valid uuid.
        for _id in _ids:
            try:
                binary_uuid = _id[1]
                urn  = uuid.UUID(bytes=binary_uuid).urn
                is_client_uuid = True
                return (urn_to_uuid(urn), is_client_uuid)
            except (ValueError, IndexError, TypeError) :
                continue
        #nothing there; so make one up.
        urn  = uuid.uuid4().urn
        text_uuid = urn_to_uuid(urn)
        is_client_uuid = False #made it up on the server
        logging.debug(f"I made a UUID up: {text_uuid}")
        return (text_uuid, is_client_uuid)

    def get_annotations(self, message, headers):
        """
        Provide a stand-alone record for downstream
        processing.

        - Text uuid
        - If text uus was made up.
        - crc of message payload.
        """
        annotations = {}
        text_uuid, is_client_uuid  = self.get_text_uuid(headers)
        annotations["con_text_uuid"] = text_uuid
        annotations["con_is_client_uuid"] = is_client_uuid
        annotations["con_message_crc32"] =  zlib.crc32(message["content"])
        return annotations

    
    def connect(self):
        pass

    def mark_done(self):
        pass

    def close(self):
        pass

class Mock_message:
    """
    A class to define hold, and issue mock messages.

    The class variable Mock_message.message_list
    holds the list defined messages.

    Messages hold a message payload, and metadata,
    which include headers.    
    """
    message_list = []
    
    def __init__(self, usecase, timestamp=None, binary_uuid=None, is_client_uuid=True):
        "default to a  client-supplied UUID usecase"
        self.usecase = usecase
        self.message = os.urandom(50000)
        self.topic = "mock.topic"
        self.wrap_method =  hop.models.Blob
        self.headers = [("_mock_use_case", usecase)]
        
        # allow for simulating duplicates.
        self.timestamp = timestamp if timestamp else int(time.time()/1000) 
   
        # allow for client and server-side ,,,,
        # (pre hop lcient v1.8.0) simulatiosn
        self.is_client_uuid = is_client_uuid
        if self.is_client_uuid:
            self.binary_uuid = binary_uuid if binary_uuid else uuid.uuid4().bytes
            if self.is_client_uuid : self.headers.append (('_id',self.binary_uuid))
        #keep them all 
        Mock_message.message_list.append(self)
        
    def get_message(self):
        "generate, and return message body as if from HOP"
        return self.wrap_method(self.message).serialize()

    def get_metadata(self):
        "generate and return message body as if from HOP"
        metadata = {
            "topic" : self.topic,
            "timestamp" : self.timestamp,
            "headers" : self.headers
            }
        return metadata

    def get_all(self):
        "return messages, metadata as if from HOP"
        message = self.get_message()
        metadata = self.get_metadata()
        return message, metadata

        
class Mock_consumer(Base_consumer):
    """
    A mock consumer support generation
    of test use cases that cannot normally
    be generated by the test  producer.

    mock does this by producing data as if
    if was recieved from hop.

    Use cases supported:
    
    - use of hop client prior to hop ....
      ... client 1.8.0 (no _id in header.)
    - more-than-once delivery
    - replay of stream due to cursor reset.
    """

    def __init__(self, config):
        logging.info(f"Mock Consumer configured")
        # repeated recent  duplicate (e.g recived more than at least once) 
        # client_supplied UUID
        m = Mock_message(b'client-side uuid, recieved twice')
        _ = Mock_message(b'client-side uuid, recieved twice',
                         timestamp=m.timestamp, binary_uuid=m.binary_uuid)
        
        # repeated non-recent duplicate, (e.g cursor reset)
        # - client_supplied BINARY_UUID, both UUID's the same.
        # -timestamps are identical 
        t0 = time.time() - 24*60*60*10  #ten days old
        t0 = int(t0/1000)
        m = Mock_message(b'client-side uuid, recieved twice',timestamp = t0)
        _ = Mock_message(b'client-side uuid, recieved twice',timestamp = t0, binary_uuid=m.binary_uuid)

        # repeated  recent  duplicate (e.g recived more than at least once) 
        # - server_supplied UUID differ.
        # timestamps are identical
        m = Mock_message(b'server-side uuid, recieved twice',
                         is_client_uuid=False)
        m = Mock_message(b'server-side uuid, recieved twice',
                            is_client_uuid=False,
                            timestamp=m.timestamp)
        
        # repeated non-recent duplicate, (e.g cursor reset)
        # server_supplied UUID (e.g UUID's differ.)
        # timestamps are identical
        t0 = time.time() - 24*60*60*10  #ten days old
        t0 = int(t0/1000)
        m = Mock_message(b'server-side uuid, recieved twice',
                         is_client_uuid=False,
                         timestamp = t0)
        m = Mock_message(b'server-side uuid, recieved twice',
                         is_client_uuid=False,
                         timestamp = t0)

        super().__init__(config)


    def is_active(self):
        "has hop shut down?"
        if self.n_events > 0 :
            self.n_events = self.n_events-1
            return True
        else:
            return False

    def get_next(self):
        "get next mock message"
        for m  in Mock_message.message_list:
            message, metadata = m.get_all()
            annotations = self.get_annotations(message, metadata["headers"])
            logging.info(f"moch data -- {metadata}  {annotations}")
            yield (message, metadata, annotations)

            

class Hop_consumer(Base_consumer):
    """
    A class to consume data from Hop

    The consumer is configured via a 'tofl'
    configuraiton file, where stazas are
    selcted by command line switchs
    
    The consumer reads messages from any stream
    authorized by its credentials, subject to a veto
    list which is read at startup. To accomodate newly
    defined topics, the list is refreshed by
    configurable timeout. Refreses are logged, and
    also written to the topics.log  in the currnet working
    directory

    Howeer, for testing the consumder will
    consume only from a configured test topic.
    This provides a defined environemt.
    
    HOP credentials are stored as AWS secrets
    specifed in the configuration file ...

    
    """
    def __init__(self, config):
        self.config           = config
        self.groupname        = config["hop-groupname"]
        self.until_eos        = config["hop-until-eos"]
        if 'hop-local-auth' in config:
            self.local_auth_file      = config['hop-local-auth']
            self.secret_name      = ''
            self.region_name      = ''
        else:
            self.local_auth_file      = ''
            self.secret_name      = config["hop-aws-secret-name"]
            self.region_name      = config["hop-aws-secret-region"]
        self.test_topic       = config["hop-test-topic"]
        self.test_topic_max_messages       = config.get("hop-test-max-messages",10)  #demo hack
        self.vetoed_topics    = config["hop-vetoed-topics"]
        self.vetoed_topics.append(self.test_topic)  # don't consume test topic   
        self.refresh_interval = config["hop-topic-refresh-interval-seconds"]
        self.last_last_refresh_time = 0

        self.authorize()
        self.base_url = (
                f"kafka://"   \
                f"{self.username}@" \
                f"{config['hop-hostname']}:" \
                f"{config['hop-port']}/"
            )

        self.refresh_url_every =  1000  # make this a config
        self.n_recieved = 0
        super().__init__(config)

    def refresh_url(self):
        """
        initalize/refresh the list of topics to record PRN
        
        """
        # return if interval too small
        interval = time.time() - self.last_last_refresh_time
        if self.refresh_interval > interval:
            return

        # interval exceeded -- reset time, and refresh topic list
        self.last_last_refresh_time = time.time()
        if self.test_topic:
            # trivial refresh this topic supports  test and debug.
            topics = self.test_topic
            self.url = (f"{self.base_url}{topics}")
            logging.info(f'''Consuming hop-test-topic URL: "{self.url}"''')
        else:
            # Read the available topics from the given broker
            # notice that we are not given "public topics" we
            # are given topics we are permitted to read. .seems
            # to include the private test topic.
            topic_dict = list_topics(url=self.base_url, auth=self.auth)

            # Concatinate the avilable topics with the broker address
            # omit vetoed  topics
            topics = ','.join([t for t in topic_dict.keys() if t not in self.vetoed_topics])
            self.url = (f"{self.base_url}{topics}")
            logging.info(f"Hop Url (re)configured: {self.url} excluding {self.vetoed_topics}")
            with open("topics.log","a") as f:
                f.write(f"{datetime.datetime.utcnow().isoformat()} {topics}\n")



    def connect(self):
        "connect to HOP a a consumer (archiver)"
        start_at = StartPosition.EARLIEST
        # start_at = StartPosition.LATEST
        stream = Stream(auth=self.auth, start_at=start_at, until_eos=self.until_eos)

        # Return the connection to the client as :class:"hop.io.Consumer" instance
        # THe commented uot group ID (below)) migh tbe useful in some development
        # environments it allows for re-consumption of all the existing events in all
        # the topics.
        self.refresh_url()
        group_id = f"{self.username}-{self.groupname}"
        group_id = f"{self.username}-{self.groupname}{random.randint(0,10000)}"
        self.client = stream.open(url=self.url, group_id=group_id)
        # self.client = stream.open(url=self.url)
        logging.info(f"opening stream at {self.url} group: {group_id} startpos {start_at}")

    def authorize(self):
        if self.local_auth_file:
            self.username = os.environ['HOP_USERNAME']
            self.auth  = hop.auth.load_auth(self.local_auth_file)
        else:
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

    def is_active(self):
        return True

    def get_next(self):
        self.refresh_url() # needed bfore first call to set topic list.
        for result in self.client.read(metadata=True, autocommit=False):
            # What happens on error? GEt nothing back? None?
            # -- seems to stall in self.client.read
            # -- lack a full understanding fo thsi case.
            self.refresh_url()
            message = result[0].serialize()
            # metadata remarks
            # save the original metadata for mark_done api.
            # make a syntheite metadata reflecting what the ...
            # user would  see, omitting kafak internals,
            self.original_metadata = result[1]
            if result[1].headers is None :
                headers = []
            else:
                # Bson Librsy compatablity:
                # tuple -> list
                # Nuke Null values to empty string
                headers = []
                for key, val in result[1].headers:
                    if val == None : val = ""
                    headers.append([key, val]) 

            #copy out the metadata of interest to what we save
            metadata = {"timestamp" : result[1].timestamp,
                        "headers" : headers,
                        "topic" : result[1].topic
                        }
            self.n_recieved  += 1
            #logging.info(f"topic, msg size, meta size: {metadata.topic} {len(message)}")
            annotations = self.get_annotations(message, metadata["headers"])
            yield (message, metadata, annotations)

    def mark_done(self):
        """
        mark that we are done processing, since autocommit=False

        original metadata is required per source code.
        """
        #import pdb; pdb.set_trace()
        self.client.mark_done(self.original_metadata)

    def close(self):
        self.client.close()
